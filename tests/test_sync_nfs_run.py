"""
Tests for utils/rag_sync_nfs.py as the NAS ingestion run (v0.3 session B2, step A.11).

What is covered here, and not elsewhere:
  - the run takes its own lock (rag.nas_ingest.lock), never rag.ingest_lock;
  - the phase order: poll, scan, plan, reconcile per source, then submit;
  - --dry-run writes nothing and sends no write request (the hub mock only ever sees GET-style calls);
  - --rebuild-from-hub is wired to rebuild_from_hub and nothing else;
  - a hub that cannot be reached at startup stops the run before anything is written;
  - the exit codes, the secret-file loading, and the no-Qdrant import of the script.

Nothing live is touched. The hub is a MagicMock HubClient, or a real HubClient over an httpx MockTransport. The
state and registry are tmp files. Production paths (the config's db_base_path and lock path) are never opened: the
tests call the functions directly with tmp paths, and only read the real ragnart.yaml.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
from collections import Counter
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest
import yaml

from ragnart.endpoint_client import HubClient
from ragnart.lock import EXIT_LOCK_HELD, nas_ingest_lock_path_from_config, lock_path_from_config
from ragnart.registry import RegistryDB
from ragnart.schema import (
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
)
from ragnart.state import StateDB
from ragnart.sync_plan_core import SyncPlan

REPO_ROOT = Path(__file__).resolve().parent.parent
OWNER = "tester"
SOURCES = (SOURCE_ID_NAS, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_OPENCODE_K11)
WRITE_METHODS = (
    "upsert_bytes", "upsert_text", "delete", "change_key", "change_metadata", "publish_project", "remove_project",
)


@pytest.fixture
def sync_module():
    """Load utils/rag_sync_nfs.py as a module. It is a script, not a package."""
    spec = importlib.util.spec_from_file_location("rag_sync_nfs", REPO_ROOT / "utils" / "rag_sync_nfs.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def state_db(tmp_path):
    db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    yield db
    db.close()


@pytest.fixture
def registry_db(tmp_path):
    db = RegistryDB(str(tmp_path / "registry" / "hub_registry.db"))
    yield db
    db.close()


def _hub() -> MagicMock:
    """A hub stand-in whose reads answer like the API: a pipeline version, an empty listing, a 'received' status."""
    client = MagicMock(spec=HubClient)
    client.info.return_value = {"pipeline_version": "p1"}
    client.items.return_value = []
    client.status.return_value = {"status": "received"}
    client.upsert_bytes.return_value = {"request_id": "req-1", "status": "received"}
    client.upsert_text.return_value = {"request_id": "req-1", "status": "received"}
    return client


def _clients(hub: MagicMock) -> dict:
    return {source: hub for source in SOURCES}


def _config(root: Path | None = None, batch: int = 200) -> dict:
    """A ragnart.yaml-shaped dict. With a root, one user's NAS crawl is configured; the hub block has no secrets."""
    users = {"tester@example.com": {"owner": OWNER, "nfs_roots": [str(root)]}} if root is not None else {}
    return {
        "rag": {
            "scanner": {"include_extensions": [".md"]},
            "sync": {"max_delete_fraction": 0.10},
            "nas_ingest": {"hub_url": "http://hub.invalid:8101", "secrets": {}, "resubmit_batch_size": batch},
        },
        "users": users,
    }


def _args(**overrides) -> argparse.Namespace:
    values = {"user": None, "allow_mass_delete": False, "dry_run": False, "rebuild_from_hub": False}
    values.update(overrides)
    return argparse.Namespace(**values)


def yaml_load(path: Path) -> dict:
    """Read a YAML file for reading only (the real ragnart.yaml, to learn its lock paths)."""
    with open(path, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _write_secret(path: Path, text: str = "not-a-real-secret") -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return str(path)


# ---------------------------------------------------------------------------
# The lock
# ---------------------------------------------------------------------------


def test_the_run_takes_the_nas_ingest_lock_and_not_the_index_lock(sync_module, monkeypatch, tmp_path):
    """main() passes the rag.nas_ingest.lock path to acquire_or_exit, never rag.ingest_lock."""
    real_config = yaml_load(REPO_ROOT / "ragnart.yaml")
    taken: list[str] = []

    def refuse(lock_path: str) -> None:
        taken.append(lock_path)
        sys.exit(EXIT_LOCK_HELD)

    monkeypatch.setattr(sync_module, "acquire_or_exit", refuse)
    monkeypatch.setattr(sys, "argv", ["rag_sync_nfs.py"])

    with pytest.raises(SystemExit) as exc_info:
        sync_module.main()

    assert exc_info.value.code == EXIT_LOCK_HELD
    assert taken == [nas_ingest_lock_path_from_config(real_config)]
    assert taken != [lock_path_from_config(real_config)]


def test_a_refused_lock_exits_75_and_opens_no_state_db(sync_module, monkeypatch):
    """
    When another writer holds the NAS-ingestion lock, main() exits with EXIT_LOCK_HELD and never opens rag_state.db.

    The queue must not be opened by a writer that was refused the lock. The StateDB stand-in must never be constructed.
    """
    def refuse(lock_path: str) -> None:
        sys.exit(EXIT_LOCK_HELD)

    state_db_factory = MagicMock(name="StateDB")
    monkeypatch.setattr(sync_module, "acquire_or_exit", refuse)
    monkeypatch.setattr(sync_module, "StateDB", state_db_factory)
    monkeypatch.setattr(sys, "argv", ["rag_sync_nfs.py"])

    with pytest.raises(SystemExit) as exc_info:
        sync_module.main()

    assert exc_info.value.code == EXIT_LOCK_HELD
    state_db_factory.assert_not_called()


def test_the_nas_ingest_lock_file_is_in_the_data_root(sync_module):
    """The configured lock lives beside the databases, on the NAS data root, like rag-ingest.lock."""
    path = nas_ingest_lock_path_from_config(yaml_load(REPO_ROOT / "ragnart.yaml"))
    assert os.path.basename(path) == "rag-nas-ingest.lock"
    assert os.path.dirname(path) == os.path.dirname(lock_path_from_config(yaml_load(REPO_ROOT / "ragnart.yaml")))


# ---------------------------------------------------------------------------
# Phase order
# ---------------------------------------------------------------------------


def test_phases_run_in_the_documented_order_poll_scan_plan_reconcile_then_submit(
    sync_module, state_db, registry_db, monkeypatch, tmp_path,
):
    """poll -> scan -> plan -> reconcile (each source, after the scan) -> submit. Reconcile comes before submit."""
    order: list[str] = []
    empty_scan = {"scanned": 0, "existing_paths": set(), "skipped_roots": [], "project_markers": [], "new_hashes": {}}

    def poll(state, clients):
        order.append("poll")
        return Counter()

    def nfs(state, config, user_filter=None):
        order.append("scan_nfs")
        return dict(empty_scan)

    def chats(state, config, user_filter=None):
        order.append("scan_claude")
        return {"scanned": 0, "existing_paths": set(), "skipped_roots": []}

    def opencode(state, config, user_filter=None):
        order.append("scan_opencode")
        return {"scanned": 0, "existing_paths": set()}

    def plan(*args, **kwargs):
        order.append("plan")
        return SyncPlan()

    def reconcile(state, client, source_id, present, fraction, **kwargs):
        order.append(f"reconcile:{source_id}")
        return Counter()

    def submit(*args, **kwargs):
        order.append("submit")
        return Counter()

    monkeypatch.setattr(sync_module, "poll_outstanding", poll)
    monkeypatch.setattr(sync_module, "scan_nfs_roots", nfs)
    monkeypatch.setattr(sync_module, "scan_claude_chats", chats)
    monkeypatch.setattr(sync_module, "scan_opencode_sessions", opencode)
    monkeypatch.setattr(sync_module, "build_sync_plan", plan)
    monkeypatch.setattr(sync_module, "reconcile_source", reconcile)
    monkeypatch.setattr(sync_module, "submit_plan", submit)

    assert sync_module.sync(_config(), state_db, registry_db, _clients(_hub()), _args()) == 0

    assert order == [
        "poll", "scan_nfs", "scan_claude", "scan_opencode", "plan",
        f"reconcile:{SOURCE_ID_NAS}", f"reconcile:{SOURCE_ID_CLAUDE_LINUX_SHARED_NFS}", f"reconcile:{SOURCE_ID_OPENCODE_K11}",
        "submit",
    ]


def test_reconcile_receives_the_presence_of_the_scan_and_the_configured_batch_size(
    sync_module, state_db, registry_db, monkeypatch,
):
    seen: list[dict] = []
    seen_present: list[set] = []

    def reconcile(state, client, source_id, present, fraction, **kwargs):
        seen.append(kwargs)
        seen_present.append(set(present))
        return Counter()

    monkeypatch.setattr(sync_module, "reconcile_source", reconcile)
    monkeypatch.setattr(sync_module, "poll_outstanding", lambda state, clients: Counter())
    monkeypatch.setattr(sync_module, "submit_plan", lambda *a, **k: Counter())
    monkeypatch.setattr(sync_module, "build_sync_plan", lambda *a, **k: SyncPlan())

    assert sync_module.sync(_config(batch=7), state_db, registry_db, _clients(_hub()), _args()) == 0

    assert len(seen) == 3
    assert all(kwargs["resubmit_batch_size"] == 7 for kwargs in seen)
    assert all(kwargs["hub_items"] == [] for kwargs in seen)


def test_a_source_whose_listing_fails_is_skipped_and_the_others_still_reconcile(sync_module, state_db, registry_db, monkeypatch):
    """A hub that fails one listing decides nothing for that source; the run goes on to submit."""
    failing_nas = _hub()
    failing_nas.items.side_effect = httpx.ConnectError("hub down")
    clients = {SOURCE_ID_NAS: failing_nas, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS: _hub(), SOURCE_ID_OPENCODE_K11: _hub()}
    reconciled: list[str] = []

    def reconcile(state, client, source_id, present, fraction, **kwargs):
        reconciled.append(source_id)
        return Counter()

    monkeypatch.setattr(sync_module, "reconcile_source", reconcile)
    monkeypatch.setattr(sync_module, "poll_outstanding", lambda state, clients: Counter())
    submitted: list[bool] = []
    monkeypatch.setattr(sync_module, "submit_plan", lambda *a, **k: submitted.append(True) or Counter())
    monkeypatch.setattr(sync_module, "build_sync_plan", lambda *a, **k: SyncPlan())

    assert sync_module.sync(_config(), state_db, registry_db, clients, _args()) == 0

    assert reconciled == [SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_OPENCODE_K11]
    assert submitted == [True]


def test_a_fixed_owner_refuses_the_run_before_any_scan_or_write(sync_module, state_db, registry_db, monkeypatch):
    # The NAS source gets a fixed owner, which the hub would refuse every per-item owner for.
    registry_db._conn.execute("UPDATE sources SET owner = 'florian' WHERE id = ?", (SOURCE_ID_NAS,))
    registry_db._conn.commit()
    monkeypatch.setattr(sync_module, "scan_nfs_roots", lambda *a, **k: pytest.fail("scan ran before the refusal"))

    with pytest.raises(RuntimeError, match="fixed owner"):
        sync_module.sync(_config(), state_db, registry_db, _clients(_hub()), _args())

    assert state_db.get_counts()["total"] == 0


def test_a_batch_size_below_one_is_refused_before_any_scan(sync_module, state_db, registry_db, monkeypatch):
    monkeypatch.setattr(sync_module, "scan_nfs_roots", lambda *a, **k: pytest.fail("scan ran with a bad batch size"))

    with pytest.raises(ValueError, match="resubmit_batch_size"):
        sync_module.sync(_config(batch=0), state_db, registry_db, _clients(_hub()), _args())


# ---------------------------------------------------------------------------
# Dry run: reads the hub, writes nothing, sends no write request
# ---------------------------------------------------------------------------


def _seed_completed_nas_files(state_db: StateDB, root: Path, count: int) -> list[str]:
    """Real files under the root, each queued and completed with its on-disk mtime, so the scan sees no change."""
    root.mkdir(parents=True, exist_ok=True)
    paths = []
    for index in range(count):
        path = root / f"f{index:02d}.md"
        path.write_text(f"file {index}", encoding="utf-8")
        mtime = os.stat(path).st_mtime
        state_db.discover_or_update(str(path), OWNER, mtime, origin_path=str(path), source_type="file",
                                    file_type="md", source_id=SOURCE_ID_NAS)
        state_db.mark_completed(str(path), chunk_count=2, pipeline_version="p1", content_hash="h", project_id="global")
        paths.append(str(path))
    return paths


def test_dry_run_reads_the_hub_and_sends_no_write_request_and_changes_no_state(sync_module, tmp_path):
    """
    The real dry-run path, on a real state copy. The hub lists every local file except one, and holds one orphan.
    The dry run must count what it would do (an orphan, a missing file) and call only read methods.
    """
    live_db = str(tmp_path / "state" / "rag_state.db")
    live_registry = str(tmp_path / "state" / "hub_registry.db")
    root = tmp_path / "nas"
    live = StateDB(live_db)
    paths = _seed_completed_nas_files(live, root, 20)
    live.close()
    before = _snapshot(live_db)

    hub = _hub()
    hub.items.return_value = [
        {"key": path, "chunk_count": 2, "content_hash": "h", "pipeline_version": "p1", "owner": OWNER,
         "project_id": "global", "source_type": "file", "file_type": "md", "inconsistent": []}
        for path in paths[1:]
    ] + [
        {"key": str(root / "orphan.md"), "chunk_count": 2, "content_hash": "h", "pipeline_version": "p1",
         "owner": OWNER, "project_id": "global", "source_type": "file", "file_type": "md", "inconsistent": []},
    ]

    assert sync_module.run_dry_run(_config(root), live_db, live_registry, _clients(hub), _args(dry_run=True)) == 0

    # Only read methods were called on the hub, for every source.
    called = {name for name, _args_, _kwargs in hub.mock_calls}
    assert called <= {"info", "items", "status"}
    assert not any(name in WRITE_METHODS for name in called)
    # Nothing changed in the live state, and the live registry was not created.
    assert _snapshot(live_db) == before
    assert not os.path.exists(live_registry)


def test_dry_run_output_reports_the_reconcile_counts_and_the_listing_time_per_source(sync_module, tmp_path, capsys):
    live_db = str(tmp_path / "state" / "rag_state.db")
    root = tmp_path / "nas"
    live = StateDB(live_db)
    _seed_completed_nas_files(live, root, 20)
    live.close()
    hub = _hub()

    sync_module.run_dry_run(_config(root), live_db, str(tmp_path / "state" / "hub_registry.db"), _clients(hub), _args(dry_run=True))

    out = capsys.readouterr().out
    assert "Dry run: nothing was written" in out
    for source in SOURCES:
        assert f"  {source}: listing " in out
    for key in ("orphan_would_delete=", "missing_would_resubmit=", "version_would_resubmit=", "drift_would_fix="):
        assert key in out


def test_dry_run_output_reports_the_zero_chunks_skipped_count_of_its_source(sync_module, tmp_path, monkeypatch, capsys):
    """
    missing_zero_chunks_skipped is a reconcile count with no dry-run ("would") form. The report must show the count
    reconcile returned, on the reconcile line of the source it came from.
    """
    live_db = str(tmp_path / "state" / "rag_state.db")
    StateDB(live_db).close()

    def reconcile(state, client, source_id, present, fraction, **kwargs):
        return Counter({"missing_zero_chunks_skipped": 3}) if source_id == SOURCE_ID_NAS else Counter()

    monkeypatch.setattr(sync_module, "reconcile_source", reconcile)

    code = sync_module.run_dry_run(_config(), live_db, str(tmp_path / "state" / "hub_registry.db"), _clients(_hub()), _args(dry_run=True))

    assert code == 0
    lines = capsys.readouterr().out.splitlines()
    listing_line = next(i for i, line in enumerate(lines) if line.startswith(f"  {SOURCE_ID_NAS}: listing "))
    assert "missing_zero_chunks_skipped=3" in lines[listing_line + 1]


def test_dry_run_exits_2_when_a_listing_cannot_be_read(sync_module, tmp_path):
    live_db = str(tmp_path / "state" / "rag_state.db")
    StateDB(live_db).close()
    hub = _hub()
    hub.items.side_effect = httpx.ConnectError("hub down")

    code = sync_module.run_dry_run(_config(), live_db, str(tmp_path / "state" / "hub_registry.db"), _clients(hub), _args(dry_run=True))

    assert code == sync_module.EXIT_HUB_UNAVAILABLE


def _snapshot(db_path: str) -> list[tuple]:
    """Every queue row, as a sorted list. Used to show that a dry run left the live state exactly as it was."""
    conn = sqlite3.connect(db_path)
    try:
        return sorted(conn.execute("SELECT * FROM ingestion_queue").fetchall())
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rebuild wiring
# ---------------------------------------------------------------------------


def test_rebuild_flag_calls_rebuild_from_hub_and_does_not_run_the_sync(sync_module, state_db, registry_db, monkeypatch, tmp_path):
    calls: list[tuple] = []

    def rebuild(state, clients, config, user_filter=None):
        calls.append((state, user_filter))
        return Counter({"rows_written": 0})

    monkeypatch.setattr(sync_module, "rebuild_from_hub", rebuild)
    monkeypatch.setattr(sync_module, "sync", lambda *a, **k: pytest.fail("the sync ran with --rebuild-from-hub"))

    code = sync_module._run_against_hub(
        _config(), str(tmp_path / "unused.db"), str(tmp_path / "unused-registry.db"), _clients(_hub()),
        _args(rebuild_from_hub=True, user=OWNER),
    )

    assert code == sync_module.EXIT_OK
    assert len(calls) == 1
    assert calls[0][1] == OWNER


def test_a_hub_failure_during_the_rebuild_listing_exits_2_and_writes_nothing(sync_module, tmp_path):
    db_path = str(tmp_path / "state" / "rag_state.db")
    hub = _hub()
    hub.items.side_effect = httpx.ConnectError("hub down")

    code = sync_module._run_against_hub(
        _config(), db_path, str(tmp_path / "state" / "hub_registry.db"), _clients(hub),
        _args(rebuild_from_hub=True),
    )

    assert code == sync_module.EXIT_HUB_UNAVAILABLE
    assert _snapshot(db_path) == []


def test_rebuild_with_dry_run_is_a_usage_error(sync_module, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["rag_sync_nfs.py", "--dry-run", "--rebuild-from-hub"])
    monkeypatch.setattr(sync_module, "acquire_or_exit", lambda *a, **k: pytest.fail("a parse error took the lock"))

    with pytest.raises(SystemExit) as exc_info:
        sync_module.main()

    assert exc_info.value.code == 2


@pytest.mark.parametrize("extra", [[], ["--dry-run"], ["--user", OWNER]])
def test_state_db_without_rebuild_from_hub_is_a_usage_error(sync_module, monkeypatch, tmp_path, extra):
    """--state-db is accepted only with --rebuild-from-hub. Any other run is refused before the lock or a secret is read."""
    monkeypatch.setattr(sys, "argv", ["rag_sync_nfs.py", "--state-db", str(tmp_path / "scratch.db"), *extra])
    monkeypatch.setattr(sync_module, "acquire_or_exit", lambda *a, **k: pytest.fail("a parse error took the lock"))
    monkeypatch.setattr(sync_module, "build_clients", lambda *a, **k: pytest.fail("a parse error read the secrets"))

    with pytest.raises(SystemExit) as exc_info:
        sync_module.main()

    assert exc_info.value.code == 2
    assert not (tmp_path / "scratch.db").exists()


def test_state_db_with_rebuild_from_hub_points_the_rebuild_at_that_file(sync_module, monkeypatch, tmp_path):
    """main() hands the --state-db path to the rebuild, and the live state path is not used."""
    scratch = str(tmp_path / "scratch" / "rag_state.db")
    seen: list[str] = []

    def run_against_hub(config, db_path, registry_path, clients, args):
        seen.append(db_path)
        return sync_module.EXIT_OK

    monkeypatch.setattr(sys, "argv", ["rag_sync_nfs.py", "--rebuild-from-hub", "--state-db", scratch])
    monkeypatch.setattr(sync_module, "acquire_or_exit", lambda *a, **k: None)
    monkeypatch.setattr(sync_module, "build_clients", lambda *a, **k: _clients(_hub()))
    monkeypatch.setattr(sync_module, "hub_startup_failures", lambda clients: {})
    monkeypatch.setattr(sync_module, "_run_against_hub", run_against_hub)

    with pytest.raises(SystemExit) as exc_info:
        sync_module.main()

    assert exc_info.value.code == sync_module.EXIT_OK
    assert seen == [scratch]


def test_rebuild_into_a_scratch_state_opens_no_registry(sync_module, tmp_path):
    """A rebuild into a scratch rag_state.db writes that file only. The hub registry path is never created."""
    scratch = str(tmp_path / "scratch" / "rag_state.db")
    live_registry = tmp_path / "live" / "hub_registry.db"

    code = sync_module._run_against_hub(
        _config(), scratch, str(live_registry), _clients(_hub()), _args(rebuild_from_hub=True),
    )

    assert code == sync_module.EXIT_OK
    assert os.path.exists(scratch)
    assert not live_registry.exists()
    assert not live_registry.parent.exists()


# ---------------------------------------------------------------------------
# Startup: a hub that cannot be reached stops the run before anything is written
# ---------------------------------------------------------------------------


def test_an_unreachable_hub_at_startup_exits_2_with_nothing_written(sync_module, tmp_path, monkeypatch):
    """A real HubClient whose transport raises ConnectError. Nothing is scanned, polled or written."""
    db_path = str(tmp_path / "state" / "rag_state.db")
    registry_path = str(tmp_path / "state" / "hub_registry.db")
    secret = _write_secret(tmp_path / "secrets" / "nas")

    def refuse(request):
        raise httpx.ConnectError("connection refused", request=request)

    http = httpx.Client(base_url="http://hub.invalid:8101", transport=httpx.MockTransport(refuse))
    clients = {source: HubClient("http://hub.invalid:8101", secret, http_client=http) for source in SOURCES}
    for name in ("scan_nfs_roots", "scan_claude_chats", "scan_opencode_sessions", "poll_outstanding", "submit_plan"):
        monkeypatch.setattr(sync_module, name, lambda *a, **k: pytest.fail(f"{name} ran with the hub down"))

    code = sync_module._run_against_hub(_config(), db_path, registry_path, clients, _args())

    assert code == sync_module.EXIT_HUB_UNAVAILABLE
    assert not os.path.exists(db_path)
    assert not os.path.exists(registry_path)


def test_a_startup_answer_of_401_is_a_hub_refusal_and_exits_2(sync_module, tmp_path, monkeypatch):
    """A secret the hub does not accept (401) fails the startup check the same way as an unreachable hub."""
    db_path = str(tmp_path / "state" / "rag_state.db")
    secret = _write_secret(tmp_path / "secrets" / "nas")

    def reject(request):
        return httpx.Response(401, json={"detail": "unknown source secret"})

    http = httpx.Client(base_url="http://hub.invalid:8101", transport=httpx.MockTransport(reject))
    clients = {source: HubClient("http://hub.invalid:8101", secret, http_client=http) for source in SOURCES}

    code = sync_module._run_against_hub(_config(), db_path, str(tmp_path / "state" / "hub_registry.db"), clients, _args())

    assert code == sync_module.EXIT_HUB_UNAVAILABLE
    assert not os.path.exists(db_path)


def test_hub_failures_name_each_failing_source(sync_module):
    hub = _hub()
    hub.info.side_effect = httpx.ConnectError("down")
    failures = sync_module.hub_startup_failures(_clients(hub))
    assert sorted(failures) == sorted(SOURCES)


# ---------------------------------------------------------------------------
# Secrets and configuration
# ---------------------------------------------------------------------------


def test_build_clients_makes_one_client_per_source_from_its_secret_file_and_never_shows_the_secret(sync_module, tmp_path):
    secret_text = "sekret-value-that-must-not-leak"
    config = _config()
    config["rag"]["nas_ingest"]["secrets"] = {
        source: _write_secret(tmp_path / "secrets" / source, secret_text) for source in SOURCES
    }

    clients = sync_module.build_clients(config)

    assert sorted(clients) == sorted(SOURCES)
    assert all(isinstance(client, HubClient) for client in clients.values())
    assert secret_text not in repr(clients[SOURCE_ID_NAS])


def test_build_clients_names_a_source_with_no_secret_path(sync_module, tmp_path):
    """The NAS source has its secret; the Claude source has no path at all, and the message names it."""
    config = _config()
    config["rag"]["nas_ingest"]["secrets"] = {SOURCE_ID_NAS: _write_secret(tmp_path / "secrets" / "nas")}

    with pytest.raises(ValueError, match=f"no path for source '{SOURCE_ID_CLAUDE_LINUX_SHARED_NFS}'"):
        sync_module.build_clients(config)


def test_build_clients_refuses_a_missing_secret_file(sync_module, tmp_path):
    config = _config()
    config["rag"]["nas_ingest"]["secrets"] = {source: str(tmp_path / "absent" / source) for source in SOURCES}

    with pytest.raises(OSError):
        sync_module.build_clients(config)


def test_build_clients_refuses_a_missing_nas_ingest_block(sync_module):
    with pytest.raises(ValueError, match="rag.nas_ingest"):
        sync_module.build_clients({"rag": {}})


def test_main_exits_3_on_an_unusable_config_after_taking_the_lock(sync_module, monkeypatch, tmp_path):
    """A config problem is a refusal (3), reported after the lock is taken and before the hub is contacted."""
    config = _config()
    monkeypatch.setattr(sync_module, "acquire_or_exit", lambda path: None)
    monkeypatch.setattr(sync_module, "nas_ingest_lock_path_from_config", lambda cfg: str(tmp_path / "x.lock"))
    monkeypatch.setattr(sync_module.yaml, "safe_load", lambda handle: config)
    monkeypatch.setattr(sys, "argv", ["rag_sync_nfs.py"])

    with pytest.raises(SystemExit) as exc_info:
        sync_module.main()

    assert exc_info.value.code == sync_module.EXIT_REFUSED


# ---------------------------------------------------------------------------
# Exit codes and the import contract
# ---------------------------------------------------------------------------


def test_exit_codes_are_the_documented_values(sync_module):
    assert sync_module.EXIT_OK == 0
    assert sync_module.EXIT_DELETION_REFUSED == 1
    assert sync_module.EXIT_HUB_UNAVAILABLE == 2
    assert sync_module.EXIT_REFUSED == 3
    assert EXIT_LOCK_HELD == 75


def test_the_script_loads_no_qdrant_client_and_has_no_apply_step(sync_module):
    """The import of the script in a fresh interpreter loads neither qdrant_client nor the embedder."""
    code = (
        "import importlib.util, json, sys\n"
        f"spec = importlib.util.spec_from_file_location('rag_sync_nfs', {str(REPO_ROOT / 'utils' / 'rag_sync_nfs.py')!r})\n"
        "module = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(module)\n"
        "heavy = [n for n in sys.modules if n == 'qdrant_client' or n.startswith('qdrant_client.') or n == 'docling' or n.startswith('docling.') or n == 'ragnart.embeddings']\n"
        "print(json.dumps({'heavy': heavy, 'apply': hasattr(module, 'apply_sync_plan'), 'get_client': hasattr(module, 'get_client')}))\n"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", code], cwd=REPO_ROOT, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == {"heavy": [], "apply": False, "get_client": False}
