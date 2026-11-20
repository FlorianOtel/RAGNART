"""
End-to-end tests for the filesystem endpoint runner (ragnart/endpoint.py) against the real hub API.

The hub side is the real ragnart.ingest_api app, driven through starlette's TestClient, which is an
httpx.Client. The endpoint side is ragnart.endpoint.run() (or main()), with its HubClient pointed at the
same TestClient. So every request the runner makes goes through the API's auth, validation and intake
writes, and the intake table is read back to see what the hub recorded.

The hub's worker is not run. Its outcome is simulated by writing the intake row's result through the
registry (update_intake), and by making the Qdrant listing show the item, exactly as the worker would.
Qdrant is a MagicMock: GET /v1/items returns whatever a test puts in its scroll result.

Nothing live is touched: tmp dirs, a tmp registry and a tmp spool only.
"""

from __future__ import annotations

import fcntl
import hashlib
import logging
import os
import shutil
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

import ragnart.cli as cli
import ragnart.endpoint as endpoint
import ragnart.ingest_api as ingest_api
from ragnart.cli import registry_db_path
from ragnart.endpoint_client import HubClient, HubError
from ragnart.endpoint_state import EndpointState
from ragnart.registry import RegistryDB

MBP_SECRET = "mbp-secret-for-filesystem-tests"
BASE_URL = "http://hub.test:8101"

# A fixed clock. The grace period is measured from here, so the tests never depend on the real time.
NOW = datetime(2026, 11, 19, 10, 0, tzinfo=timezone.utc)

LOGGER_NAME = "ragnart.endpoint"


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


@pytest.fixture(autouse=True)
def _reset_endpoint_logger():
    """main() attaches stream and file handlers to the endpoint logger. Remove them after each test."""
    yield
    logger = logging.getLogger(LOGGER_NAME)
    for handler in list(logger.handlers):
        handler.close()
    logger.handlers.clear()


# ---------------------------------------------------------------------------
# Hub fixtures (the same pattern as tests/test_endpoint_client.py)
# ---------------------------------------------------------------------------

@pytest.fixture
def hub_config(tmp_path, monkeypatch) -> dict:
    """A tmp ragnart.yaml: the include list and admission cap the API reads. A text file over 1000 bytes is refused."""
    cfg = {
        "db_base_path": str(tmp_path / "hub-db"),
        "rag": {
            "qdrant_url": "http://qdrant.invalid:6333",
            "admission": {
                "max_text_file_size_bytes": 1000,
                "max_chunks_per_item": 5000,
                "fffd_ratio_threshold": 0.005,
            },
            "scanner": {"include_extensions": [".md", ".txt"]},
        },
    }
    path = tmp_path / "ragnart.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    monkeypatch.setattr(cli, "CONFIG_PATH", path)
    return cfg


@pytest.fixture
def qdrant(monkeypatch) -> MagicMock:
    """A mocked Qdrant client. GET /v1/items reads its scroll result; by default the hub lists nothing."""
    client = MagicMock(name="qdrant_client")
    client.scroll.return_value = ([], None)
    monkeypatch.setattr(ingest_api, "get_client", lambda url, timeout=60: client)
    return client


@pytest.fixture
def hub(hub_config, qdrant):
    """The ingestion API with the MBP filesystem source registered. The lifespan has run."""
    registry = RegistryDB(registry_db_path(hub_config))
    registry.register_source("MBP", "filesystem", "endpoint", "florian", _sha256(MBP_SECRET))
    registry.close()
    with TestClient(ingest_api.app) as client:
        yield client


@pytest.fixture
def sent(hub) -> list:
    """Every request the endpoint sends to the hub, in order."""
    requests: list[httpx.Request] = []
    hub.event_hooks["request"].append(requests.append)
    return requests


# ---------------------------------------------------------------------------
# Endpoint fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def ws(tmp_path, hub, monkeypatch) -> SimpleNamespace:
    """
    A tmp endpoint host: a source root, a state database, a secret file and the endpoint.yaml.

    The HubClient the runner makes is wired to the TestClient, so a run talks to the real API.
    Paths are real paths (no symlink in the tmp prefix), because the runner keys items by real path.
    """
    base = Path(os.path.realpath(tmp_path))
    root = base / "MBP"
    root.mkdir()
    secret = base / "MBP.secret"
    secret.write_text(MBP_SECRET + "\n", encoding="utf-8")

    real_client = HubClient
    monkeypatch.setattr(
        endpoint, "HubClient",
        lambda base_url, secret_file, **kwargs: real_client(base_url, secret_file, http_client=hub, **kwargs),
    )
    workspace = SimpleNamespace(
        base=base,
        root=root,
        secret=secret,
        state_db=base / "state" / "endpoint.db",
        log=base / "logs" / "endpoint.log",
        config=base / "endpoint.yaml",
    )
    write_config(workspace)
    return workspace


def write_config(ws: SimpleNamespace, *, grace_hours: float = 24, ignore_file: Path | None = None,
                 connector: str = "filesystem", root: Path | None = None) -> Path:
    """Write endpoint.yaml for the MBP source. Options are changed per test."""
    entry = {
        "id": "MBP",
        "connector": connector,
        "root": str(root or ws.root),
        "secret_file": str(ws.secret),
    }
    if ignore_file is not None:
        entry["ignore_file"] = str(ignore_file)
    cfg = {
        "hub_url": BASE_URL,
        "state_db": str(ws.state_db),
        "log_file": str(ws.log),
        "grace_hours": grace_hours,
        "sources": [entry],
    }
    ws.config.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return ws.config


def run(ws: SimpleNamespace, **kwargs) -> int:
    """Run the endpoint once against the workspace's config. kwargs go to endpoint.run()."""
    return endpoint.run(endpoint.load_config(str(ws.config)), **kwargs)


# ---------------------------------------------------------------------------
# Reads and simulated worker steps
# ---------------------------------------------------------------------------

def intake_rows(hub_config: dict) -> list[dict]:
    """Every intake row the hub recorded, in insertion order. Read-only."""
    conn = sqlite3.connect(f"file:{registry_db_path(hub_config)}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute("SELECT * FROM intake ORDER BY rowid")]
    finally:
        conn.close()


def upserted_keys(hub_config: dict) -> set[str]:
    """The keys of the upsert rows the hub recorded."""
    return {row["key"] for row in intake_rows(hub_config) if row["kind"] == "upsert"}


def upserts_for(hub_config: dict, key: str) -> list[dict]:
    """The upsert rows the hub recorded for one key, oldest first."""
    return [row for row in intake_rows(hub_config) if row["kind"] == "upsert" and row["key"] == key]


def state_rows(ws: SimpleNamespace) -> dict[str, dict]:
    """The endpoint's own rows for the MBP source, read read-only."""
    state = EndpointState(str(ws.state_db), read_only=True)
    try:
        return state.rows("MBP")
    finally:
        state.close()


def point(key: str) -> SimpleNamespace:
    """A Qdrant point of the MBP source, as GET /v1/items reads it (payload only)."""
    return SimpleNamespace(payload={"source_path": key, "content_hash": "h", "pipeline_version": "p1"})


def hub_point(key: str, content_hash: str) -> SimpleNamespace:
    """Two Qdrant points of one item, as the hub lists it: the same hash and pipeline version on both."""
    payload = {"source_path": key, "content_hash": content_hash, "pipeline_version": "p1"}
    return [SimpleNamespace(payload=payload), SimpleNamespace(payload=payload)]


def complete(hub_config: dict, qdrant: MagicMock, request_id: str, key: str, chunk_count: int = 2) -> None:
    """
    Simulate the hub worker finishing one upsert: the intake row is completed with a chunk count,
    and the Qdrant listing shows the item, as it does once its points are written.
    """
    registry = RegistryDB(registry_db_path(hub_config))
    try:
        registry.update_intake(request_id, status="completed", chunk_count=chunk_count)
    finally:
        registry.close()
    qdrant.scroll.return_value = ([point(key)], None)


def settle_first_upload(ws, hub_config, qdrant, name: str = "notes.md") -> str:
    """
    Write one file, submit it, let the worker complete it, and run once more so the poll records it.
    Returns the file's key. The local row is then completed with two chunks.
    """
    key = str(ws.root / name)
    (ws.root / name).write_bytes(b"hello")
    assert run(ws, now=NOW) == 0
    complete(hub_config, qdrant, upserts_for(hub_config, key)[-1]["id"], key)
    assert run(ws, now=NOW) == 0
    assert state_rows(ws)[key]["status"] == "completed"
    return key


# ---------------------------------------------------------------------------
# Submit, status poll, edit, move, delete
# ---------------------------------------------------------------------------

def test_new_file_is_submitted_and_recorded_pending(ws, hub_config):
    """A new file is upserted: the hub records a row with its key and type, and the local row is pending with that request id."""
    (ws.root / "notes.md").write_bytes(b"hello")

    assert run(ws, now=NOW) == 0

    key = str(ws.root / "notes.md")
    request = upserts_for(hub_config, key)[0]
    assert len(upserts_for(hub_config, key)) == 1
    assert request["source_id"] == "MBP"
    assert request["file_type"] == "md"
    stat = (ws.root / "notes.md").stat()
    assert request["modified_at"] == datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
    row = state_rows(ws)[key]
    assert row["status"] == "pending"
    assert row["request_id"] == request["id"]
    assert row["content_hash"] == _md5(b"hello")


def test_status_poll_records_completed_and_an_unchanged_file_is_not_resubmitted(ws, hub_config, qdrant):
    """The poll records the worker's outcome. A completed, unchanged file is then left alone."""
    key = str(ws.root / "notes.md")
    (ws.root / "notes.md").write_bytes(b"hello")
    assert run(ws, now=NOW) == 0
    complete(hub_config, qdrant, upserts_for(hub_config, key)[0]["id"], key, chunk_count=2)

    assert run(ws, now=NOW) == 0

    row = state_rows(ws)[key]
    assert (row["status"], row["chunk_count"], row["reason"]) == ("completed", 2, None)
    assert len(intake_rows(hub_config)) == 1, "no second request for an unchanged file"


def test_poll_records_an_ignored_outcome_with_its_admission_reason(ws, hub_config, qdrant):
    """A request the worker ignores (admission) is recorded ignored, with the admission reason."""
    key = str(ws.root / "notes.md")
    (ws.root / "notes.md").write_bytes(b"hello")
    assert run(ws, now=NOW) == 0
    registry = RegistryDB(registry_db_path(hub_config))
    registry.update_intake(upserts_for(hub_config, key)[0]["id"], status="ignored",
                           admission_reason="too many chunks")
    registry.close()

    assert run(ws, now=NOW) == 0

    row = state_rows(ws)[key]
    assert (row["status"], row["reason"]) == ("ignored", "too many chunks")
    assert len(upserts_for(hub_config, key)) == 1, "an ignored file is not resubmitted while unchanged"


def test_failed_outcome_is_submitted_again(ws, hub_config):
    """A request the worker failed is recorded failed, and the unchanged file is submitted again."""
    key = str(ws.root / "notes.md")
    (ws.root / "notes.md").write_bytes(b"hello")
    assert run(ws, now=NOW) == 0
    registry = RegistryDB(registry_db_path(hub_config))
    registry.update_intake(upserts_for(hub_config, key)[0]["id"], status="failed", error="worker crashed (simulated)")
    registry.close()

    assert run(ws, now=NOW) == 0

    assert len(upserts_for(hub_config, key)) == 2
    row = state_rows(ws)[key]
    assert row["status"] == "pending"
    assert row["request_id"] == upserts_for(hub_config, key)[-1]["id"]


def test_request_the_hub_does_not_know_is_failed_and_submitted_again(ws, hub_config):
    """A pending row whose request id the hub does not know (404) becomes failed, and the file is submitted again."""
    key = str(ws.root / "notes.md")
    (ws.root / "notes.md").write_bytes(b"hello")
    assert run(ws, now=NOW) == 0
    first_request = upserts_for(hub_config, key)[0]["id"]
    # Remove the intake row behind the hub's back, so the status endpoint answers 404 for it.
    conn = sqlite3.connect(registry_db_path(hub_config))
    with conn:
        conn.execute("DELETE FROM intake WHERE id = ?", (first_request,))
    conn.close()

    assert run(ws, now=NOW) == 0

    # The vanished request has no intake row left, so the one upsert there is the re-submission.
    # Its id is the one the row now carries, and the row is pending again.
    new_requests = upserts_for(hub_config, key)
    assert len(new_requests) == 1
    assert new_requests[0]["id"] != first_request
    row = state_rows(ws)[key]
    assert (row["status"], row["request_id"]) == ("pending", new_requests[0]["id"])


def test_refused_at_the_door_is_recorded_ignored_with_the_reason(ws, hub_config):
    """A file over the text cap (413) is not queued: its row is ignored with the hub's reason, and no spool file is left."""
    (ws.root / "big.md").write_bytes(b"x" * 2000)

    assert run(ws, now=NOW) == 0

    row = state_rows(ws)[str(ws.root / "big.md")]
    assert row["status"] == "ignored"
    assert "larger than" in row["reason"]
    assert intake_rows(hub_config) == []
    assert list((Path(hub_config["db_base_path"]) / "spool").iterdir()) == []


def test_refused_edit_of_an_indexed_file_deletes_its_old_points(ws, hub_config, qdrant):
    """
    An indexed file edited into a refusal (413) has its old points deleted, then its row is ignored with the reason.

    Same rule as the hub's own path: an item that becomes ignored loses its old points (ingest.py deletes before admission).
    """
    key = settle_first_upload(ws, hub_config, qdrant)
    (ws.root / "notes.md").write_bytes(b"x" * 2000)

    assert run(ws, now=NOW) == 0

    deletes = [row["key"] for row in intake_rows(hub_config) if row["kind"] == "delete"]
    assert deletes == [key], "the old points of the indexed file are deleted"
    row = state_rows(ws)[key]
    assert row["status"] == "ignored"
    assert "larger than" in row["reason"]


def test_refused_edit_of_a_file_the_hub_lists_deletes_its_old_points(ws, hub_config, qdrant):
    """
    A refused upsert deletes the old points when the hub lists the key, even if the local row says it had none.

    The row is completed with zero chunks here, and the hub's listing still shows the key.
    """
    key = str(ws.root / "notes.md")
    (ws.root / "notes.md").write_bytes(b"hello")
    assert run(ws, now=NOW) == 0
    complete(hub_config, qdrant, upserts_for(hub_config, key)[0]["id"], key, chunk_count=0)
    assert run(ws, now=NOW) == 0
    assert state_rows(ws)[key]["chunk_count"] == 0
    (ws.root / "notes.md").write_bytes(b"x" * 2000)

    assert run(ws, now=NOW) == 0

    deletes = [row["key"] for row in intake_rows(hub_config) if row["kind"] == "delete"]
    assert deletes == [key]
    assert state_rows(ws)[key]["status"] == "ignored"


def test_edited_file_is_submitted_again(ws, hub_config, qdrant):
    """A changed file (its content hash differs from the row's) is upserted again, and the row takes the new hash."""
    key = settle_first_upload(ws, hub_config, qdrant)
    (ws.root / "notes.md").write_bytes(b"a longer, edited body")

    assert run(ws, now=NOW) == 0

    assert len(upserts_for(hub_config, key)) == 2
    row = state_rows(ws)[key]
    assert row["status"] == "pending"
    assert row["content_hash"] == _md5(b"a longer, edited body")


def test_moved_file_is_a_change_key_not_an_upsert(ws, hub_config, qdrant):
    """A file moved with the same content is a change_key: the hub keeps its points, and no new upsert is sent."""
    old_key = settle_first_upload(ws, hub_config, qdrant)
    (ws.root / "sub").mkdir()
    new_path = ws.root / "sub" / "notes.md"
    os.rename(ws.root / "notes.md", new_path)
    new_key = str(new_path)

    assert run(ws, now=NOW) == 0

    changes = [row for row in intake_rows(hub_config) if row["kind"] == "change_key"]
    assert [(row["key"], row["new_key"]) for row in changes] == [(old_key, new_key)]
    assert len(upserts_for(hub_config, old_key)) == 1
    assert upserts_for(hub_config, new_key) == [], "the moved file was not uploaded again"
    rows = state_rows(ws)
    assert old_key not in rows
    assert rows[new_key]["status"] == "pending"
    assert rows[new_key]["chunk_count"] == 2, "the moved row keeps the chunk count it had"


def test_deleted_file_is_marked_missing_then_deleted_after_grace(ws, hub_config, qdrant):
    """A vanished file is marked missing on the first sighting, and deleted by a run after the grace period."""
    key = settle_first_upload(ws, hub_config, qdrant)
    # A second file stays, so the walk still finds items: an empty walk is incomplete (see the empty-walk test).
    (ws.root / "keep.md").write_bytes(b"still here")
    assert run(ws, now=NOW) == 0
    os.remove(ws.root / "notes.md")

    assert run(ws, now=NOW) == 0
    assert state_rows(ws)[key]["missing_since"] == NOW.isoformat(timespec="seconds")
    assert not any(row["kind"] == "delete" for row in intake_rows(hub_config)), "still inside the grace period"

    assert run(ws, now=NOW + timedelta(hours=25)) == 0
    deletes = [row["key"] for row in intake_rows(hub_config) if row["kind"] == "delete"]
    assert deletes == [key]
    assert key not in state_rows(ws)


def test_empty_walk_with_state_rows_is_incomplete(ws, hub_config, qdrant, caplog):
    """
    A walk that finds no file while the state holds rows for the source is incomplete (design docs/RAGNART-design.md §4.7).

    Here the only file is gone, so the walk finds nothing. Past the grace period, the row must still be
    neither marked missing nor deleted, and a WARNING must say why.
    """
    key = settle_first_upload(ws, hub_config, qdrant)
    os.remove(ws.root / "notes.md")
    caplog.set_level(logging.WARNING, logger=LOGGER_NAME)

    assert run(ws, now=NOW + timedelta(hours=48)) == 0

    row = state_rows(ws)[key]
    assert row["missing_since"] is None
    assert not any(r["kind"] == "delete" for r in intake_rows(hub_config))
    assert any(
        record.levelname == "WARNING" and "found no item" in record.getMessage()
        for record in caplog.records
    )


def test_reappearance_within_grace_clears_missing_since(ws, hub_config, qdrant):
    """A file that comes back before the grace period ends is not deleted, and its missing mark is cleared."""
    key = settle_first_upload(ws, hub_config, qdrant)
    os.remove(ws.root / "notes.md")
    assert run(ws, now=NOW) == 0

    (ws.root / "notes.md").write_bytes(b"hello")  # the same bytes, so the same content hash
    assert run(ws, now=NOW + timedelta(hours=1)) == 0

    row = state_rows(ws)[key]
    assert row["missing_since"] is None
    assert not any(r["kind"] == "delete" for r in intake_rows(hub_config))
    assert len(upserts_for(hub_config, key)) == 1, "same content: no new upload"


# ---------------------------------------------------------------------------
# Reconcile against the hub's listing
# ---------------------------------------------------------------------------

def test_hub_key_unknown_locally_gets_a_delete_request(ws, hub_config, qdrant):
    """A key the hub lists with no local row is deleted, while a key the local state knows is left alone."""
    key = settle_first_upload(ws, hub_config, qdrant)
    ghost = str(ws.root / "ghost.md")
    qdrant.scroll.return_value = ([point(key), point(ghost)], None)

    assert run(ws, now=NOW) == 0

    deletes = [row["key"] for row in intake_rows(hub_config) if row["kind"] == "delete"]
    assert deletes == [ghost]
    assert key in state_rows(ws)


def test_empty_local_state_never_deletes_hub_items(ws, hub_config, qdrant):
    """With no local rows at all (a lost state database), the hub's items are never deleted by the orphan rule."""
    qdrant.scroll.return_value = ([point(str(ws.root / "precious.md"))], None)

    assert run(ws, now=NOW) == 0

    assert not any(row["kind"] == "delete" for row in intake_rows(hub_config))


def test_completed_row_the_hub_no_longer_lists_is_submitted_again(ws, hub_config, qdrant):
    """A completed row with chunks, whose item the hub no longer lists, is uploaded again while the file is on disk."""
    key = settle_first_upload(ws, hub_config, qdrant)
    qdrant.scroll.return_value = ([], None)

    assert run(ws, now=NOW) == 0

    assert len(upserts_for(hub_config, key)) == 2
    assert state_rows(ws)[key]["status"] == "pending"


# ---------------------------------------------------------------------------
# What is walked
# ---------------------------------------------------------------------------

def test_excluded_names_and_ragignore_are_skipped(ws, hub_config):
    """._x, ~$x, .DS_Store, anything under .git, and a .ragignore match are skipped. An ordinary name with a mid-name ._ is admitted."""
    (ws.root / "ok.md").write_text("ok")
    (ws.root / "Geoffrey_A._Moore.md").write_text("mid-name dot underscore")
    (ws.root / "._sidecar.md").write_text("apple double")
    (ws.root / "~$lock.md").write_text("office lock")
    (ws.root / ".DS_Store").write_text("finder")
    (ws.root / ".git").mkdir()
    (ws.root / ".git" / "inside.md").write_text("git")
    (ws.root / "draft.md").write_text("ignored by .ragignore")
    (ws.root / ".ragignore").write_text("draft.md\n")

    assert run(ws, now=NOW) == 0

    assert upserted_keys(hub_config) == {str(ws.root / "ok.md"), str(ws.root / "Geoffrey_A._Moore.md")}


def test_global_ignore_file_applies_when_configured(ws, hub_config):
    """A source's ignore_file is a global layer: its patterns apply to the whole root."""
    global_ignore = ws.base / "global.ragignore"
    global_ignore.write_text("secret*.md\n", encoding="utf-8")
    write_config(ws, ignore_file=global_ignore)
    (ws.root / "secret1.md").write_text("no")
    (ws.root / "open.md").write_text("yes")

    assert run(ws, now=NOW) == 0

    assert upserted_keys(hub_config) == {str(ws.root / "open.md")}


def test_symlinks_out_of_the_root_are_not_followed(ws, hub_config):
    """A file symlink pointing outside the root, and a folder symlink to outside, contribute nothing."""
    outside = ws.base / "outside"
    outside.mkdir()
    (outside / "leak.md").write_text("outside the root")
    os.symlink(outside / "leak.md", ws.root / "link.md")
    os.symlink(outside, ws.root / "linkdir")

    assert run(ws, now=NOW) == 0

    assert upserted_keys(hub_config) == set()


def test_volumes_paths_are_never_walked(ws, hub_config):
    """The /Volumes guard refuses a real path under /Volumes, and a path that only shares a prefix with the root."""
    assert endpoint.allowed_real_path("/Volumes/NFS Disks/Florian/x.md", "/") is False
    assert endpoint.allowed_real_path("/Volumes", "/") is False
    assert endpoint.allowed_real_path("/Volumes/x", "/Volumes") is False
    assert endpoint.allowed_real_path("/Users/fotel/MBP/a.md", "/Users/fotel/MBP") is True
    assert endpoint.allowed_real_path("/Users/fotel/MBP-old/a.md", "/Users/fotel/MBP") is False
    with pytest.raises(endpoint.SourceError, match="never walked"):
        endpoint._checked_root("/Volumes/NFS Disks/Florian")


def test_a_source_rooted_under_volumes_is_refused_before_any_hub_call(ws, hub_config, sent, caplog):
    """A root under /Volumes fails its source: the run logs it, returns EXIT_FAILED, and sends no request."""
    write_config(ws, root=Path("/Volumes/NFS Disks/Florian"))
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)

    assert run(ws, now=NOW) == endpoint.EXIT_FAILED

    assert sent == []
    assert "never walked" in caplog.text


def test_missing_root_changes_no_state(ws, hub_config, qdrant):
    """A root that disappears (an unmounted disk, say) fails its source. Its items are not treated as vanished."""
    key = settle_first_upload(ws, hub_config, qdrant)
    shutil.rmtree(ws.root)

    assert run(ws, now=NOW + timedelta(hours=48)) == endpoint.EXIT_FAILED

    assert state_rows(ws)[key]["status"] == "completed"
    assert not any(row["kind"] == "delete" for row in intake_rows(hub_config))


def test_unreadable_file_makes_the_walk_incomplete(ws, hub_config, qdrant, monkeypatch):
    """A file that cannot be read marks the walk incomplete, so no item is treated as vanished on that run."""
    key = settle_first_upload(ws, hub_config, qdrant)

    real_describe = endpoint._describe_file

    def unreadable(real, file_type, row):
        if real == key:
            raise OSError("permission denied (simulated)")
        return real_describe(real, file_type, row)

    monkeypatch.setattr(endpoint, "_describe_file", unreadable)
    assert run(ws, now=NOW + timedelta(hours=48)) == 0

    assert state_rows(ws)[key]["missing_since"] is None
    assert not any(row["kind"] == "delete" for row in intake_rows(hub_config))


# ---------------------------------------------------------------------------
# Project markers
# ---------------------------------------------------------------------------

def test_project_marker_is_refused_and_its_files_are_still_submitted(ws, hub_config, sent, caplog):
    """A project.toml is offered to the hub and refused, logged at WARNING. The files beside it are submitted as ordinary items."""
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)
    project = ws.root / "proj"
    (project / ".ragnart").mkdir(parents=True)
    (project / ".ragnart" / "project.toml").write_text('name = "Demo"\nid = "demo"\n', encoding="utf-8")
    (project / "notes.md").write_text("inside the project")

    assert run(ws, now=NOW) == 0

    assert (sent[-1].method, sent[-1].url.path) == ("POST", "/v1/projects")
    assert any(
        record.levelname == "WARNING" and "refused by the hub: project records from MBP are not supported yet" in record.getMessage()
        for record in caplog.records
    )
    assert upserted_keys(hub_config) == {str(project / "notes.md")}


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def test_dry_run_writes_nothing_and_prints_the_plan(ws, hub_config, qdrant, sent, caplog):
    """A dry run makes no hub write and no state write: the state rows and the intake table are unchanged, and the plan is logged."""
    settle_first_upload(ws, hub_config, qdrant)
    before = state_rows(ws)
    intake_before = intake_rows(hub_config)
    (ws.root / "notes.md").write_bytes(b"edited, so a real run would upload it")
    (ws.root / "fresh.md").write_text("new")
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)
    sent.clear()

    assert run(ws, now=NOW, dry_run=True) == 0

    assert intake_rows(hub_config) == intake_before
    assert state_rows(ws) == before
    assert {request.method for request in sent} == {"GET"}, "a dry run only reads"
    assert "would upsert " + str(ws.root / "notes.md") in caplog.text
    assert "would upsert " + str(ws.root / "fresh.md") in caplog.text


def test_dry_run_on_a_fresh_host_creates_no_state_file(ws, hub_config):
    """A dry run with no state database yet does not create one."""
    (ws.root / "notes.md").write_text("hello")

    assert run(ws, now=NOW, dry_run=True) == 0

    assert not ws.state_db.exists()
    assert intake_rows(hub_config) == []


# ---------------------------------------------------------------------------
# The run lock and the command line
# ---------------------------------------------------------------------------

def test_second_concurrent_run_exits_75(ws, hub_config, capsys):
    """While one run holds the lock on the state database, a second run exits 75 and does nothing. Once the lock is free, it runs."""
    (ws.root / "notes.md").write_text("hello")
    ws.state_db.parent.mkdir(parents=True, exist_ok=True)
    held = open(str(ws.state_db) + ".lock", "a", encoding="utf-8")
    fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert endpoint.main(["run", "--config", str(ws.config)]) == endpoint.EXIT_LOCK_HELD
        assert intake_rows(hub_config) == []
        assert "holds the lock" in capsys.readouterr().out
    finally:
        held.close()

    assert endpoint.main(["run", "--config", str(ws.config)]) == endpoint.EXIT_OK
    assert upserted_keys(hub_config) == {str(ws.root / "notes.md")}


@pytest.mark.parametrize("patch, message", [
    ({"sources": [{"id": "MBP", "connector": "filesystem", "root": "/x", "secret_file": "/s"},
                  {"id": "mbp", "connector": "filesystem", "root": "/y", "secret_file": "/s"}]}, "duplicate source id"),
    ({"sources": [{"id": "bad:id", "connector": "filesystem", "root": "/x", "secret_file": "/s"}]}, "source id"),
    ({"sources": [{"id": "MBP", "connector": "opencode", "root": "/x", "secret_file": "/s"}]}, "connector"),
    ({"sources": [{"id": "MBP", "connector": "filesystem", "root": "/x", "secret_file": "/s", "typo": 1}]}, "unknown key"),
    ({"grace_hours": -1}, "grace_hours"),
    ({"grace_hours": True}, "grace_hours"),
    ({"surprise": 1}, "unknown key"),
])
def test_invalid_config_is_refused(tmp_path, patch, message):
    """Each invalid config is a ConfigError that names the problem. Every case otherwise has a valid source list."""
    cfg = {
        "hub_url": BASE_URL,
        "state_db": str(tmp_path / "s.db"),
        "sources": [{"id": "MBP", "connector": "filesystem", "root": "/x", "secret_file": "/s"}],
    }
    cfg.update(patch)
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")

    with pytest.raises(endpoint.ConfigError, match=message):
        endpoint.load_config(str(path))


# ---------------------------------------------------------------------------
# Session B2 (A.18): a 422 is a request error, nothing is deleted, the row is left for the next run
# ---------------------------------------------------------------------------

def _refuse_as_bad_request(action, source, client):
    """Stands in for the upload: the hub answers 422, as it does for a malformed request."""
    raise HubError(422, "bad request (test)")


def test_422_on_a_new_file_records_no_row_and_the_next_run_submits_it(ws, hub_config, monkeypatch, caplog):
    """A 422 is not an admission decision: no row is recorded and an ERROR names the cause. The next run sends it."""
    (ws.root / "notes.md").write_bytes(b"hello")
    real_send = endpoint._send_upsert
    monkeypatch.setattr(endpoint, "_send_upsert", _refuse_as_bad_request)

    with caplog.at_level(logging.ERROR, logger=LOGGER_NAME):
        assert run(ws, now=NOW) == 0

    assert intake_rows(hub_config) == []
    assert state_rows(ws) == {}
    assert any(record.levelno == logging.ERROR and "422" in record.getMessage() for record in caplog.records)

    monkeypatch.setattr(endpoint, "_send_upsert", real_send)
    assert run(ws, now=NOW) == 0
    assert len(upserts_for(hub_config, str(ws.root / "notes.md"))) == 1


def test_422_on_an_edited_file_deletes_nothing_and_keeps_the_old_row(ws, hub_config, qdrant, monkeypatch):
    """An indexed file whose edit is answered 422: no delete is sent, and the row keeps its old completed state and hash."""
    key = settle_first_upload(ws, hub_config, qdrant)
    (ws.root / "notes.md").write_bytes(b"a longer, edited body")
    monkeypatch.setattr(endpoint, "_send_upsert", _refuse_as_bad_request)

    assert run(ws, now=NOW) == 0

    assert [row for row in intake_rows(hub_config) if row["kind"] == "delete"] == []
    row = state_rows(ws)[key]
    assert (row["status"], row["content_hash"]) == ("completed", _md5(b"hello"))


# ---------------------------------------------------------------------------
# Session B2 (A.18): re-submission after a pipeline version change, capped per run
# ---------------------------------------------------------------------------

def test_completed_item_on_an_older_pipeline_is_submitted_again(ws, hub_config, qdrant, monkeypatch):
    """/v1/info reports p2 while the points say p1: the completed item is sent again, and its row is pending."""
    key = settle_first_upload(ws, hub_config, qdrant)
    monkeypatch.setattr(ingest_api, "PIPELINE_VERSION_P1", "p2")

    assert run(ws, now=NOW) == 0

    assert len(upserts_for(hub_config, key)) == 2
    assert state_rows(ws)[key]["status"] == "pending"


def test_older_pipeline_resubmission_is_capped_per_run(ws, hub_config, qdrant, monkeypatch):
    """Three items on p1 with /v1/info at p2 and a batch of two: two are sent in the first run, the third in the next."""
    names = ["a.md", "b.md", "c.md"]
    keys = [str(ws.root / name) for name in names]
    for name in names:
        (ws.root / name).write_bytes(name.encode("utf-8"))
    assert run(ws, now=NOW) == 0
    registry = RegistryDB(registry_db_path(hub_config))
    try:
        for key in keys:
            registry.update_intake(upserts_for(hub_config, key)[0]["id"], status="completed", chunk_count=2)
    finally:
        registry.close()
    qdrant.scroll.return_value = ([point(key) for key in keys], None)
    assert run(ws, now=NOW) == 0
    monkeypatch.setattr(ingest_api, "PIPELINE_VERSION_P1", "p2")

    assert run(ws, now=NOW, resubmit_batch_size=2) == 0
    assert [len(upserts_for(hub_config, key)) for key in keys] == [2, 2, 1]

    # The first two are still waiting for a worker, so only the third is due now.
    assert run(ws, now=NOW, resubmit_batch_size=2) == 0
    assert [len(upserts_for(hub_config, key)) for key in keys] == [2, 2, 2]


@pytest.mark.parametrize("value", [0, -1, True, "200", 1.5])
def test_resubmit_batch_size_must_be_a_whole_number_of_one_or_more(ws, value):
    """A zero, negative, boolean, text or fractional batch size is a configuration error."""
    cfg = yaml.safe_load(ws.config.read_text(encoding="utf-8"))
    cfg["resubmit_batch_size"] = value
    ws.config.write_text(yaml.safe_dump(cfg), encoding="utf-8")

    with pytest.raises(endpoint.ConfigError):
        endpoint.load_config(str(ws.config))


# ---------------------------------------------------------------------------
# Session B2 (A.18): --rebuild-from-hub
# ---------------------------------------------------------------------------

def _rebuild(ws: SimpleNamespace):
    return endpoint.rebuild_from_hub(endpoint.load_config(str(ws.config)))


def test_rebuild_refuses_a_state_that_already_holds_rows_before_any_hub_call(ws, hub_config, sent):
    """A state with rows is never rebuilt over: the refusal comes before the hub is asked for its listing."""
    (ws.root / "notes.md").write_bytes(b"hello")
    assert run(ws, now=NOW) == 0
    before = len(sent)

    with pytest.raises(RuntimeError, match="already holds"):
        _rebuild(ws)

    assert len(sent) == before


def test_rebuild_restores_a_file_row_from_the_hub_and_sends_no_upload(ws, hub_config, qdrant, sent):
    """The restored row carries the hub's hash, and the rebuild itself sends only the listing read."""
    key = str(ws.root / "notes.md")
    (ws.root / "notes.md").write_bytes(b"hello")
    qdrant.scroll.return_value = (hub_point(key, _md5(b"hello")), None)
    before = len(sent)

    counts = _rebuild(ws)

    assert counts["restored"] == 1
    assert [request.method for request in sent[before:]] == ["GET"]
    row = state_rows(ws)[key]
    assert (row["status"], row["content_hash"], row["chunk_count"]) == ("completed", _md5(b"hello"), 2)


def test_after_a_file_rebuild_an_unchanged_file_is_not_submitted_again(ws, hub_config, qdrant):
    """The placeholder stat makes the next run hash the file; equal to the hub's hash, it is not sent again."""
    key = str(ws.root / "notes.md")
    (ws.root / "notes.md").write_bytes(b"hello")
    qdrant.scroll.return_value = (hub_point(key, _md5(b"hello")), None)
    _rebuild(ws)

    assert run(ws, now=NOW) == 0

    assert intake_rows(hub_config) == []
    assert state_rows(ws)[key]["size"] == len(b"hello")


def test_after_a_file_rebuild_a_file_that_differs_from_the_hub_is_submitted(ws, hub_config, qdrant):
    """A file whose bytes differ from the hub's hash is found changed on the next run, and is submitted."""
    key = str(ws.root / "notes.md")
    (ws.root / "notes.md").write_bytes(b"edited while the state was lost")
    qdrant.scroll.return_value = (hub_point(key, _md5(b"hello")), None)
    _rebuild(ws)

    assert run(ws, now=NOW) == 0

    assert len(upserts_for(hub_config, key)) == 1


def test_a_file_missing_after_a_rebuild_is_marked_missing_by_the_next_run(ws, hub_config, qdrant):
    """A restored file row with no file on disk is missing on the next run, and its grace period starts then."""
    gone = str(ws.root / "gone.md")
    (ws.root / "other.md").write_bytes(b"still here")
    qdrant.scroll.return_value = (hub_point(gone, _md5(b"old")), None)
    _rebuild(ws)

    assert run(ws, now=NOW) == 0

    assert state_rows(ws)[gone]["missing_since"] == NOW.isoformat(timespec="seconds")


def test_the_command_line_refuses_a_rebuild_over_a_non_empty_state(ws, hub_config):
    """main(run --rebuild-from-hub) on a state with rows logs the refusal and exits 1; nothing is restored."""
    (ws.root / "notes.md").write_bytes(b"hello")
    assert run(ws, now=NOW) == 0

    code = endpoint.main(["run", "--config", str(ws.config), "--rebuild-from-hub"])

    assert code == endpoint.EXIT_FAILED
    assert len(state_rows(ws)) == 1



# ---------------------------------------------------------------------------
# An unreachable hub is one log line; any other failure keeps its traceback
# ---------------------------------------------------------------------------

def _raise_connect_error(action, source, client):
    """Stands in for the upload: the hub cannot be reached."""
    raise httpx.ConnectError("All connection attempts failed")


def test_unreachable_hub_logs_one_line_without_a_traceback(ws, hub_config, monkeypatch, caplog):
    """A transport failure gives exactly one ERROR record, with no exc_info, and the run is marked failed."""
    (ws.root / "notes.md").write_bytes(b"hello")
    monkeypatch.setattr(endpoint, "_send_upsert", _raise_connect_error)

    with caplog.at_level(logging.ERROR, logger=LOGGER_NAME):
        assert run(ws, now=NOW) == endpoint.EXIT_FAILED

    records = [r for r in caplog.records if r.name == LOGGER_NAME]
    assert len(records) == 1
    assert records[0].exc_info is None
    assert records[0].getMessage() == (
        f"MBP: hub unreachable (ConnectError: All connection attempts failed); "
        "nothing sent, the next run retries"
    )


def test_another_failure_still_logs_a_traceback(ws, hub_config, monkeypatch, caplog):
    """Anything that is not a transport error keeps logger.exception, so the traceback is in the log."""
    (ws.root / "notes.md").write_bytes(b"hello")

    def boom(action, source, client):
        raise RuntimeError("boom")

    monkeypatch.setattr(endpoint, "_send_upsert", boom)
    with caplog.at_level(logging.ERROR, logger=LOGGER_NAME):
        assert run(ws, now=NOW) == endpoint.EXIT_FAILED

    records = [r for r in caplog.records if r.name == LOGGER_NAME]
    assert len(records) == 1
    assert records[0].exc_info is not None
