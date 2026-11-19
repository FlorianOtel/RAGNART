"""
Tests for the deletion guards in utils/rag_sync_nfs.py (sync()) and StateDB.find_deleted().

Everything runs on a temporary SQLite file and a MagicMock HubClient (one mock stands for all three
NAS-side sources). The production queue at db_base_path is never opened, and no hub is contacted.

The deletes, moves and attachments are requests to the ingestion API, asserted on the hub mock.
The config has no opencode or claude_chats section, so those scans return at once
without network access.
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ragnart.endpoint_client import HubClient
from ragnart.registry import RegistryDB
from ragnart.scanner import scan_nfs_roots
from ragnart.schema import SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_NAS, SOURCE_ID_OPENCODE_K11
from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent
OWNER = "tester"


@pytest.fixture
def sync_module():
    """Load utils/rag_sync_nfs.py as a module. It is a script, not a package."""
    spec = importlib.util.spec_from_file_location("rag_sync_nfs", REPO_ROOT / "utils" / "rag_sync_nfs.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # Reconcile has its own tests (tests/test_sync_reconcile.py). These tests are about the deletion guards of the
    # scan and plan, so reconcile is stood down here. A hub listing that matched every local row would be needed
    # otherwise, and it would only add resends that these tests do not look at.
    module.reconcile_source = _no_reconcile
    return module


def _no_reconcile(*args, **kwargs) -> Counter:
    """Stands in for reconcile_source in these tests: it reports nothing and sends nothing."""
    return Counter()


WRITE_METHODS = (
    "upsert_bytes", "upsert_text", "delete", "change_key", "change_metadata", "publish_project", "remove_project",
)


def _write_calls(hub: MagicMock) -> list[str]:
    """The names of every write method the fake hub was called with. A dry run must leave this list empty."""
    return [name for name, _args, _kwargs in hub.mock_calls if name in WRITE_METHODS]


@pytest.fixture
def hub() -> MagicMock:
    """
    A HubClient stand-in for all three NAS-side sources. Writes are accepted with a request id, as the API answers.
    Reads answer an empty listing and a 'received' status. Use reset_mock() to clear the calls, not a new mock.
    """
    client = MagicMock(spec=HubClient)
    client.upsert_bytes.return_value = {"request_id": "req-test", "status": "received"}
    client.upsert_text.return_value = {"request_id": "req-test", "status": "received"}
    client.status.return_value = {"status": "received"}
    client.info.return_value = {"pipeline_version": "p1"}
    client.items.return_value = []
    return client


@pytest.fixture
def state_db(tmp_path):
    db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    yield db
    db.close()


@pytest.fixture
def registry_db(tmp_path):
    # Kept apart from the queue's directory, so "the live registry does not exist" stays testable.
    db = RegistryDB(str(tmp_path / "registry" / "hub_registry.db"))
    yield db
    db.close()


def _config(root, max_delete_fraction: float = 0.10) -> dict:
    """A ragnart.yaml-shaped dict with one user root and the sync guard key set."""
    return {
        "rag": {
            "nas_ingest": {"hub_url": "http://hub.invalid:8101", "secrets": {}, "resubmit_batch_size": 200},
            "scanner": {"include_extensions": [".md"]},
            "sync": {"max_delete_fraction": max_delete_fraction},
        },
        "users": {"tester@example.com": {"owner": OWNER, "nfs_roots": [str(root)]}},
    }


def _args(allow_mass_delete: bool = False, dry_run: bool = False) -> argparse.Namespace:
    return argparse.Namespace(user=OWNER, allow_mass_delete=allow_mass_delete, dry_run=dry_run)


def _make_files(root: Path, count: int) -> list[str]:
    root.mkdir(parents=True, exist_ok=True)
    paths = []
    for index in range(count):
        path = root / f"f{index:02d}.md"
        path.write_text(f"file {index}", encoding="utf-8")
        paths.append(str(path))
    return paths


def _complete_all(state_db: StateDB) -> None:
    """Mark every queued row completed, as if each file had been ingested."""
    for row in state_db._conn.execute("SELECT file_path FROM ingestion_queue").fetchall():
        state_db.mark_completed(row["file_path"])


def _rows(state_db: StateDB) -> dict[str, str]:
    """Return {file_path: status} for every queued row."""
    return {
        row["file_path"]: row["status"]
        for row in state_db._conn.execute("SELECT file_path, status FROM ingestion_queue").fetchall()
    }


def _missing(state_db: StateDB) -> set[str]:
    """Paths of every tombstoned row (missing_since set)."""
    return {
        row["file_path"]
        for row in state_db._conn.execute(
            "SELECT file_path FROM ingestion_queue WHERE missing_since IS NOT NULL"
        ).fetchall()
    }


def _age_tombstones(state_db: StateDB, hours: int) -> None:
    """Move every tombstone's missing_since back by `hours`, as if that much time had passed."""
    aged = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    state_db._conn.execute(
        "UPDATE ingestion_queue SET missing_since = ? WHERE missing_since IS NOT NULL", (aged,)
    )
    state_db._conn.commit()


def _clients(hub: MagicMock) -> dict:
    """One client per NAS-side source. All three are the same fake hub, so every call lands in one log."""
    return {SOURCE_ID_NAS: hub, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS: hub, SOURCE_ID_OPENCODE_K11: hub}


def _first_sync_completed(sync_module, state_db, registry_db, config, hub) -> None:
    """Run a clean sync over the tree, then mark everything it queued as completed. The call log is then cleared."""
    assert sync_module.sync(config, state_db, registry_db, _clients(hub), _args()) == 0
    _complete_all(state_db)
    hub.reset_mock()


# ----------------------------------------------------------------------------
# Item 1: skipped roots
# ----------------------------------------------------------------------------


def test_root_renamed_away_is_skipped_and_nothing_is_deleted(sync_module, state_db, registry_db, hub, tmp_path, caplog) -> None:
    root = tmp_path / "nas"
    _make_files(root, 3)
    config = _config(root)
    _first_sync_completed(sync_module, state_db, registry_db, config, hub)
    before = _rows(state_db)
    assert len(before) == 3

    # The mount disappears: the configured root is no longer a directory.
    os.rename(root, tmp_path / "nas-moved")

    with caplog.at_level(logging.ERROR, logger="rag_sync_nfs"):
        exit_code = sync_module.sync(config, state_db, registry_db, _clients(hub), _args())

    assert exit_code == sync_module.EXIT_DELETION_REFUSED
    assert _rows(state_db) == before
    hub.delete.assert_not_called()
    assert "Deletion phase SKIPPED" in caplog.text
    assert str(root) in caplog.text


def test_empty_root_with_rows_beneath_it_is_skipped(sync_module, state_db, registry_db, hub, tmp_path) -> None:
    """A directory that yields zero files, while the queue has rows under it, counts as unmounted."""
    root = tmp_path / "nas"
    paths = _make_files(root, 2)
    config = _config(root)
    _first_sync_completed(sync_module, state_db, registry_db, config, hub)
    before = _rows(state_db)

    for path in paths:
        os.remove(path)

    assert sync_module.sync(config, state_db, registry_db, _clients(hub), _args()) == sync_module.EXIT_DELETION_REFUSED
    assert _rows(state_db) == before
    hub.delete.assert_not_called()


def test_claude_root_unreachable_skips_whole_deletion_phase(sync_module, state_db, registry_db, hub, tmp_path) -> None:
    """
    An unreachable claude_chats root must stop the deletion phase, even when the NFS
    roots are fine. Otherwise the re-keyed claude-session:// row (not a path, so no
    NFS prefix guard applies) would look deleted and its points on the hub would be deleted.
    """
    root = tmp_path / "nas"
    _make_files(root, 2)
    # max_delete_fraction=1.0 so only the skipped-root guard can stop the deletion.
    config = _config(root, max_delete_fraction=1.0)
    _first_sync_completed(sync_module, state_db, registry_db, config, hub)

    # A completed Claude Code session row whose transcript lives under a missing root.
    session_key = "claude-session://47cfe354-39f7-45a0-993e-1df37c4fb88e"
    state_db.discover_or_update(session_key, OWNER, 0.0, origin_path="/gone/x.jsonl")
    state_db.mark_completed(session_key)
    config["claude_chats"] = {"roots": [{"path": str(tmp_path / "no-such-claude-root"), "owner": OWNER}]}
    before = _rows(state_db)

    assert sync_module.sync(config, state_db, registry_db, _clients(hub), _args()) == sync_module.EXIT_DELETION_REFUSED
    assert _rows(state_db) == before
    assert session_key in _rows(state_db)
    hub.delete.assert_not_called()


def test_scan_reports_missing_root_in_skipped_roots(state_db, tmp_path) -> None:
    missing = tmp_path / "never-existed"
    result = scan_nfs_roots(state_db, _config(missing), user_filter=OWNER)

    assert result["skipped_roots"] == [str(missing)]
    assert result["scanned"] == 0


def test_empty_root_with_no_rows_is_not_skipped(state_db, tmp_path) -> None:
    """An empty directory the queue knows nothing about is a legitimate empty source."""
    root = tmp_path / "empty"
    root.mkdir()

    result = scan_nfs_roots(state_db, _config(root), user_filter=OWNER)

    assert result["skipped_roots"] == []


# ----------------------------------------------------------------------------
# Item 2: mass-delete guard
# ----------------------------------------------------------------------------


def test_mass_delete_guard_blocks_deletion_until_allowed(sync_module, state_db, registry_db, hub, tmp_path, caplog) -> None:
    root = tmp_path / "nas"
    paths = _make_files(root, 10)
    config = _config(root, max_delete_fraction=0.10)
    _first_sync_completed(sync_module, state_db, registry_db, config, hub)

    # 2 of 10 completed files vanish: 20%, above the 10% limit.
    os.remove(paths[0])
    os.remove(paths[1])
    before = _rows(state_db)

    with caplog.at_level(logging.ERROR, logger="rag_sync_nfs"):
        assert sync_module.sync(config, state_db, registry_db, _clients(hub), _args(allow_mass_delete=False)) == sync_module.EXIT_DELETION_REFUSED

    # Refused: no tombstones, no deletes.
    assert _rows(state_db) == before
    assert _missing(state_db) == set()
    hub.delete.assert_not_called()
    assert "Deletion phase SKIPPED" in caplog.text

    # The operator re-runs with the flag: the two vanished files are tombstoned, not deleted.
    assert sync_module.sync(config, state_db, registry_db, _clients(hub), _args(allow_mass_delete=True)) == 0
    assert set(_rows(state_db)) == set(paths)
    assert _missing(state_db) == set(paths[:2])
    hub.delete.assert_not_called()


def test_deletion_at_exactly_the_limit_is_allowed(sync_module, state_db, registry_db, hub, tmp_path) -> None:
    """The guard is strictly greater-than: 1 of 10 at a 10% limit is tombstoned."""
    root = tmp_path / "nas"
    paths = _make_files(root, 10)
    config = _config(root, max_delete_fraction=0.10)
    _first_sync_completed(sync_module, state_db, registry_db, config, hub)

    os.remove(paths[0])

    assert sync_module.sync(config, state_db, registry_db, _clients(hub), _args()) == 0
    assert _missing(state_db) == {str(paths[0])}
    assert str(paths[0]) in _rows(state_db)
    hub.delete.assert_not_called()


def test_vanished_file_is_purged_only_after_the_grace_period(sync_module, state_db, registry_db, hub, tmp_path) -> None:
    """Tombstone on the first sync (points kept). Purge with a wait=True delete once 24 h have passed."""
    root = tmp_path / "nas"
    paths = _make_files(root, 4)
    config = _config(root, max_delete_fraction=1.0)
    _first_sync_completed(sync_module, state_db, registry_db, config, hub)

    os.remove(paths[0])
    assert sync_module.sync(config, state_db, registry_db, _clients(hub), _args()) == 0
    assert _missing(state_db) == {str(paths[0])}
    hub.delete.assert_not_called()

    # Still inside the grace period on an immediate re-sync: nothing is purged.
    assert sync_module.sync(config, state_db, registry_db, _clients(hub), _args()) == 0
    hub.delete.assert_not_called()
    assert str(paths[0]) in _rows(state_db)

    # 25 h later, the next sync purges the points and then the row.
    _age_tombstones(state_db, hours=25)
    assert sync_module.sync(config, state_db, registry_db, _clients(hub), _args()) == 0

    assert hub.delete.call_count == 1
    assert hub.delete.call_args.args == (str(paths[0]),)
    assert str(paths[0]) not in _rows(state_db)


def test_moved_file_keeps_its_points_and_is_rekeyed_on_disk(sync_module, state_db, registry_db, hub, tmp_path) -> None:
    """A real rename on disk: the old key's points are re-keyed, nothing is deleted, nothing re-queued."""
    root = tmp_path / "nas"
    paths = _make_files(root, 3)
    config = _config(root, max_delete_fraction=1.0)
    _first_sync_completed(sync_module, state_db, registry_db, config, hub)

    old = paths[0]
    new = str(root / "moved.md")
    os.rename(old, new)

    assert sync_module.sync(config, state_db, registry_db, _clients(hub), _args()) == 0

    rows = _rows(state_db)
    assert old not in rows
    assert rows[new] == "completed"
    hub.delete.assert_not_called()
    # The move is one change_key request: old key to new key. Its points are kept and nothing is re-embedded.
    hub.change_key.assert_called_once_with(old, new)


def test_deletion_log_states_reason(sync_module, state_db, registry_db, hub, tmp_path, caplog) -> None:
    root = tmp_path / "nas"
    paths = _make_files(root, 2)
    config = _config(root, max_delete_fraction=1.0)
    _first_sync_completed(sync_module, state_db, registry_db, config, hub)

    os.remove(paths[0])
    with caplog.at_level(logging.INFO, logger="rag_sync_nfs"):
        assert sync_module.sync(config, state_db, registry_db, _clients(hub), _args()) == 0

    assert "file missing on disk" in caplog.text


# ----------------------------------------------------------------------------
# Owner scope: one user's sync never deletes another owner's rows
# ----------------------------------------------------------------------------


def test_user_sync_leaves_other_owners_rows_alone(sync_module, state_db, registry_db, hub, tmp_path) -> None:
    root = tmp_path / "nas"
    paths = _make_files(root, 2)
    config = _config(root, max_delete_fraction=0.9)
    _first_sync_completed(sync_module, state_db, registry_db, config, hub)

    # A shared row owned by another user. Its file is not part of this user's scan.
    shared_path = str(tmp_path / "shared" / "laf.md")
    state_db.discover_or_update(shared_path, "la-familia", 0.0)
    state_db.mark_completed(shared_path)

    os.remove(paths[0])
    assert sync_module.sync(config, state_db, registry_db, _clients(hub), _args()) == 0

    # This user's vanished file is tombstoned. The other owner's row is neither tombstoned nor purged.
    rows = _rows(state_db)
    assert rows[shared_path] == "completed"
    assert _missing(state_db) == {str(paths[0])}
    assert str(paths[0]) in rows


def test_find_deleted_scopes_to_owners(state_db) -> None:
    florian_gone = "/nas/florian/gone.md"
    shared_gone = "/nas/shared/gone.md"
    state_db.discover_or_update(florian_gone, OWNER, 0.0)
    state_db.mark_completed(florian_gone)
    state_db.discover_or_update(shared_gone, "la-familia", 0.0)
    state_db.mark_completed(shared_gone)

    completed_gone, all_gone = state_db.find_deleted(set(), owners=[OWNER])
    assert completed_gone == [florian_gone]
    assert all_gone == [florian_gone]

    completed_gone, all_gone = state_db.find_deleted(set(), owners=None)
    assert sorted(completed_gone) == sorted([florian_gone, shared_gone])


def test_find_deleted_with_empty_owner_list_considers_nothing(state_db) -> None:
    state_db.discover_or_update("/nas/gone.md", OWNER, 0.0)
    state_db.mark_completed("/nas/gone.md")

    assert state_db.find_deleted(set(), owners=[]) == ([], [])


def test_count_with_prefix_counts_rows_under_the_prefix_only(state_db) -> None:
    state_db.discover_or_update("/nas/a/one.md", OWNER, 0.0)
    state_db.discover_or_update("/nas/a/two.md", OWNER, 0.0)
    state_db.discover_or_update("/nas/ab/three.md", OWNER, 0.0)

    assert state_db.count_with_prefix("/nas/a/") == 2
    assert state_db.count_with_prefix("/nas/x/") == 0


# ----------------------------------------------------------------------------
# Dry run: prints the plan, takes no lock, writes nothing to the live database
# ----------------------------------------------------------------------------


def test_dry_run_prints_the_plan_and_writes_nothing(sync_module, state_db, registry_db, hub, tmp_path, monkeypatch, capsys) -> None:
    root = tmp_path / "nas"
    paths = _make_files(root, 3)
    config = _config(root, max_delete_fraction=1.0)
    _first_sync_completed(sync_module, state_db, registry_db, config, hub)
    live_db = str(tmp_path / "state" / "rag_state.db")

    # The sync would now tombstone one file and queue a new one. The dry run must show
    # the tombstone and must not leave the new file queued in the live database.
    os.remove(paths[0])
    new_file = root / "brand-new.md"
    new_file.write_text("brand new", encoding="utf-8")
    before = _rows(state_db)
    before_missing = _missing(state_db)

    # A dry run takes no lock.
    monkeypatch.setattr(sync_module, "acquire_or_exit", lambda *a, **k: pytest.fail("dry run took the lock"))

    # No live registry exists yet, so the dry run must not create one.
    live_registry = str(tmp_path / "state" / "hub_registry.db")
    exit_code = sync_module.run_dry_run(config, live_db, live_registry, _clients(hub), _args(dry_run=True))

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Sync plan" in output
    assert f"{paths[0]}  (file missing on disk)" in output
    assert str(new_file) not in _rows(state_db)
    assert _rows(state_db) == before
    assert _missing(state_db) == before_missing
    assert _write_calls(hub) == []
    assert not os.path.exists(live_registry)


def test_dry_run_plans_on_the_live_registry_copy_and_leaves_it_unchanged(
    sync_module, state_db, registry_db, hub, tmp_path, monkeypatch, capsys,
) -> None:
    """
    With a live registry present, the dry run plans from a copy of it and writes nothing back.

    The live registry holds a project whose marker is gone. The plan must show it as removed,
    because the dry run read it, and the live registry must still hold it afterwards.
    """
    root = tmp_path / "nas"
    _make_files(root, 3)
    config = _config(root, max_delete_fraction=1.0)
    _first_sync_completed(sync_module, state_db, registry_db, config, hub)
    registry_db.upsert_project("pid-gone", str(tmp_path / "gone"), "Gone", OWNER, SOURCE_ID_NAS)
    live_registry = str(tmp_path / "registry" / "hub_registry.db")

    monkeypatch.setattr(sync_module, "acquire_or_exit", lambda *a, **k: pytest.fail("dry run took the lock"))

    exit_code = sync_module.run_dry_run(
        config, str(tmp_path / "state" / "rag_state.db"), live_registry, _clients(hub), _args(dry_run=True),
    )
    assert _write_calls(hub) == []

    assert exit_code == 0
    assert "project removals: 1" in capsys.readouterr().out
    assert "pid-gone" in [p["id"] for p in registry_db.list_projects()]
