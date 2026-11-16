"""
Tests for the deletion guards in utils/rag_sync_nfs.py (sync()) and StateDB.find_deleted().

Everything runs on a temporary SQLite file and a MagicMock Qdrant client. The
production queue at db_base_path is never opened, and no Qdrant server is contacted.
The config has no opencode or claude_chats section, so those scans return at once
without network access.
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ragnart.scanner import scan_nfs_roots
from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent
OWNER = "tester"


@pytest.fixture
def sync_module():
    """Load utils/rag_sync_nfs.py as a module. It is a script, not a package."""
    spec = importlib.util.spec_from_file_location("rag_sync_nfs", REPO_ROOT / "utils" / "rag_sync_nfs.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def qdrant(sync_module, monkeypatch) -> MagicMock:
    """Replace get_client() in the sync script with a mock, and return that mock."""
    client = MagicMock(name="qdrant")
    monkeypatch.setattr(sync_module, "get_client", lambda url, timeout=60: client)
    return client


@pytest.fixture
def state_db(tmp_path):
    db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    yield db
    db.close()


def _config(root, max_delete_fraction: float = 0.10) -> dict:
    """A ragnart.yaml-shaped dict with one user root and the sync guard key set."""
    return {
        "rag": {
            "qdrant_url": "http://qdrant.invalid:6333",
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


def _first_sync_completed(sync_module, state_db, config, qdrant) -> None:
    """Run a clean sync over the tree, then mark everything it queued as completed."""
    assert sync_module.sync(config, state_db, _args()) == 0
    _complete_all(state_db)
    qdrant.delete.reset_mock()
    qdrant.set_payload.reset_mock()


# ----------------------------------------------------------------------------
# Item 1: skipped roots
# ----------------------------------------------------------------------------


def test_root_renamed_away_is_skipped_and_nothing_is_deleted(sync_module, state_db, qdrant, tmp_path, caplog) -> None:
    root = tmp_path / "nas"
    _make_files(root, 3)
    config = _config(root)
    _first_sync_completed(sync_module, state_db, config, qdrant)
    before = _rows(state_db)
    assert len(before) == 3

    # The mount disappears: the configured root is no longer a directory.
    os.rename(root, tmp_path / "nas-moved")

    with caplog.at_level(logging.ERROR, logger="rag_sync_nfs"):
        exit_code = sync_module.sync(config, state_db, _args())

    assert exit_code == sync_module.EXIT_DELETION_REFUSED
    assert _rows(state_db) == before
    qdrant.delete.assert_not_called()
    assert "Deletion phase SKIPPED" in caplog.text
    assert str(root) in caplog.text


def test_empty_root_with_rows_beneath_it_is_skipped(sync_module, state_db, qdrant, tmp_path) -> None:
    """A directory that yields zero files, while the queue has rows under it, counts as unmounted."""
    root = tmp_path / "nas"
    paths = _make_files(root, 2)
    config = _config(root)
    _first_sync_completed(sync_module, state_db, config, qdrant)
    before = _rows(state_db)

    for path in paths:
        os.remove(path)

    assert sync_module.sync(config, state_db, _args()) == sync_module.EXIT_DELETION_REFUSED
    assert _rows(state_db) == before
    qdrant.delete.assert_not_called()


def test_claude_root_unreachable_skips_whole_deletion_phase(sync_module, state_db, qdrant, tmp_path) -> None:
    """
    An unreachable claude_chats root must stop the deletion phase, even when the NFS
    roots are fine. Otherwise the re-keyed claude-session:// row (not a path, so no
    NFS prefix guard applies) would look deleted and its Qdrant points would be purged.
    """
    root = tmp_path / "nas"
    _make_files(root, 2)
    # max_delete_fraction=1.0 so only the skipped-root guard can stop the deletion.
    config = _config(root, max_delete_fraction=1.0)
    _first_sync_completed(sync_module, state_db, config, qdrant)

    # A completed Claude Code session row whose transcript lives under a missing root.
    session_key = "claude-session://47cfe354-39f7-45a0-993e-1df37c4fb88e"
    state_db.discover_or_update(session_key, OWNER, 0.0, origin_path="/gone/x.jsonl")
    state_db.mark_completed(session_key)
    config["claude_chats"] = {"roots": [{"path": str(tmp_path / "no-such-claude-root"), "owner": OWNER}]}
    before = _rows(state_db)

    assert sync_module.sync(config, state_db, _args()) == sync_module.EXIT_DELETION_REFUSED
    assert _rows(state_db) == before
    assert session_key in _rows(state_db)
    qdrant.delete.assert_not_called()


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


def test_mass_delete_guard_blocks_deletion_until_allowed(sync_module, state_db, qdrant, tmp_path, caplog) -> None:
    root = tmp_path / "nas"
    paths = _make_files(root, 10)
    config = _config(root, max_delete_fraction=0.10)
    _first_sync_completed(sync_module, state_db, config, qdrant)

    # 2 of 10 completed files vanish: 20%, above the 10% limit.
    os.remove(paths[0])
    os.remove(paths[1])
    before = _rows(state_db)

    with caplog.at_level(logging.ERROR, logger="rag_sync_nfs"):
        assert sync_module.sync(config, state_db, _args(allow_mass_delete=False)) == sync_module.EXIT_DELETION_REFUSED

    # Refused: no tombstones, no deletes.
    assert _rows(state_db) == before
    assert _missing(state_db) == set()
    qdrant.delete.assert_not_called()
    assert "Deletion phase SKIPPED" in caplog.text

    # The operator re-runs with the flag: the two vanished files are tombstoned, not deleted.
    assert sync_module.sync(config, state_db, _args(allow_mass_delete=True)) == 0
    assert set(_rows(state_db)) == set(paths)
    assert _missing(state_db) == set(paths[:2])
    qdrant.delete.assert_not_called()


def test_deletion_at_exactly_the_limit_is_allowed(sync_module, state_db, qdrant, tmp_path) -> None:
    """The guard is strictly greater-than: 1 of 10 at a 10% limit is tombstoned."""
    root = tmp_path / "nas"
    paths = _make_files(root, 10)
    config = _config(root, max_delete_fraction=0.10)
    _first_sync_completed(sync_module, state_db, config, qdrant)

    os.remove(paths[0])

    assert sync_module.sync(config, state_db, _args()) == 0
    assert _missing(state_db) == {str(paths[0])}
    assert str(paths[0]) in _rows(state_db)
    qdrant.delete.assert_not_called()


def test_vanished_file_is_purged_only_after_the_grace_period(sync_module, state_db, qdrant, tmp_path) -> None:
    """Tombstone on the first sync (points kept). Purge with a wait=True delete once 24 h have passed."""
    root = tmp_path / "nas"
    paths = _make_files(root, 4)
    config = _config(root, max_delete_fraction=1.0)
    _first_sync_completed(sync_module, state_db, config, qdrant)

    os.remove(paths[0])
    assert sync_module.sync(config, state_db, _args()) == 0
    assert _missing(state_db) == {str(paths[0])}
    qdrant.delete.assert_not_called()

    # Still inside the grace period on an immediate re-sync: nothing is purged.
    assert sync_module.sync(config, state_db, _args()) == 0
    qdrant.delete.assert_not_called()
    assert str(paths[0]) in _rows(state_db)

    # 25 h later, the next sync purges the points and then the row.
    _age_tombstones(state_db, hours=25)
    assert sync_module.sync(config, state_db, _args()) == 0

    assert qdrant.delete.call_count == 1
    assert qdrant.delete.call_args.kwargs["wait"] is True
    assert qdrant.delete.call_args.kwargs["points_selector"].filter.must[0].match.value == str(paths[0])
    assert str(paths[0]) not in _rows(state_db)


def test_moved_file_keeps_its_points_and_is_rekeyed_on_disk(sync_module, state_db, qdrant, tmp_path) -> None:
    """A real rename on disk: the old key's points are re-keyed, nothing is deleted, nothing re-queued."""
    root = tmp_path / "nas"
    paths = _make_files(root, 3)
    config = _config(root, max_delete_fraction=1.0)
    _first_sync_completed(sync_module, state_db, config, qdrant)

    old = paths[0]
    new = str(root / "moved.md")
    os.rename(old, new)

    assert sync_module.sync(config, state_db, _args()) == 0

    rows = _rows(state_db)
    assert old not in rows
    assert rows[new] == "completed"
    qdrant.delete.assert_not_called()
    assert [call.kwargs["payload"] for call in qdrant.set_payload.call_args_list] == [{"source_path": new}]
    filter_value = qdrant.set_payload.call_args.kwargs["points"].must[0].match.value
    assert filter_value == old


def test_deletion_log_states_reason(sync_module, state_db, qdrant, tmp_path, caplog) -> None:
    root = tmp_path / "nas"
    paths = _make_files(root, 2)
    config = _config(root, max_delete_fraction=1.0)
    _first_sync_completed(sync_module, state_db, config, qdrant)

    os.remove(paths[0])
    with caplog.at_level(logging.INFO, logger="rag_sync_nfs"):
        assert sync_module.sync(config, state_db, _args()) == 0

    assert "file missing on disk" in caplog.text


# ----------------------------------------------------------------------------
# Owner scope: one user's sync never deletes another owner's rows
# ----------------------------------------------------------------------------


def test_user_sync_leaves_other_owners_rows_alone(sync_module, state_db, qdrant, tmp_path) -> None:
    root = tmp_path / "nas"
    paths = _make_files(root, 2)
    config = _config(root, max_delete_fraction=0.9)
    _first_sync_completed(sync_module, state_db, config, qdrant)

    # A shared row owned by another user. Its file is not part of this user's scan.
    shared_path = str(tmp_path / "shared" / "laf.md")
    state_db.discover_or_update(shared_path, "la-familia", 0.0)
    state_db.mark_completed(shared_path)

    os.remove(paths[0])
    assert sync_module.sync(config, state_db, _args()) == 0

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


def test_dry_run_prints_the_plan_and_writes_nothing(sync_module, state_db, qdrant, tmp_path, monkeypatch, capsys) -> None:
    root = tmp_path / "nas"
    paths = _make_files(root, 3)
    config = _config(root, max_delete_fraction=1.0)
    _first_sync_completed(sync_module, state_db, config, qdrant)
    live_db = str(tmp_path / "state" / "rag_state.db")

    # The sync would now tombstone one file and queue a new one. The dry run must show
    # the tombstone and must not leave the new file queued in the live database.
    os.remove(paths[0])
    new_file = root / "brand-new.md"
    new_file.write_text("brand new", encoding="utf-8")
    before = _rows(state_db)
    before_missing = _missing(state_db)

    # A dry run neither takes the lock nor builds a Qdrant client.
    monkeypatch.setattr(sync_module, "acquire_or_exit", lambda *a, **k: pytest.fail("dry run took the lock"))
    monkeypatch.setattr(sync_module, "get_client", lambda *a, **k: pytest.fail("dry run built a Qdrant client"))

    exit_code = sync_module.run_dry_run(config, live_db, _args(dry_run=True))

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Sync plan" in output
    assert f"{paths[0]}  (file missing on disk)" in output
    assert str(new_file) not in _rows(state_db)
    assert _rows(state_db) == before
    assert _missing(state_db) == before_missing
    qdrant.set_payload.assert_not_called()
    qdrant.delete.assert_not_called()
