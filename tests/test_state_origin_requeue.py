"""
Tests for StateDB.discover_or_update() when the origin_path of a known key changes.

A claude-session key can have several copies on disk. When a different copy becomes the
winner of a scan, the row must follow it: origin_path is updated and the row re-queued.
Everything runs on a real StateDB in tmp_path. No Qdrant or OpenCode is contacted.
"""

from __future__ import annotations

from ragnart.state import StateDB

OWNER = "tester"
KEY = "claude-session://47cfe354-0000-4000-8000-000000000002"
OLD_ORIGIN = "/home/a/.claude/projects/-mnt-a/old.jsonl"
NEW_ORIGIN = "/home/b/.claude/projects/-mnt-b/new.jsonl"


def _completed_row(state_db: StateDB, origin: str | None, mtime: float) -> None:
    """Queue one key and mark it completed, as a finished ingestion would leave it."""
    state_db.discover_or_update(KEY, OWNER, mtime, origin_path=origin, source_type="claude_session")
    state_db.mark_completed(KEY, chunk_count=4, completed_at="2026-01-01T00:00:00+00:00")


def _row(state_db: StateDB) -> dict:
    return dict(state_db._conn.execute(
        "SELECT * FROM ingestion_queue WHERE file_path = ?", (KEY,),
    ).fetchone())


def test_origin_change_requeues_a_completed_row(tmp_path) -> None:
    state_db = StateDB(str(tmp_path / "rag_state.db"))
    try:
        _completed_row(state_db, OLD_ORIGIN, 1000.0)

        state_db.discover_or_update(KEY, OWNER, 1000.0, origin_path=NEW_ORIGIN, source_type="claude_session")

        row = _row(state_db)
        assert row["status"] == "pending"
        assert row["origin_path"] == NEW_ORIGIN
        # The completion fields of the old copy are cleared for the re-ingest.
        assert row["completed_at"] is None
        assert row["started_at"] is None
        assert row["error_msg"] is None
        assert row["retry_count"] == 0
    finally:
        state_db.close()


def test_origin_change_stores_the_winners_mtime(tmp_path) -> None:
    """The winner can be older than the stored copy. Its own mtime is stored, not the old one."""
    state_db = StateDB(str(tmp_path / "rag_state.db"))
    try:
        _completed_row(state_db, OLD_ORIGIN, 5000.0)

        state_db.discover_or_update(KEY, OWNER, 1200.0, origin_path=NEW_ORIGIN)

        assert _row(state_db)["last_modified"] == 1200.0
    finally:
        state_db.close()


def test_same_origin_leaves_a_completed_row_alone(tmp_path) -> None:
    state_db = StateDB(str(tmp_path / "rag_state.db"))
    try:
        _completed_row(state_db, OLD_ORIGIN, 1000.0)

        state_db.discover_or_update(KEY, OWNER, 1000.0, origin_path=OLD_ORIGIN)

        row = _row(state_db)
        assert row["status"] == "completed"
        assert row["origin_path"] == OLD_ORIGIN
    finally:
        state_db.close()


def test_none_origin_never_changes_a_row(tmp_path) -> None:
    """A caller that passes no origin_path (for example OpenCode) is not a change."""
    state_db = StateDB(str(tmp_path / "rag_state.db"))
    try:
        _completed_row(state_db, OLD_ORIGIN, 1000.0)

        state_db.discover_or_update(KEY, OWNER, 1000.0, origin_path=None)

        row = _row(state_db)
        assert row["status"] == "completed"
        assert row["origin_path"] == OLD_ORIGIN
    finally:
        state_db.close()


def test_stored_null_origin_is_not_a_change(tmp_path) -> None:
    """A row with no stored origin (for example a pre-backfill NAS row) is not re-queued when one is given."""
    state_db = StateDB(str(tmp_path / "rag_state.db"))
    try:
        _completed_row(state_db, None, 1000.0)

        state_db.discover_or_update(KEY, OWNER, 1000.0, origin_path=NEW_ORIGIN)

        row = _row(state_db)
        assert row["status"] == "completed"
        assert row["origin_path"] is None
    finally:
        state_db.close()
