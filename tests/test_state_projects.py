"""
Tests for the sync-support methods added to StateDB in v0.2 session C.

Every test runs on a real StateDB in tmp_path. No mocks, no Qdrant, no NFS.
Rows are seeded through the public discover_or_update / mark_completed calls
so the tests exercise the same paths the scanner and worker use.
"""

from __future__ import annotations

from ragnart.state import StateDB

OWNER = "tester"
FILE_A = "/mnt/nfs/x/a.md"
FILE_B = "/mnt/nfs/x/b.md"
HASH_A = "a" * 32
HASH_B = "b" * 32


def _new_db(tmp_path) -> StateDB:
    return StateDB(str(tmp_path / "rag_state.db"))


def _row(state_db: StateDB, file_path: str) -> dict:
    """Return the raw row as a dict, including columns the public API does not expose."""
    row = state_db._conn.execute(
        "SELECT * FROM ingestion_queue WHERE file_path = ?", (file_path,),
    ).fetchone()
    return dict(row) if row is not None else None


def _completed(state_db: StateDB, file_path: str, chunk_count: int = 3) -> None:
    """Queue a file and mark it completed, as a finished ingestion would leave it."""
    state_db.discover_or_update(file_path, OWNER, 1000.0, origin_path=file_path)
    state_db.mark_completed(file_path, chunk_count=chunk_count, completed_at="2026-01-01T00:00:00+00:00")


# ----------------------------------------------------------------------
# row_exists

def test_row_exists_false_then_true(tmp_path) -> None:
    state_db = _new_db(tmp_path)
    try:
        assert state_db.row_exists(FILE_A) is False
        state_db.discover_or_update(FILE_A, OWNER, 1000.0)
        assert state_db.row_exists(FILE_A) is True
    finally:
        state_db.close()


# ----------------------------------------------------------------------
# discover_or_update hash kwargs: insert-only

def test_hash_fields_written_on_insert(tmp_path) -> None:
    state_db = _new_db(tmp_path)
    try:
        state_db.discover_or_update(
            FILE_A, OWNER, 1000.0,
            size_bytes=42, content_hash=HASH_A, hashed_at="2026-01-01T00:00:00+00:00",
        )
        row = _row(state_db, FILE_A)
        assert row["size_bytes"] == 42
        assert row["content_hash"] == HASH_A
        assert row["hashed_at"] == "2026-01-01T00:00:00+00:00"
    finally:
        state_db.close()


def test_hash_fields_unchanged_by_later_no_op_call(tmp_path) -> None:
    state_db = _new_db(tmp_path)
    try:
        state_db.discover_or_update(
            FILE_A, OWNER, 1000.0,
            size_bytes=42, content_hash=HASH_A, hashed_at="2026-01-01T00:00:00+00:00",
        )
        # Same mtime, so this is a no-op branch. The different hash values must not be stored.
        state_db.discover_or_update(
            FILE_A, OWNER, 1000.0,
            size_bytes=999, content_hash=HASH_B, hashed_at="2030-01-01T00:00:00+00:00",
        )
        row = _row(state_db, FILE_A)
        assert row["size_bytes"] == 42
        assert row["content_hash"] == HASH_A
        assert row["hashed_at"] == "2026-01-01T00:00:00+00:00"
    finally:
        state_db.close()


# ----------------------------------------------------------------------
# attachment

def test_set_project_id_and_rows_for_attachment(tmp_path) -> None:
    state_db = _new_db(tmp_path)
    try:
        _completed(state_db, FILE_A)
        # A pending row must not show up for attachment: it has no Qdrant points yet.
        state_db.discover_or_update(FILE_B, OWNER, 1000.0)

        state_db.set_project_id(FILE_A, "p1")

        rows = state_db.rows_for_attachment()
        assert [r["file_path"] for r in rows] == [FILE_A]
        assert rows[0]["project_id"] == "p1"
        assert set(rows[0]) == {"file_path", "project_id", "source_type", "launch_dir"}
    finally:
        state_db.close()


def test_rows_for_attachment_returns_launch_dir_for_sessions(tmp_path) -> None:
    state_db = _new_db(tmp_path)
    try:
        key = "claude-session://abc"
        state_db.discover_or_update(key, OWNER, 1000.0, origin_path="/x/abc.jsonl", source_type="claude_session")
        state_db.mark_completed(key, chunk_count=2, launch_dir="/mnt/nfs/x/proj")

        rows = state_db.rows_for_attachment()
        assert rows == [{
            "file_path": key,
            "project_id": "global",
            "source_type": "claude_session",
            "launch_dir": "/mnt/nfs/x/proj",
        }]
    finally:
        state_db.close()


# ----------------------------------------------------------------------
# tombstones

def test_mark_then_clear_missing(tmp_path) -> None:
    state_db = _new_db(tmp_path)
    try:
        _completed(state_db, FILE_A)
        state_db.mark_missing(FILE_A, "2026-01-01T00:00:00+00:00")
        assert _row(state_db, FILE_A)["missing_since"] == "2026-01-01T00:00:00+00:00"

        state_db.clear_missing(FILE_A)
        assert _row(state_db, FILE_A)["missing_since"] is None
    finally:
        state_db.close()


def test_second_mark_missing_keeps_the_first_timestamp(tmp_path) -> None:
    """A3: the grace is measured from the first time the file was seen missing."""
    state_db = _new_db(tmp_path)
    try:
        _completed(state_db, FILE_A)
        state_db.mark_missing(FILE_A, "2026-01-01T00:00:00+00:00")
        # A later sync sees it missing again. The stored first timestamp must survive.
        state_db.mark_missing(FILE_A, "2026-01-02T12:00:00+00:00")
        assert _row(state_db, FILE_A)["missing_since"] == "2026-01-01T00:00:00+00:00"
    finally:
        state_db.close()


def test_rows_missing_since_before_uses_iso_ordering(tmp_path) -> None:
    state_db = _new_db(tmp_path)
    try:
        _completed(state_db, FILE_A)
        _completed(state_db, FILE_B)
        state_db.mark_missing(FILE_A, "2026-01-01T00:00:00+00:00")
        state_db.mark_missing(FILE_B, "2026-01-03T00:00:00+00:00")

        # Only A is older than the cutoff. B is newer, and a present row never qualifies.
        assert state_db.rows_missing_since_before("2026-01-02T00:00:00+00:00") == [FILE_A]
        assert state_db.rows_missing_since_before("2025-01-01T00:00:00+00:00") == []
    finally:
        state_db.close()


# ----------------------------------------------------------------------
# moves

def test_rename_key_moves_the_row_and_keeps_chunk_count(tmp_path) -> None:
    state_db = _new_db(tmp_path)
    old_path = "/mnt/nfs/x/old.md"
    new_path = "/mnt/nfs/x/new.md"
    try:
        _completed(state_db, old_path, chunk_count=7)

        state_db.rename_key(old_path, new_path, 2000.0, origin_path=new_path)

        assert _row(state_db, old_path) is None
        moved = _row(state_db, new_path)
        assert moved["chunk_count"] == 7
        assert moved["status"] == "completed"
        assert moved["origin_path"] == new_path
        assert moved["last_modified"] == 2000.0
    finally:
        state_db.close()


def test_completed_rows_with_content_hash_excludes_null_and_blank(tmp_path) -> None:
    state_db = _new_db(tmp_path)
    try:
        # A: a real hash. B: NULL hash. C: blank hash. All three completed.
        state_db.discover_or_update(FILE_A, OWNER, 1000.0, size_bytes=1, content_hash=HASH_A)
        state_db.mark_completed(FILE_A, chunk_count=1)
        state_db.discover_or_update(FILE_B, OWNER, 1000.0)
        state_db.mark_completed(FILE_B, chunk_count=1)
        blank = "/mnt/nfs/x/c.md"
        state_db.discover_or_update(blank, OWNER, 1000.0, content_hash="")
        state_db.mark_completed(blank, chunk_count=1)

        assert state_db.completed_rows_with_content_hash() == {FILE_A: HASH_A}
    finally:
        state_db.close()


def test_completed_rows_with_content_hash_ignores_pending_rows(tmp_path) -> None:
    state_db = _new_db(tmp_path)
    try:
        # Pending with a hash: not completed, so not returned.
        state_db.discover_or_update(FILE_A, OWNER, 1000.0, size_bytes=1, content_hash=HASH_A)
        assert state_db.completed_rows_with_content_hash() == {}
    finally:
        state_db.close()


# ----------------------------------------------------------------------
# item_counts_by_project (read by GET /v1/projects)

def test_item_counts_by_project_counts_only_completed_rows(tmp_path) -> None:
    state_db = _new_db(tmp_path)
    try:
        completed_a = "/mnt/nfs/x/proj/a.md"
        completed_b = "/mnt/nfs/x/proj/b.md"
        pending_c = "/mnt/nfs/x/proj/c.md"
        for file_path in (completed_a, completed_b, pending_c):
            state_db.discover_or_update(file_path, OWNER, 1000.0, origin_path=file_path)
        state_db.mark_completed(completed_a, chunk_count=2, completed_at="2026-01-01T00:00:00+00:00")
        state_db.mark_completed(completed_b, chunk_count=2, completed_at="2026-01-01T00:00:00+00:00")
        state_db.set_project_id(completed_a, "p-root")
        state_db.set_project_id(completed_b, "p-root")
        state_db.set_project_id(pending_c, "p-root")

        # pending_c is not completed, so it is not counted.
        assert state_db.item_counts_by_project() == {"p-root": 2}
    finally:
        state_db.close()


def test_item_counts_by_project_empty_when_nothing_completed(tmp_path) -> None:
    state_db = _new_db(tmp_path)
    try:
        assert state_db.item_counts_by_project() == {}
    finally:
        state_db.close()
