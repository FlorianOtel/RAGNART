"""
Tests for StateDB.insert_completed_rows (session B2, the rebuild's all-or-nothing row write).

The rows are written in one transaction: every row is committed, or a failure rolls back the whole batch.
The state is a real StateDB in a tmp file, and a separate sqlite3 connection reads what was committed.
"""

from __future__ import annotations

import sqlite3

import pytest

from ragnart.state import StateDB


@pytest.fixture
def state(tmp_path) -> StateDB:
    db = StateDB(str(tmp_path / "rag_state.db"))
    yield db
    db.close()


def _row(file_path: str, **extra) -> dict:
    """One row as the rebuild builds it. A None value is left out, so the column keeps its default."""
    row = {
        "file_path": file_path,
        "owner": "florian",
        "last_modified": 1_700_000_000.0,
        "origin_path": file_path,
        "source_type": "file",
        "file_type": "md",
        "source_id": "nas",
        "size_bytes": None,
        "content_hash": "abc",
        "hashed_at": None,
        "chunk_count": 2,
        "pipeline_version": "p1",
        "project_id": None,
        "launch_dir": None,
    }
    row.update(extra)
    return row


def _committed_rows(db_path: str) -> dict[str, dict]:
    """The committed rows, read through a separate connection so only committed data is seen."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return {row["file_path"]: dict(row) for row in conn.execute("SELECT * FROM ingestion_queue")}
    finally:
        conn.close()


def test_every_row_is_committed_as_completed_and_a_none_value_keeps_the_column_default(state):
    """All rows are written as completed and visible to another connection. project_id None takes its default 'global'."""
    state.insert_completed_rows([_row("/mnt/nfs/a.md"), _row("/mnt/nfs/b.md", size_bytes=5)])

    rows = _committed_rows(state._db_path)
    assert set(rows) == {"/mnt/nfs/a.md", "/mnt/nfs/b.md"}
    for row in rows.values():
        assert row["status"] == "completed"
        assert row["completed_at"] is not None
        assert row["first_seen_at"] == row["completed_at"]
        assert row["project_id"] == "global"
    assert rows["/mnt/nfs/b.md"]["size_bytes"] == 5


def test_a_failure_midway_writes_no_row_and_the_connection_stays_usable(state):
    """
    The second row repeats the first file_path, so its insert fails after the first row was written. Neither row
    is committed, and the database accepts a new batch afterwards.
    """
    with pytest.raises(sqlite3.IntegrityError):
        state.insert_completed_rows([_row("/mnt/nfs/a.md"), _row("/mnt/nfs/a.md")])

    assert _committed_rows(state._db_path) == {}
    assert state.get_counts()["total"] == 0

    state.insert_completed_rows([_row("/mnt/nfs/c.md")])
    assert set(_committed_rows(state._db_path)) == {"/mnt/nfs/c.md"}


def test_an_empty_batch_writes_nothing_and_raises_nothing(state):
    """A rebuild in which nothing was restored still completes: the empty batch commits no rows."""
    state.insert_completed_rows([])

    assert state.get_counts()["total"] == 0
