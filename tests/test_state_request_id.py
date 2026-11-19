"""
Tests for session B2 step A.7 in ragnart/state.py: migration 5 (the request_id column), the optional
request_id on mark_processing(), and the helpers the submit and poll steps use.

Covers:
  - a fresh database reaches user_version 5 with the request_id column;
  - a database at user_version 4 (migration 4 reserved and empty) upgrades to 5 and keeps its rows;
  - mark_processing() stores the request id, and clears it when none is given (the default);
  - processing_rows_with_request_id() returns only processing rows that name a request;
  - pending_rows() returns the pending rows in insertion order with the fields a submission needs;
  - set_launch_dir() stores a session's launch directory before its completion.

Everything runs on a tmp database. No live database is touched.
"""

from __future__ import annotations

import sqlite3

import pytest

from ragnart.state import StateDB


def _user_version(db_path: str) -> int:
    """Return PRAGMA user_version through a separate connection."""
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()


def _columns(db_path: str) -> set[str]:
    """Return the column names of ingestion_queue through a separate connection."""
    conn = sqlite3.connect(db_path)
    try:
        return {row[1] for row in conn.execute("PRAGMA table_info(ingestion_queue)")}
    finally:
        conn.close()


def _raw_request_id(db_path: str, file_path: str):
    """Return the request_id column of one row through a separate connection."""
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT request_id FROM ingestion_queue WHERE file_path = ?", (file_path,)
        ).fetchone()
        return row[0]
    finally:
        conn.close()


@pytest.fixture
def db_path(tmp_path) -> str:
    return str(tmp_path / "db" / "sqlite" / "rag_state.db")


@pytest.fixture
def db(db_path) -> StateDB:
    state = StateDB(db_path)
    yield state
    state.close()


def test_fresh_database_reaches_version_5_with_request_id(db, db_path):
    # A brand-new database runs every migration, so migration 5 adds the column.
    assert _user_version(db_path) == 5
    assert "request_id" in _columns(db_path)


def test_upgrade_from_version_4_keeps_rows_and_adds_request_id(db_path, tmp_path):
    # Build a database as it was at user_version 4: the request_id column is removed again,
    # and the version is set back to 4. Its rows must survive the upgrade to 5.
    first = StateDB(db_path)
    first.discover_or_update("/mnt/nfs/Florian/a.md", "florian", 1000.0, source_id="nas")
    first.mark_completed("/mnt/nfs/Florian/a.md", chunk_count=3)
    first.close()
    raw = sqlite3.connect(db_path)
    raw.execute("ALTER TABLE ingestion_queue DROP COLUMN request_id")
    raw.execute("PRAGMA user_version = 4")
    raw.commit()
    raw.close()
    assert "request_id" not in _columns(db_path)

    upgraded = StateDB(db_path)
    try:
        assert _user_version(db_path) == 5
        assert "request_id" in _columns(db_path)
        counts = upgraded.get_counts()
        assert counts["completed"] == 1
        assert counts["total"] == 1
    finally:
        upgraded.close()
    assert _raw_request_id(db_path, "/mnt/nfs/Florian/a.md") is None


def test_mark_processing_stores_the_request_id(db, db_path):
    db.discover_or_update("/mnt/nfs/Florian/a.md", "florian", 1000.0, source_id="nas")

    db.mark_processing("/mnt/nfs/Florian/a.md", request_id="req-1")

    assert _raw_request_id(db_path, "/mnt/nfs/Florian/a.md") == "req-1"
    assert db.get_counts()["processing"] == 1


def test_mark_processing_without_a_request_id_clears_it(db, db_path):
    # The default is None, and None is stored as given: a processing row with no request has none outstanding.
    db.discover_or_update("/mnt/nfs/Florian/a.md", "florian", 1000.0, source_id="nas")
    db.mark_processing("/mnt/nfs/Florian/a.md", request_id="req-1")

    db.mark_processing("/mnt/nfs/Florian/a.md")

    assert _raw_request_id(db_path, "/mnt/nfs/Florian/a.md") is None


def test_processing_rows_with_request_id_lists_only_those_rows(db):
    # Three rows: one processing with a request, one processing with none, one pending.
    for name in ("a", "b", "c"):
        db.discover_or_update(f"/mnt/nfs/Florian/{name}.md", "florian", 1000.0, source_id="nas")
    db.mark_processing("/mnt/nfs/Florian/a.md", request_id="req-a")
    db.mark_processing("/mnt/nfs/Florian/b.md")

    rows = db.processing_rows_with_request_id()

    assert rows == [{"file_path": "/mnt/nfs/Florian/a.md", "request_id": "req-a", "source_id": "nas"}]


def test_processing_rows_with_request_id_skips_a_completed_row(db):
    # Completing a row ends its wait: it is no longer processing, so the poll does not ask about it.
    db.discover_or_update("/mnt/nfs/Florian/a.md", "florian", 1000.0, source_id="nas")
    db.mark_processing("/mnt/nfs/Florian/a.md", request_id="req-a")
    db.mark_completed("/mnt/nfs/Florian/a.md", chunk_count=2)

    assert db.processing_rows_with_request_id() == []


def test_pending_rows_returns_the_submission_fields_oldest_first(db):
    db.discover_or_update(
        "/mnt/nfs/Florian/b.pdf", "florian", 2000.0,
        origin_path="/mnt/nfs/Florian/b.pdf", file_type="pdf", source_id="nas",
    )
    db.discover_or_update(
        "claude-session://abc", "florian", 3000.0,
        origin_path="/home/x/.claude/projects/p/abc.jsonl", source_type="claude_session",
        source_id="claude-linux-shared-nfs",
    )
    # A completed row is not pending, so it is not returned.
    db.discover_or_update("/mnt/nfs/Florian/c.md", "florian", 1000.0, source_id="nas")
    db.mark_completed("/mnt/nfs/Florian/c.md", chunk_count=1)

    rows = db.pending_rows()

    assert [row["file_path"] for row in rows] == ["/mnt/nfs/Florian/b.pdf", "claude-session://abc"]
    assert rows[0] == {
        "file_path": "/mnt/nfs/Florian/b.pdf",
        "owner": "florian",
        "origin_path": "/mnt/nfs/Florian/b.pdf",
        "last_modified": 2000.0,
        "file_type": "pdf",
        "source_id": "nas",
    }
    assert rows[1]["origin_path"] == "/home/x/.claude/projects/p/abc.jsonl"


def test_set_launch_dir_stores_the_directory_and_none_clears_it(db, db_path):
    db.discover_or_update("claude-session://abc", "florian", 3000.0, source_id="claude-linux-shared-nfs")

    db.set_launch_dir("claude-session://abc", "/mnt/nfs/Florian/proj")
    conn = sqlite3.connect(db_path)
    stored = conn.execute(
        "SELECT launch_dir FROM ingestion_queue WHERE file_path = 'claude-session://abc'"
    ).fetchone()[0]
    conn.close()
    assert stored == "/mnt/nfs/Florian/proj"

    db.set_launch_dir("claude-session://abc", None)
    conn = sqlite3.connect(db_path)
    stored = conn.execute(
        "SELECT launch_dir FROM ingestion_queue WHERE file_path = 'claude-session://abc'"
    ).fetchone()[0]
    conn.close()
    assert stored is None
