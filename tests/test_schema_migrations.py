"""
Tests for the ingestion-queue schema migrations in ragnart/state.py.

Covers:
  - a fresh database reaching the newest schema version;
  - the new project tables and the reserved 'global' row;
  - the new per-item columns;
  - re-opening a migrated database;
  - discover_or_update() and mark_completed() writing the new fields;
  - a database shaped like the live one (user_version 0, old schema) migrating in place.
"""

import os
import sqlite3

import pytest

from ragnart.state import StateDB

# The schema version that the newest migration produces.
LATEST_VERSION = 3

# Columns added by migration version 2 (ragnart/state.py _MIGRATIONS).
NEW_COLUMNS = {
    "project_id",
    "source_type",
    "file_type",
    "origin_path",
    "size_bytes",
    "content_hash",
    "hashed_at",
    "first_seen_at",
    "pipeline_version",
    "chunk_count",
    "missing_since",
    "launch_dir",
}

# Tables added by migration version 2.
NEW_TABLES = {"projects", "project_tags", "project_links", "source_links", "grants"}

# Column added by migration version 3 (ragnart/state.py _MIGRATIONS).
V3_COLUMNS = {"source_id"}

# The ingestion_queue schema exactly as ragnart/state.py had it before migrations
# existed, as the live rag_state.db was created. The live DB has user_version 0 and
# already has skip_reason, which migration 1 adds, so this copy includes it.
OLD_LIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS ingestion_queue (
    file_path       TEXT PRIMARY KEY,
    owner           TEXT NOT NULL,
    last_modified   REAL NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    error_msg       TEXT,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 5,
    started_at      TEXT,
    completed_at    TEXT,
    progress_detail TEXT,
    skip_reason     TEXT
);
"""


def _read_row(db_path: str, file_path: str) -> dict:
    """Read one ingestion_queue row through a separate connection."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM ingestion_queue WHERE file_path = ?", (file_path,)
        ).fetchone()
        return dict(row)
    finally:
        conn.close()


def _user_version(db_path: str) -> int:
    """Return PRAGMA user_version through a separate connection."""
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("PRAGMA user_version").fetchone()[0]
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


def test_fresh_db_ends_at_the_latest_schema_version(db, db_path):
    # A brand-new database runs every migration and records the newest version.
    assert _user_version(db_path) == LATEST_VERSION


def test_new_tables_and_global_row_exist(db, db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        table_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert NEW_TABLES <= table_names

        # The reserved 'global' project: exactly one row, with no path and no parent.
        global_rows = conn.execute("SELECT * FROM projects WHERE id = 'global'").fetchall()
        assert len(global_rows) == 1
        assert global_rows[0]["path"] is None
        assert global_rows[0]["parent"] is None
    finally:
        conn.close()


def test_new_columns_exist_on_ingestion_queue(db, db_path):
    conn = sqlite3.connect(db_path)
    try:
        column_names = {
            row[1] for row in conn.execute("PRAGMA table_info(ingestion_queue)")
        }
    finally:
        conn.close()
    assert NEW_COLUMNS <= column_names
    assert "launch_dir" in column_names


def test_source_id_column_exists_on_ingestion_queue(db, db_path):
    # Migration 3 adds source_id; it is nullable, so existing rows need no value.
    conn = sqlite3.connect(db_path)
    try:
        column_names = {
            row[1] for row in conn.execute("PRAGMA table_info(ingestion_queue)")
        }
    finally:
        conn.close()
    assert V3_COLUMNS <= column_names


def test_reopening_an_already_migrated_db_is_a_no_op(db_path):
    first = StateDB(db_path)
    first.close()

    # The second open must not raise, and the version must stay where it was.
    second = StateDB(db_path)
    second.close()
    assert _user_version(db_path) == LATEST_VERSION


def test_discover_or_update_writes_source_id_on_insert(db, db_path):
    db.discover_or_update("/f.md", "tester", 1.0, source_id="nas")

    assert _read_row(db_path, "/f.md")["source_id"] == "nas"


def test_discover_or_update_without_source_id_leaves_it_null(db, db_path):
    # The call shape without source_id, as before v0.3, stores NULL.
    db.discover_or_update("/g.md", "tester", 1.0)

    assert _read_row(db_path, "/g.md")["source_id"] is None


def test_discover_or_update_does_not_rewrite_source_id_on_update(db, db_path):
    # source_id is set once, at insert. A later re-queue with another value is a no-op for it.
    db.discover_or_update("/h.md", "tester", 1.0, source_id="nas")

    db.discover_or_update("/h.md", "tester", 2.0, source_id="claude-linux-shared-nfs")

    row = _read_row(db_path, "/h.md")
    assert row["status"] == "pending"
    assert row["last_modified"] == 2.0
    assert row["source_id"] == "nas"


def test_discover_or_update_sets_origin_path_and_first_seen_at_on_insert(db, db_path):
    db.discover_or_update(
        "/a.md", "tester", 1.0, origin_path="/a.md", source_type="file", file_type="md"
    )

    row = _read_row(db_path, "/a.md")
    assert row["origin_path"] == "/a.md"
    assert row["source_type"] == "file"
    assert row["file_type"] == "md"
    assert row["first_seen_at"] is not None


def test_existing_rows_default_to_global_project_id(db, db_path):
    # A row discovered through the normal path, with no project given, is in 'global'.
    db.discover_or_update("/b.md", "tester", 1.0)

    assert _read_row(db_path, "/b.md")["project_id"] == "global"


def test_mark_completed_writes_new_metadata_fields(db, db_path):
    db.discover_or_update("/c.md", "tester", 1.0)

    db.mark_completed(
        "/c.md",
        chunk_count=5,
        source_type="file",
        file_type="md",
        pipeline_version="p1",
        size_bytes=10,
        content_hash="abc",
        hashed_at="2026-01-01T00:00:00+00:00",
        completed_at="2026-01-01T00:00:01+00:00",
        launch_dir="/some/launch/dir",
    )

    row = _read_row(db_path, "/c.md")
    assert row["status"] == "completed"
    assert row["chunk_count"] == 5
    assert row["source_type"] == "file"
    assert row["file_type"] == "md"
    assert row["pipeline_version"] == "p1"
    assert row["size_bytes"] == 10
    assert row["content_hash"] == "abc"
    assert row["hashed_at"] == "2026-01-01T00:00:00+00:00"
    assert row["completed_at"] == "2026-01-01T00:00:01+00:00"
    assert row["launch_dir"] == "/some/launch/dir"


def test_mark_completed_without_extra_args_only_sets_status_and_timestamp(db, db_path):
    # The existing call shape, mark_completed(file_path), must behave as it did before.
    db.discover_or_update("/d.md", "tester", 1.0)

    db.mark_completed("/d.md")

    row = _read_row(db_path, "/d.md")
    assert row["status"] == "completed"
    assert row["completed_at"] is not None
    assert row["launch_dir"] is None
    assert row["chunk_count"] is None
    assert row["content_hash"] is None


def test_claim_next_pending_returns_origin_path_and_last_modified(db):
    db.discover_or_update("/e.md", "tester", 42.5, origin_path="/e.md")

    claimed = db.claim_next_pending()

    assert claimed["file_path"] == "/e.md"
    assert claimed["owner"] == "tester"
    assert claimed["origin_path"] == "/e.md"
    assert claimed["last_modified"] == 42.5


def test_live_shaped_db_migrates_in_place(db_path):
    # Build the database as the live one is: old schema, user_version 0, two rows.
    # Row 'a' has completed; row 'b' has not.
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    raw = sqlite3.connect(db_path)
    raw.executescript(OLD_LIVE_SCHEMA)
    raw.execute(
        "INSERT INTO ingestion_queue "
        "(file_path, owner, last_modified, status, completed_at, max_retries) "
        "VALUES ('/nas/a.md', 'tester', 1.0, 'completed', '2026-01-02T03:04:05+00:00', 3)"
    )
    raw.execute(
        "INSERT INTO ingestion_queue "
        "(file_path, owner, last_modified, status, completed_at, max_retries) "
        "VALUES ('/nas/b.md', 'tester', 2.0, 'pending', NULL, 5)"
    )
    raw.commit()
    assert raw.execute("PRAGMA user_version").fetchone()[0] == 0
    raw.close()

    # Opening through StateDB must migrate it without raising. The existing
    # skip_reason column makes migration 1's ALTER fail, and that must be tolerated.
    state = StateDB(db_path)
    state.close()

    assert _user_version(db_path) == LATEST_VERSION

    row_a = _read_row(db_path, "/nas/a.md")
    row_b = _read_row(db_path, "/nas/b.md")

    # Both rows are still there with their original data.
    assert row_a["owner"] == "tester"
    assert row_a["last_modified"] == 1.0
    assert row_a["status"] == "completed"
    assert row_a["completed_at"] == "2026-01-02T03:04:05+00:00"
    assert row_b["owner"] == "tester"
    assert row_b["last_modified"] == 2.0
    assert row_b["status"] == "pending"

    # The max_retries raise from migration 1 still applies to the old row.
    assert row_a["max_retries"] == 5

    # Every existing row is in the 'global' project.
    assert row_a["project_id"] == "global"
    assert row_b["project_id"] == "global"

    # Migration 3 adds source_id as NULL; the backfill sets it later.
    assert row_a["source_id"] is None
    assert row_b["source_id"] is None

    # first_seen_at comes from completed_at where completed_at was set, and stays NULL otherwise.
    assert row_a["first_seen_at"] == "2026-01-02T03:04:05+00:00"
    assert row_b["first_seen_at"] is None

    # The reserved 'global' project row exists.
    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM projects WHERE id = 'global'").fetchone()[0]
    finally:
        conn.close()
    assert count == 1
