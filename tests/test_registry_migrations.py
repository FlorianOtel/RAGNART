"""
Tests for the hub-registry schema in ragnart/registry.py: migration 1, the seeds and the open modes.

Covers:
  - a fresh registry reaching user_version 1 with the five tables;
  - the three seeded sources (secret NULL) and the reserved 'global' project row;
  - no unique index on projects.name (v0.4 adds it);
  - the project_references kind CHECK;
  - re-opening a migrated registry (idempotent);
  - read-only opens: a missing file is never created, and a write raises.

Every test runs on a temporary file under tmp_path. The production registry is never opened.
"""

from __future__ import annotations

import sqlite3

import pytest

from ragnart.registry import RegistryDB
from ragnart.schema import SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_NAS, SOURCE_ID_OPENCODE_K11

# The schema version that the newest registry migration produces.
LATEST_VERSION = 1

# The five tables migration 1 creates.
TABLES = {"sources", "projects", "project_tags", "project_references", "grants"}


def _user_tables(db: RegistryDB) -> set[str]:
    """Names of the tables in the database, leaving out SQLite's own sqlite_* tables."""
    rows = db._conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {row["name"] for row in rows}


def _user_version(db: RegistryDB) -> int:
    return db._conn.execute("PRAGMA user_version").fetchone()[0]


def test_fresh_registry_reaches_latest_version_with_five_tables(tmp_path) -> None:
    db = RegistryDB(str(tmp_path / "sqlite" / "hub_registry.db"))
    try:
        assert _user_version(db) == LATEST_VERSION
        assert _user_tables(db) == TABLES
    finally:
        db.close()


def test_fresh_open_creates_the_parent_directory(tmp_path) -> None:
    path = tmp_path / "nested" / "sqlite" / "hub_registry.db"
    db = RegistryDB(str(path))
    db.close()
    assert path.exists()


def test_three_sources_are_seeded_with_no_secret(tmp_path) -> None:
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    try:
        sources = {row["id"]: row for row in db.list_sources()}
        assert set(sources) == {SOURCE_ID_NAS, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_OPENCODE_K11}
        assert sources[SOURCE_ID_NAS]["connector"] == "filesystem"
        assert sources[SOURCE_ID_CLAUDE_LINUX_SHARED_NFS]["connector"] == "claude_code"
        assert sources[SOURCE_ID_OPENCODE_K11]["connector"] == "opencode"
        assert sources[SOURCE_ID_OPENCODE_K11]["owner_binding"] == "opencode.owner in ragnart.yaml"

        # The secret column exists, but is NULL for every seeded source until session B.
        secrets = db._conn.execute("SELECT secret FROM sources").fetchall()
        assert [row["secret"] for row in secrets] == [None, None, None]
    finally:
        db.close()


def test_global_project_row_is_seeded_as_the_state_db_seeds_it(tmp_path) -> None:
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    try:
        assert db.list_projects() == [
            {"id": "global", "path": None, "name": "Global", "owner": None, "home_source_id": None},
        ]
    finally:
        db.close()


def test_projects_has_no_unique_index_on_name(tmp_path) -> None:
    """Name uniqueness is a v0.4 rule. Only the primary key may be a unique index on projects."""
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    try:
        indexes = db._conn.execute("PRAGMA index_list(projects)").fetchall()
        # origin 'pk' is the primary-key index on id; anything else would be a CREATE INDEX.
        assert all(row["origin"] == "pk" for row in indexes)
    finally:
        db.close()


def test_project_references_kind_is_checked(tmp_path) -> None:
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    try:
        db._conn.execute(
            "INSERT INTO project_references (project_id, kind, target_id) VALUES ('p', 'session', 's')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            db._conn.execute(
                "INSERT INTO project_references (project_id, kind, target_id) VALUES ('p', 'folder', 'x')"
            )
    finally:
        db.close()


def test_reopening_a_migrated_registry_changes_nothing(tmp_path) -> None:
    path = str(tmp_path / "hub_registry.db")
    first = RegistryDB(path)
    first.upsert_project("pid-a", "/nas/a", "A", "tester", SOURCE_ID_NAS)
    first.close()

    second = RegistryDB(path)
    try:
        assert _user_version(second) == LATEST_VERSION
        assert len(second.list_sources()) == 3
        ids = [p["id"] for p in second.list_projects()]
        assert ids == ["global", "pid-a"]
    finally:
        second.close()


def test_read_only_open_of_a_missing_file_raises_and_creates_nothing(tmp_path) -> None:
    path = tmp_path / "missing" / "hub_registry.db"
    with pytest.raises(sqlite3.OperationalError):
        RegistryDB(str(path), read_only=True)
    assert not path.exists()
    assert not path.parent.exists()


def test_read_only_open_reads_and_refuses_writes(tmp_path) -> None:
    path = str(tmp_path / "hub_registry.db")
    RegistryDB(path).close()

    reader = RegistryDB(path, read_only=True)
    try:
        assert [s["id"] for s in reader.list_sources()] == sorted(
            [SOURCE_ID_NAS, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_OPENCODE_K11]
        )
        with pytest.raises(sqlite3.OperationalError):
            reader.upsert_project("pid-x", "/nas/x", "X", "tester", SOURCE_ID_NAS)
    finally:
        reader.close()
