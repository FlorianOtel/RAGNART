"""
Tests for the hub-registry schema in ragnart/registry.py: migration 1, the seeds and the open modes.

Covers:
  - a fresh registry reaching user_version 2 with the six tables;
  - the three seeded sources (secret_hash NULL, kind 'hub') and the reserved 'global' project row;
  - migration 2 on a registry at version 1: the rename, the new columns, the hub kinds, the
    case-insensitive unique index, and the same schema as a fresh registry;
  - no unique index on projects.name (v0.4 adds it);
  - the project_references kind CHECK;
  - re-opening a migrated registry (idempotent);
  - read-only opens: a missing file is never created, and a write raises.

Every test runs on a temporary file under tmp_path. The production registry is never opened.
"""

from __future__ import annotations

import sqlite3

import pytest

from ragnart import registry as registry_module
from ragnart.registry import RegistryDB
from ragnart.schema import SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_NAS, SOURCE_ID_OPENCODE_K11

# The schema version that the newest registry migration produces.
LATEST_VERSION = 2

# The six tables: five from migration 1, plus intake from migration 2.
TABLES = {"sources", "projects", "project_tags", "project_references", "grants", "intake"}

# The three hub sources that migration 1 seeds.
HUB_SOURCE_IDS = {SOURCE_ID_NAS, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_OPENCODE_K11}


def _user_tables(db: RegistryDB) -> set[str]:
    """Names of the tables in the database, leaving out SQLite's own sqlite_* tables."""
    rows = db._conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {row["name"] for row in rows}


def _user_version(db: RegistryDB) -> int:
    return db._conn.execute("PRAGMA user_version").fetchone()[0]


def _table_columns(db: RegistryDB, table: str) -> list[tuple]:
    """The columns of one table as PRAGMA table_info rows: (cid, name, type, notnull, default, pk)."""
    return [tuple(row) for row in db._conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _index_names(db: RegistryDB, table: str) -> set[str]:
    """The names of the indexes on one table, including SQLite's automatic primary-key index."""
    return {row["name"] for row in db._conn.execute(f"PRAGMA index_list({table})").fetchall()}


def test_fresh_registry_reaches_latest_version_with_six_tables(tmp_path) -> None:
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


def test_three_sources_are_seeded_with_no_secret_hash(tmp_path) -> None:
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    try:
        sources = {row["id"]: row for row in db.list_sources()}
        assert set(sources) == HUB_SOURCE_IDS
        assert sources[SOURCE_ID_NAS]["connector"] == "filesystem"
        assert sources[SOURCE_ID_CLAUDE_LINUX_SHARED_NFS]["connector"] == "claude_code"
        assert sources[SOURCE_ID_OPENCODE_K11]["connector"] == "opencode"
        assert sources[SOURCE_ID_OPENCODE_K11]["owner_binding"] == "opencode.owner in ragnart.yaml"

        # The seeded sources have no secret: they are not submitted to through the API.
        hashes = db._conn.execute("SELECT secret_hash FROM sources").fetchall()
        assert [row["secret_hash"] for row in hashes] == [None, None, None]
    finally:
        db.close()


def test_secret_column_is_renamed_to_secret_hash(tmp_path) -> None:
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    try:
        names = {column[1] for column in _table_columns(db, "sources")}
        assert "secret_hash" in names
        assert "secret" not in names
    finally:
        db.close()


def test_seeded_sources_are_hub_kind_and_hub_source_ids_lists_them(tmp_path) -> None:
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    try:
        kinds = {row["id"]: row["kind"] for row in db._conn.execute("SELECT id, kind FROM sources")}
        assert kinds == {source_id: "hub" for source_id in HUB_SOURCE_IDS}
        assert db.hub_source_ids() == HUB_SOURCE_IDS
    finally:
        db.close()


def _open_at_version_one(path: str, monkeypatch) -> None:
    """
    Create a registry at user_version 1: open it with migration 1 only, then close it.

    registry_module reads _MIGRATIONS when a RegistryDB is opened, so the patch is in force only
    for that open. The patch is undone when the with-block ends.
    """
    with monkeypatch.context() as patch:
        patch.setattr(registry_module, "_MIGRATIONS", registry_module._MIGRATIONS[:1])
        RegistryDB(path).close()


def test_version_one_registry_upgrades_to_the_same_schema_as_a_fresh_one(tmp_path, monkeypatch) -> None:
    old_path = str(tmp_path / "old" / "hub_registry.db")
    _open_at_version_one(old_path, monkeypatch)

    # Confirm the starting point really is version 1 with the old secret column.
    raw = sqlite3.connect(old_path)
    try:
        assert raw.execute("PRAGMA user_version").fetchone()[0] == 1
        assert "secret" in {column[1] for column in raw.execute("PRAGMA table_info(sources)")}
    finally:
        raw.close()

    upgraded = RegistryDB(old_path)
    fresh = RegistryDB(str(tmp_path / "fresh" / "hub_registry.db"))
    try:
        assert _user_version(upgraded) == LATEST_VERSION
        assert _user_tables(upgraded) == TABLES
        assert _table_columns(upgraded, "sources") == _table_columns(fresh, "sources")
        assert _table_columns(upgraded, "intake") == _table_columns(fresh, "intake")
        assert _index_names(upgraded, "sources") == _index_names(fresh, "sources")
        assert _index_names(upgraded, "intake") == _index_names(fresh, "intake")
        assert upgraded.hub_source_ids() == HUB_SOURCE_IDS
    finally:
        upgraded.close()
        fresh.close()


def test_migration_two_adds_owner_and_kind_and_the_case_insensitive_index(tmp_path) -> None:
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    try:
        names = {column[1] for column in _table_columns(db, "sources")}
        assert {"kind", "owner", "secret_hash"} <= names
        assert "idx_sources_id_nocase" in _index_names(db, "sources")
    finally:
        db.close()


def test_sources_kind_is_checked_to_hub_or_endpoint(tmp_path) -> None:
    """A kind other than 'hub' or 'endpoint' is refused by the table itself, not only by register_source."""
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    try:
        # 'endpoint' is an allowed value, so the CHECK must accept it.
        db._conn.execute(
            "INSERT INTO sources (id, connector, kind, owner) VALUES ('ok-endpoint', 'filesystem', 'endpoint', 'tester')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            db._conn.execute(
                "INSERT INTO sources (id, connector, kind) VALUES ('bad-kind', 'filesystem', 'x')"
            )
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
