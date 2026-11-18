"""
Tests for utils/rag_registry_migrate.py: the copy, the row-for-row verification, the refusal,
the gated drop, and the re-run after the drop.

Both databases are real, on temporary SQLite files: a StateDB (fresh, so it carries the legacy
project tables and the reserved 'global' row) and a RegistryDB. No production file is opened.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from ragnart.registry import RegistryDB
from ragnart.schema import SOURCE_ID_NAS
from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent
REPO_PATH = "/mnt/nfs/Florian/Gin-AI/projects/RAGNART"


@pytest.fixture
def migrate(monkeypatch):
    """Load utils/rag_registry_migrate.py as a module. It is a script, not a package."""
    name = "rag_registry_migrate"
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "utils" / "rag_registry_migrate.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def legacy(tmp_path):
    """A fresh rag_state.db. Its legacy tables exist and hold only the seeded 'global' row."""
    db = StateDB(str(tmp_path / "sqlite" / "rag_state.db"))
    yield db
    db.close()


@pytest.fixture
def registry(tmp_path):
    db = RegistryDB(str(tmp_path / "sqlite" / "hub_registry.db"))
    yield db
    db.close()


def _exec(db: StateDB, sql: str, params: tuple = ()) -> None:
    db._conn.execute(sql, params)
    db._conn.commit()


def _add_legacy_project(db: StateDB, project_id: str, path: str | None, name: str, owner: str | None) -> None:
    _exec(
        db,
        "INSERT INTO projects (id, path, name, parent, owner, inherit_access, mode) "
        "VALUES (?, ?, ?, NULL, ?, 1, NULL)",
        (project_id, path, name, owner),
    )


def _seed_legacy(db: StateDB) -> None:
    """One marked project with a tag and a grant, plus the seeded 'global' row."""
    _add_legacy_project(db, "RAGNART", REPO_PATH, "RAGNART", "florian")
    _exec(db, "INSERT INTO project_tags (project_id, key, value) VALUES (?, ?, ?)",
          ("RAGNART", "kind", "repo"))
    _exec(db, "INSERT INTO grants (project_id, principal, role, granted_by, granted_at) "
              "VALUES (?, ?, ?, ?, ?)",
          ("RAGNART", "eva", "read", "florian", "2026-11-01T00:00:00+00:00"))


def _table_exists(db: StateDB, table: str) -> bool:
    return db._conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone() is not None


def _user_version(db: StateDB) -> int:
    return db._conn.execute("PRAGMA user_version").fetchone()[0]


# ---------------------------------------------------------------------------
# Happy path and copy rules
# ---------------------------------------------------------------------------

def test_happy_path_copies_projects_tags_and_grants_and_verifies(migrate, legacy, registry):
    _seed_legacy(legacy)

    report = migrate.run_apply(legacy, registry, drop_legacy=False)

    assert report["refused"] is None
    assert report["mismatches"] == []
    assert report["copied"] == {"projects": 2, "tags": 1, "grants": 1}
    assert report["dropped"] is False
    # Every legacy table is still there: no drop was asked for.
    assert _table_exists(legacy, "projects")

    assert registry.list_projects() == [
        {"id": "RAGNART", "path": REPO_PATH, "name": "RAGNART", "owner": "florian",
         "home_source_id": SOURCE_ID_NAS},
        {"id": "global", "path": None, "name": "Global", "owner": None, "home_source_id": None},
    ]
    assert registry.project_tags("RAGNART") == {"kind": "repo"}
    assert registry.list_grants() == [
        ("RAGNART", "eva", "read", "florian", "2026-11-01T00:00:00+00:00"),
    ]


def test_copy_is_idempotent_when_run_twice(migrate, legacy, registry):
    _seed_legacy(legacy)

    migrate.run_apply(legacy, registry, drop_legacy=False)
    projects_once = registry.list_projects()
    tags_once = registry.list_all_tags()
    grants_once = registry.list_grants()

    second = migrate.run_apply(legacy, registry, drop_legacy=False)

    assert second["mismatches"] == []
    assert registry.list_projects() == projects_once
    assert registry.list_all_tags() == tags_once
    assert registry.list_grants() == grants_once


def test_a_tag_removed_from_the_legacy_table_is_removed_from_the_registry_on_rerun(migrate, legacy, registry):
    # The copy replaces each project's tag set, so the registry matches the legacy rows exactly.
    _seed_legacy(legacy)
    migrate.run_apply(legacy, registry, drop_legacy=False)
    _exec(legacy, "DELETE FROM project_tags WHERE project_id = ?", ("RAGNART",))
    _exec(legacy, "INSERT INTO project_tags (project_id, key, value) VALUES (?, ?, ?)",
          ("RAGNART", "kind", "notes"))

    report = migrate.run_apply(legacy, registry, drop_legacy=False)

    assert report["mismatches"] == []
    assert registry.project_tags("RAGNART") == {"kind": "notes"}


# ---------------------------------------------------------------------------
# Refusal
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", ["project_links", "source_links"])
def test_non_empty_link_table_refuses_and_copies_nothing(migrate, legacy, registry, table):
    _seed_legacy(legacy)
    if table == "project_links":
        _exec(legacy, "INSERT INTO project_links (project_id, linked_project_id) VALUES (?, ?)",
              ("RAGNART", "global"))
    else:
        _exec(legacy, "INSERT INTO source_links (project_id, source_path) VALUES (?, ?)",
              ("RAGNART", REPO_PATH + "/docs"))

    report = migrate.run_apply(legacy, registry, drop_legacy=True)

    assert report["refused"] is not None
    assert table in report["refused"]
    assert report["copied"] is None
    assert report["dropped"] is False
    # Nothing reached the registry: only the seeded global row exists there.
    assert [p["id"] for p in registry.list_projects()] == ["global"]
    assert _table_exists(legacy, "projects")
    assert migrate.exit_code(report) == 1


# ---------------------------------------------------------------------------
# Verification and the gated drop
# ---------------------------------------------------------------------------

def test_verification_mismatch_refuses_the_drop(migrate, legacy, registry):
    # A real mismatch, not a simulated one: a tag row whose project is not in `projects` is
    # not copied (the copy walks the projects), so the registry differs from the legacy tables.
    _seed_legacy(legacy)
    _exec(legacy, "INSERT INTO project_tags (project_id, key, value) VALUES (?, ?, ?)",
          ("ORPHAN", "kind", "repo"))

    report = migrate.run_apply(legacy, registry, drop_legacy=True)

    assert report["mismatches"]
    assert any(line.startswith("project_tags:") for line in report["mismatches"])
    assert report["dropped"] is False
    assert _table_exists(legacy, "projects")
    assert _table_exists(legacy, "project_tags")
    assert _user_version(legacy) == 4  # StateDB set 4 on open; a refused drop leaves it as is
    assert migrate.exit_code(report) == 1


def test_simulated_verification_failure_also_refuses_the_drop(migrate, legacy, registry, monkeypatch):
    _seed_legacy(legacy)
    monkeypatch.setattr(migrate, "verify_copy", lambda legacy_rows, reg: ["projects: simulated"])

    report = migrate.run_apply(legacy, registry, drop_legacy=True)

    assert report["dropped"] is False
    assert _table_exists(legacy, "projects")
    assert _user_version(legacy) == 4  # StateDB set 4 on open; a refused drop leaves it as is


def test_drop_removes_the_legacy_tables_and_sets_user_version_4(migrate, legacy, registry):
    _seed_legacy(legacy)

    report = migrate.run_apply(legacy, registry, drop_legacy=True)

    assert report["mismatches"] == []
    assert report["dropped"] is True
    for table in migrate.LEGACY_TABLES:
        assert not _table_exists(legacy, table), table
    assert _user_version(legacy) == 4
    # The queue itself is untouched by the drop.
    assert _table_exists(legacy, "ingestion_queue")
    # The registry keeps every project, tag and grant.
    assert registry.project_tags("RAGNART") == {"kind": "repo"}
    assert len(registry.list_grants()) == 1


def test_drop_works_on_a_db_statedb_already_advanced_to_version_4(migrate, legacy, registry):
    # StateDB has already set user_version 4 (migration 4 is empty) and the legacy tables are
    # present. The drop must decide by table presence, so it still copies, verifies and drops.
    _seed_legacy(legacy)
    assert _user_version(legacy) == 4
    assert _table_exists(legacy, "projects")

    report = migrate.run_apply(legacy, registry, drop_legacy=True)

    assert report["already_dropped"] is False
    assert report["mismatches"] == []
    assert report["dropped"] is True
    for table in migrate.LEGACY_TABLES:
        assert not _table_exists(legacy, table), table
    assert _user_version(legacy) == 4
    assert registry.project_tags("RAGNART") == {"kind": "repo"}


def test_db_at_version_3_auto_advanced_by_statedb_still_drops(migrate, tmp_path):
    # A database last opened at version 3 (before migration 4 existed). Opening it with StateDB
    # sets version 4 without dropping anything; the tool must then still copy and drop.
    path = str(tmp_path / "sqlite" / "rag_state.db")
    first = StateDB(path)
    _seed_legacy(first)
    first._conn.execute("PRAGMA user_version = 3")
    first._conn.commit()
    first.close()

    state = StateDB(path)
    registry = RegistryDB(str(tmp_path / "sqlite" / "hub_registry.db"))
    try:
        assert _user_version(state) == 4
        assert _table_exists(state, "projects")

        report = migrate.run_apply(state, registry, drop_legacy=True)

        assert report["mismatches"] == []
        assert report["dropped"] is True
        assert not _table_exists(state, "projects")
        assert _user_version(state) == 4
    finally:
        registry.close()
        state.close()


def test_rerun_after_the_drop_is_safe(migrate, legacy, registry):
    _seed_legacy(legacy)
    migrate.run_apply(legacy, registry, drop_legacy=True)
    projects_after_drop = registry.list_projects()

    report = migrate.run_apply(legacy, registry, drop_legacy=True)

    assert report["already_dropped"] is True
    assert report["mismatches"] == []
    assert report["dropped"] is False
    assert registry.list_projects() == projects_after_drop
    assert _user_version(legacy) == 4
    assert migrate.exit_code(report) == 0


def test_after_the_drop_a_fresh_statedb_open_still_works(migrate, legacy, registry, tmp_path):
    # user_version 4 is above every StateDB migration, so reopening runs nothing and the queue works.
    _seed_legacy(legacy)
    migrate.run_apply(legacy, registry, drop_legacy=True)

    reopened = StateDB(str(tmp_path / "sqlite" / "rag_state.db"))
    try:
        assert _user_version(reopened) == 4
        assert not _table_exists(reopened, "projects")
        assert reopened.item_counts_by_project() == {}
    finally:
        reopened.close()


def test_drop_refuses_when_the_source_id_column_is_missing(migrate, tmp_path):
    # A database at user_version 2 has no source_id column. user_version 4 would make StateDB
    # skip migration 3, so the drop must refuse and leave every table in place.
    path = tmp_path / "old.db"
    raw = sqlite3.connect(str(path))
    raw.execute("CREATE TABLE ingestion_queue (file_path TEXT PRIMARY KEY)")
    raw.execute("CREATE TABLE projects (id TEXT PRIMARY KEY, path TEXT, name TEXT, owner TEXT)")
    raw.execute("PRAGMA user_version = 2")
    raw.commit()
    fake_state = SimpleNamespace(_conn=raw)

    with pytest.raises(RuntimeError, match="source_id"):
        migrate.drop_legacy_tables(fake_state)

    assert raw.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='projects'"
    ).fetchone() is not None
    assert raw.execute("PRAGMA user_version").fetchone()[0] == 2
    raw.close()


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------

def test_dry_run_reports_counts_and_writes_nothing(migrate, legacy, registry, tmp_path):
    _seed_legacy(legacy)
    conn = sqlite3.connect((tmp_path / "sqlite" / "rag_state.db").absolute().as_uri() + "?mode=ro", uri=True)
    try:
        report = migrate.dry_run_report(conn, registry)
    finally:
        conn.close()

    assert report["counts"]["projects"] == 2
    assert report["counts"]["project_links"] == 0
    assert report["would_copy"] == {"projects": 2, "tags": 1, "grants": 1}
    assert report["refused"] is None
    assert report["registry"]["projects"] == 1  # only the seeded global row
    assert [p["id"] for p in registry.list_projects()] == ["global"]
    assert migrate.exit_code(report) == 0


def test_dry_run_reports_absent_tables_as_absent(migrate, tmp_path):
    path = tmp_path / "bare.db"
    raw = sqlite3.connect(str(path))
    raw.execute("CREATE TABLE ingestion_queue (file_path TEXT PRIMARY KEY)")
    raw.commit()
    raw.close()

    conn = sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True)
    try:
        report = migrate.dry_run_report(conn, None)
    finally:
        conn.close()

    assert report["counts"] == {table: None for table in migrate.LEGACY_TABLES}
    assert report["registry"] is None
    assert report["would_copy"] == {"projects": 0, "tags": 0, "grants": 0}


def test_print_report_covers_dry_run_refusal_and_apply(migrate, legacy, registry, capsys, tmp_path):
    # The printouts are what the operator reads. Each branch must render without error.
    _seed_legacy(legacy)
    conn = sqlite3.connect((tmp_path / "sqlite" / "rag_state.db").absolute().as_uri() + "?mode=ro", uri=True)
    try:
        migrate.print_report(migrate.dry_run_report(conn, None))
    finally:
        conn.close()
    assert "DRY-RUN" in capsys.readouterr().out

    migrate.print_report(migrate.run_apply(legacy, registry, drop_legacy=True))
    out = capsys.readouterr().out
    assert "verification PASSED" in out
    assert "legacy tables dropped" in out

    migrate.print_report(migrate.run_apply(legacy, registry, drop_legacy=True))
    assert "already dropped" in capsys.readouterr().out
