"""
Tests for the project methods of ragnart/registry.py RegistryDB.

Covers upsert_project (insert, update, and an upsert over the seeded 'global' row),
replace_project_tags, project_tags, project_ids_by_tag, list_projects (shape: no parent,
mode or inherit_access), list_sources (no secret), and remove_project (tags, references
in both directions, grants; other projects untouched).

Every test runs on a RegistryDB in tmp_path. No queue database and no Qdrant server are used.
"""

from __future__ import annotations

import pytest

from ragnart.registry import RegistryDB
from ragnart.schema import SOURCE_ID_NAS


@pytest.fixture
def registry(tmp_path):
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    yield db
    db.close()


def _project(registry: RegistryDB, project_id: str, name: str = "P") -> None:
    registry.upsert_project(project_id, f"/nas/{project_id}", name, "tester", SOURCE_ID_NAS)


def test_upsert_inserts_a_new_project(registry) -> None:
    registry.upsert_project("pid-a", "/nas/a", "Alpha", "tester", SOURCE_ID_NAS)

    row = {p["id"]: p for p in registry.list_projects()}["pid-a"]
    assert row == {
        "id": "pid-a",
        "path": "/nas/a",
        "name": "Alpha",
        "owner": "tester",
        "home_source_id": SOURCE_ID_NAS,
    }


def test_upsert_updates_the_row_with_the_same_id(registry) -> None:
    registry.upsert_project("pid-a", "/nas/a", "Alpha", "tester", SOURCE_ID_NAS)

    registry.upsert_project("pid-a", "/nas/moved", "Alpha Renamed", "someone", SOURCE_ID_NAS)

    projects = [p for p in registry.list_projects() if p["id"] == "pid-a"]
    assert len(projects) == 1
    assert projects[0]["path"] == "/nas/moved"
    assert projects[0]["name"] == "Alpha Renamed"
    assert projects[0]["owner"] == "someone"


def test_upsert_over_the_seeded_global_row_updates_it_in_place(registry) -> None:
    registry.upsert_project("global", "/nas/global", "Global Renamed", "tester", SOURCE_ID_NAS)

    globals_ = [p for p in registry.list_projects() if p["id"] == "global"]
    assert len(globals_) == 1
    assert globals_[0]["name"] == "Global Renamed"


def test_list_projects_has_only_the_v03_columns(registry) -> None:
    _project(registry, "pid-a")

    for project in registry.list_projects():
        assert set(project) == {"id", "path", "name", "owner", "home_source_id"}


def test_replace_project_tags_replaces_the_whole_set(registry) -> None:
    _project(registry, "pid-a")
    registry.replace_project_tags("pid-a", {"kind": "customer", "region": "eu"})

    registry.replace_project_tags("pid-a", {"kind": "internal"})

    assert registry.project_tags("pid-a") == {"kind": "internal"}


def test_project_tags_is_empty_for_a_project_with_none(registry) -> None:
    assert registry.project_tags("no-such-project") == {}


def test_project_ids_by_tag_returns_sorted_exact_matches(registry) -> None:
    for project_id in ("pid-b", "pid-a", "pid-c"):
        _project(registry, project_id)
    registry.replace_project_tags("pid-b", {"kind": "customer"})
    registry.replace_project_tags("pid-a", {"kind": "customer"})
    registry.replace_project_tags("pid-c", {"kind": "customers"})

    assert registry.project_ids_by_tag("kind", "customer") == ["pid-a", "pid-b"]
    assert registry.project_ids_by_tag("kind", "nothing") == []


def test_list_sources_returns_no_secret_column(registry) -> None:
    sources = registry.list_sources()

    assert [s["id"] for s in sources] == sorted(s["id"] for s in sources)
    for source in sources:
        assert set(source) == {"id", "connector", "owner_binding"}


def test_remove_project_deletes_its_tags_references_grants_and_row(registry) -> None:
    _project(registry, "pid-a")
    _project(registry, "pid-b")
    registry.replace_project_tags("pid-a", {"kind": "customer"})
    registry._conn.executemany(
        "INSERT INTO project_references (project_id, kind, target_id, working) VALUES (?, ?, ?, ?)",
        [
            ("pid-a", "session", "sess-1", 0),     # a reference the removed project makes
            ("pid-b", "project", "pid-a", 0),      # a reference another project makes to the removed one
            ("pid-b", "session", "sess-2", 0),     # an unrelated reference, must survive
        ],
    )
    registry._conn.execute(
        "INSERT INTO grants (project_id, principal, role) VALUES ('pid-a', 'eva', 'reader')"
    )
    registry._conn.execute(
        "INSERT INTO grants (project_id, principal, role) VALUES ('pid-b', 'eva', 'reader')"
    )
    registry._conn.commit()

    registry.remove_project("pid-a")

    assert [p["id"] for p in registry.list_projects()] == ["global", "pid-b"]
    assert registry.project_tags("pid-a") == {}
    refs = registry._conn.execute(
        "SELECT project_id, kind, target_id FROM project_references ORDER BY project_id, target_id"
    ).fetchall()
    assert [tuple(r) for r in refs] == [("pid-b", "session", "sess-2")]
    grants = registry._conn.execute("SELECT project_id FROM grants").fetchall()
    assert [g["project_id"] for g in grants] == ["pid-b"]
