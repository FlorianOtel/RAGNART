"""
Tests for the project methods of ragnart/registry.py RegistryDB.

Covers upsert_project (insert, update, and an upsert over the seeded 'global' row),
replace_project_tags, project_tags, project_ids_by_tag, list_projects (shape: no parent,
mode or inherit_access), list_sources (no secret), and remove_project (tags, references
in both directions, grants; other projects untouched).

Also covers the sources methods (register_source, source_by_id, sources_with_secret) and the
intake methods (insert_intake, intake_by_id, next_intake, update_intake, delete_intake_for_source).

Every test runs on a RegistryDB in tmp_path. No queue database and no Qdrant server are used.
"""

from __future__ import annotations

import sqlite3

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


# ---------------------------------------------------------------------------
# Sources: registration, lookup and the case-insensitive id rule
# ---------------------------------------------------------------------------

def _register_mbp(registry: RegistryDB) -> None:
    """Register the endpoint source 'MBP' with a fixed hash, owned by florian."""
    registry.register_source("MBP", "filesystem", "endpoint", "florian", "hash-mbp")


def test_register_source_stores_the_id_in_its_case(registry) -> None:
    _register_mbp(registry)

    assert registry.source_by_id("MBP")["id"] == "MBP"


def test_source_by_id_is_case_insensitive_and_returns_the_stored_id(registry) -> None:
    _register_mbp(registry)

    found = registry.source_by_id("mbp")

    assert found == {
        "id": "MBP",
        "connector": "filesystem",
        "kind": "endpoint",
        "owner": "florian",
        "secret_hash": "hash-mbp",
    }


def test_source_by_id_finds_a_seeded_hub_source_in_any_case(registry) -> None:
    found = registry.source_by_id("NAS")

    assert found["id"] == SOURCE_ID_NAS
    assert found["kind"] == "hub"
    assert found["owner"] is None
    assert found["secret_hash"] is None


def test_source_by_id_returns_none_for_an_unknown_id(registry) -> None:
    assert registry.source_by_id("nope") is None


def test_list_sources_never_returns_the_secret_hash(registry) -> None:
    _register_mbp(registry)

    for source in registry.list_sources():
        assert "secret_hash" not in source
        assert set(source) == {"id", "connector", "owner_binding"}


@pytest.mark.parametrize("source_id", ["Nas", "NAS", "claude-LINUX-shared-nfs"])
def test_registering_a_case_only_duplicate_of_a_seeded_source_is_refused(registry, source_id) -> None:
    with pytest.raises(ValueError, match="already registered"):
        registry.register_source(source_id, "filesystem", "endpoint", "florian", "hash-x")


def test_registering_a_case_only_duplicate_of_an_endpoint_source_is_refused(registry) -> None:
    _register_mbp(registry)

    with pytest.raises(ValueError, match="already registered"):
        registry.register_source("mbp", "filesystem", "endpoint", "florian", "hash-y")


@pytest.mark.parametrize("source_id", ["TAG", "Global", "has:colon", "has_underscore", ""])
def test_registering_an_invalid_or_reserved_id_is_refused(registry, source_id) -> None:
    with pytest.raises(ValueError):
        registry.register_source(source_id, "filesystem", "endpoint", "florian", "hash-z")


def test_registering_an_unknown_kind_is_refused(registry) -> None:
    with pytest.raises(ValueError, match="unknown source kind"):
        registry.register_source("MBP", "filesystem", "bogus", "florian", "hash-z")


# ---------------------------------------------------------------------------
# Intake: round trip, claim order, updates, per-source delete
# ---------------------------------------------------------------------------

def test_intake_round_trip_keeps_the_payload_and_starts_received(registry) -> None:
    request_id = registry.insert_intake(
        "MBP", "upsert", "/Users/fotel/MBP/a.md",
        file_type="md", text="hello", metadata={"owner": "florian"},
    )

    row = registry.intake_by_id(request_id)

    assert len(request_id) == 32  # uuid4 hex
    assert row["id"] == request_id
    assert row["source_id"] == "MBP"
    assert row["kind"] == "upsert"
    assert row["key"] == "/Users/fotel/MBP/a.md"
    assert row["file_type"] == "md"
    assert row["text"] == "hello"
    assert row["metadata"] == {"owner": "florian"}
    assert row["status"] == "received"
    assert row["created_at"] == row["updated_at"]


def test_intake_by_id_returns_none_for_an_unknown_request(registry) -> None:
    assert registry.intake_by_id("0" * 32) is None


def test_intake_metadata_none_stays_none(registry) -> None:
    request_id = registry.insert_intake("MBP", "delete", "/Users/fotel/MBP/a.md")

    assert registry.intake_by_id(request_id)["metadata"] is None


def test_intake_rejects_an_unknown_kind(registry) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        registry.insert_intake("MBP", "rename", "/Users/fotel/MBP/a.md")


def test_next_intake_claims_in_insertion_order_and_marks_processing(registry) -> None:
    first = registry.insert_intake("MBP", "delete", "/k/1")
    second = registry.insert_intake("MBP", "delete", "/k/2")
    third = registry.insert_intake("MBP", "delete", "/k/3")

    claimed = [registry.next_intake()["id"] for _ in range(3)]

    assert claimed == [first, second, third]
    assert registry.intake_by_id(first)["status"] == "processing"
    assert registry.next_intake() is None


def test_next_intake_skips_rows_that_are_not_received(registry) -> None:
    done = registry.insert_intake("MBP", "delete", "/k/done")
    waiting = registry.insert_intake("MBP", "delete", "/k/waiting")
    registry.update_intake(done, status="completed")

    assert registry.next_intake()["id"] == waiting


def test_update_intake_sets_result_columns_and_bumps_updated_at(registry) -> None:
    request_id = registry.insert_intake("MBP", "upsert", "/k/1", text="x")
    before = registry.intake_by_id(request_id)

    registry.update_intake(request_id, status="completed", chunk_count=3, content_hash="abc",
                           pipeline_version="p1")

    row = registry.intake_by_id(request_id)
    assert (row["status"], row["chunk_count"], row["content_hash"], row["pipeline_version"]) == (
        "completed", 3, "abc", "p1",
    )
    assert row["updated_at"] >= before["updated_at"]
    assert row["text"] == "x"  # the request's own payload is untouched


def test_update_intake_refuses_a_request_field_and_an_unknown_name(registry) -> None:
    request_id = registry.insert_intake("MBP", "upsert", "/k/1", text="x")

    with pytest.raises(ValueError, match="cannot update intake field"):
        registry.update_intake(request_id, key="/k/other")
    # A name that is not a column at all is refused before any SQL is built.
    with pytest.raises(ValueError, match="cannot update intake field"):
        registry.update_intake(request_id, **{"status = 'completed'; --": "x"})


def test_delete_intake_for_source_removes_only_that_source_in_any_case(registry) -> None:
    registry.insert_intake("MBP", "delete", "/k/1")
    registry.insert_intake("mbp", "delete", "/k/2")
    keep = registry.insert_intake("claude-MBP", "delete", "/k/3")

    removed = registry.delete_intake_for_source("Mbp")

    assert removed == 2
    assert registry.intake_by_id(keep) is not None


def test_insert_intake_uses_the_caller_request_id(registry) -> None:
    """A caller-supplied request id is the row's id, so a spool file can be named before the row exists."""
    request_id = registry.insert_intake(SOURCE_ID_NAS, "delete", "/nas/a.md", request_id="abc123")

    assert request_id == "abc123"
    assert registry.intake_by_id("abc123")["key"] == "/nas/a.md"


def test_insert_intake_stores_owner_tag_and_project_id(registry) -> None:
    """The B2 upsert fields are stored on the row as given; when absent they read back as None."""
    with_fields = registry.insert_intake(
        "nas", "upsert", "/mnt/nfs/Florian/a.md", owner="florian", tag="cisco", project_id="global",
    )
    without = registry.insert_intake("nas", "upsert", "/mnt/nfs/Florian/b.md")

    row = registry.intake_by_id(with_fields)
    assert (row["owner"], row["tag"], row["project_id"]) == ("florian", "cisco", "global")
    bare = registry.intake_by_id(without)
    assert (bare["owner"], bare["tag"], bare["project_id"]) == (None, None, None)


def test_sources_with_secret_lists_every_kind_that_holds_a_hash(registry) -> None:
    """sources_with_secret covers hub and endpoint sources alike, and leaves out rows without a hash."""
    registry.register_source("MBP", "filesystem", "endpoint", "florian", "f" * 64)
    # The seeded hub sources have no secret, so they are not listed until one is stored.
    assert [row["id"] for row in registry.sources_with_secret()] == ["MBP"]

    registry._conn.execute("UPDATE sources SET secret_hash = ? WHERE id = ?", ("h" * 64, SOURCE_ID_NAS))
    registry._conn.commit()

    rows = {row["id"]: row for row in registry.sources_with_secret()}
    assert set(rows) == {"MBP", SOURCE_ID_NAS}
    assert rows[SOURCE_ID_NAS]["secret_hash"] == "h" * 64
    assert set(rows[SOURCE_ID_NAS]) == {"id", "connector", "owner", "secret_hash"}


def test_set_secret_hash_replaces_the_hash_of_a_hub_source_in_any_case(registry) -> None:
    """set_secret_hash stores the new hash on the row whatever its kind, and matches the id case-insensitively."""
    assert registry.set_secret_hash("nas", "a" * 64) is True

    rows = {row["id"]: row for row in registry.sources_with_secret()}
    assert rows[SOURCE_ID_NAS]["secret_hash"] == "a" * 64

    # The stored id is 'nas'; a differently cased id still finds the row.
    assert registry.set_secret_hash("NAS", "b" * 64) is True
    assert registry.source_by_id("nas")["secret_hash"] == "b" * 64


def test_set_secret_hash_for_an_unknown_id_changes_nothing(registry) -> None:
    assert registry.set_secret_hash("no-such-source", "c" * 64) is False
    assert registry.sources_with_secret() == []
