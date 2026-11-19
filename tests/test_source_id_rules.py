"""
Tests for the source-id rule (ragnart/schema.py validate_source_id) and for the
case-insensitive source matching in scope terms (ragnart/scope.py).

The registry is a real RegistryDB in tmp_path with two ragnart projects homed in `nas`.
No Qdrant, no NAS, no live database. The case-insensitive uniqueness of registered ids is
tested with the registry methods in tests/test_registry.py.
"""

from __future__ import annotations

import pytest

from ragnart.registry import RegistryDB
from ragnart.schema import SOURCE_ID_NAS, validate_source_id
from ragnart.scope import ScopeError, resolve_scope

ROOT_PATH = "/mnt/nfs/x/proj"
CHILD_PATH = "/mnt/nfs/x/proj/sub"


@pytest.fixture
def registry(tmp_path):
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    db.upsert_project("p-root", ROOT_PATH, "Root", "tester", SOURCE_ID_NAS)
    db.upsert_project("p-child", CHILD_PATH, "Child", "tester", SOURCE_ID_NAS)
    yield db
    db.close()


# ---------------------------------------------------------------------------
# validate_source_id: the shape and the reserved words
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "source_id",
    ["nas", "claude-linux-shared-nfs", "opencode-k11", "MBP", "claude-MBP", "a", "A1-b2"],
)
def test_valid_ids_are_accepted(source_id: str) -> None:
    validate_source_id(source_id)  # does not raise


@pytest.mark.parametrize(
    "source_id",
    [
        "",                # empty
        "-leading",        # must start with a letter or digit
        "has:colon",       # ':' separates the source from the path in a scope term
        "has_underscore",  # '_' is not allowed
        "has space",
        "dotted.id",
        "trailing\n",      # fullmatch refuses a trailing newline
        "TAG",             # reserved, any case
        "Global",          # reserved, any case
        "all",
        "name",
        "web",
        "none",
        "project",
    ],
)
def test_invalid_or_reserved_ids_are_refused(source_id: str) -> None:
    with pytest.raises(ValueError):
        validate_source_id(source_id)


# ---------------------------------------------------------------------------
# scope terms: the source prefix is matched case-insensitively
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("term", ["nas:/mnt/nfs/x/proj", "NAS:/mnt/nfs/x/proj", "Nas:/mnt/nfs/x/proj"])
def test_source_prefix_matches_in_any_case_and_echoes_the_stored_id(registry, term: str) -> None:
    query_filter, echo = resolve_scope([term], registry)

    assert [p["id"] for p in echo["projects"]] == ["p-root"]
    # The echo shows the stored id, whatever the case the operator typed.
    assert echo["projects"][0]["source_id"] == "nas"
    assert query_filter.must[0].match.any == ["p-root"]


def test_subtree_term_matches_in_any_case(registry) -> None:
    _, echo = resolve_scope(["NAS:/mnt/nfs/x/proj/"], registry)

    assert {p["id"] for p in echo["projects"]} == {"p-root", "p-child"}
    assert {p["source_id"] for p in echo["projects"]} == {"nas"}


def test_unknown_source_is_refused_in_any_case(registry) -> None:
    with pytest.raises(ScopeError, match="unknown source 'MBP'"):
        resolve_scope(["MBP:/mnt/nfs/x/proj"], registry)


def test_a_source_that_cannot_hold_projects_is_refused_in_any_case(registry) -> None:
    with pytest.raises(ScopeError, match="cannot hold ragnart projects"):
        resolve_scope(["CLAUDE-LINUX-SHARED-NFS:/mnt/nfs/x/proj"], registry)
