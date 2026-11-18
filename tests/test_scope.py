"""
Tests for scope resolution (ragnart/scope.py), grammar as in docs/RAGNART-design.md §8.8.

Every test runs on a real RegistryDB in tmp_path with a small fixed set of ragnart
projects. No mocks, no Qdrant, no NAS. The projects are seeded through the public
upsert_project / replace_project_tags calls, the same ones the marker sync uses.

Seeded layout (the reserved 'global' row is present from the migrations). Every
project is homed in the `nas` source:

    /mnt/nfs/x/proj          id p-root   tags kind=customer
    /mnt/nfs/x/proj/sub      id p-child  tags kind=internal
    /mnt/nfs/y/other         id p-other  tags kind=customer
"""

from __future__ import annotations

import pytest

from ragnart.registry import RegistryDB
from ragnart.schema import (
    FIELD_PROJECT_ID,
    GLOBAL_PROJECT_ID,
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
)
from ragnart.scope import ScopeError, resolve_scope

OWNER = "tester"
ROOT_PATH = "/mnt/nfs/x/proj"
CHILD_PATH = "/mnt/nfs/x/proj/sub"
OTHER_PATH = "/mnt/nfs/y/other"


def _new_db(tmp_path) -> RegistryDB:
    """Return a fresh RegistryDB with the three test projects and their tags seeded."""
    registry_db = RegistryDB(str(tmp_path / "hub_registry.db"))
    seed = [
        ("p-root", ROOT_PATH, "Root", {"kind": "customer"}),
        ("p-child", CHILD_PATH, "Child", {"kind": "internal"}),
        ("p-other", OTHER_PATH, "Other", {"kind": "customer"}),
    ]
    for project_id, path, name, tags in seed:
        registry_db.upsert_project(project_id, path, name, OWNER, SOURCE_ID_NAS)
        registry_db.replace_project_tags(project_id, tags)
    return registry_db


def _selected_ids(echo: dict) -> set[str]:
    """Return the set of project ids in a scope echo."""
    return {p["id"] for p in echo["projects"]}


def _filter_ids(query_filter) -> list[str]:
    """Return the project_id values a non-None scope filter restricts to."""
    assert len(query_filter.must) == 1
    condition = query_filter.must[0]
    assert condition.key == FIELD_PROJECT_ID
    return list(condition.match.any)


def _nas(path: str) -> str:
    """Return a path term qualified by the nas source, as a caller sends it."""
    return f"{SOURCE_ID_NAS}:{path}"


# ----------------------------------------------------------------------
# all

def test_all_returns_no_filter_and_echoes_every_project_plus_global(tmp_path):
    registry_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope(["all"], registry_db)

    assert query_filter is None
    assert echo["all"] is True
    assert echo["global"] is True
    assert _selected_ids(echo) == {"p-root", "p-child", "p-other"}
    # The echo carries the public view only, with the source each project is homed in.
    root = next(p for p in echo["projects"] if p["id"] == "p-root")
    assert root == {"id": "p-root", "path": ROOT_PATH, "name": "Root", "source_id": SOURCE_ID_NAS}


def test_all_with_other_terms_still_returns_no_filter(tmp_path):
    registry_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope(["all", _nas(ROOT_PATH)], registry_db)

    assert query_filter is None
    assert echo["all"] is True


def test_all_still_validates_every_other_term(tmp_path):
    # A bare path next to `all` is still refused: every term is validated.
    registry_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match="must name its source"):
        resolve_scope(["all", ROOT_PATH], registry_db)


# ----------------------------------------------------------------------
# <source>:<path> exact

def test_exact_path_term_matches_one_project(tmp_path):
    registry_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope([_nas(ROOT_PATH)], registry_db)

    assert _filter_ids(query_filter) == ["p-root"]
    assert echo == {
        "all": False,
        "global": False,
        "projects": [{"id": "p-root", "path": ROOT_PATH, "name": "Root", "source_id": SOURCE_ID_NAS}],
    }


def test_exact_path_term_does_not_match_descendants(tmp_path):
    registry_db = _new_db(tmp_path)

    _, echo = resolve_scope([_nas(ROOT_PATH)], registry_db)

    assert "p-child" not in _selected_ids(echo)


def test_unknown_path_raises_scope_error(tmp_path):
    registry_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match="no ragnart project at this path"):
        resolve_scope([_nas("/mnt/nfs/x/not-a-project")], registry_db)


# ----------------------------------------------------------------------
# <source>:<path>/ subtree

def test_trailing_slash_term_matches_project_and_descendants(tmp_path):
    registry_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope([_nas(f"{ROOT_PATH}/")], registry_db)

    assert sorted(_filter_ids(query_filter)) == ["p-child", "p-root"]
    assert _selected_ids(echo) == {"p-root", "p-child"}


def test_trailing_slash_term_on_non_project_base_returns_projects_below(tmp_path):
    # /mnt/nfs/x is not itself a ragnart project, but two projects sit below it.
    registry_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope([_nas("/mnt/nfs/x/")], registry_db)

    assert sorted(_filter_ids(query_filter)) == ["p-child", "p-root"]
    assert _selected_ids(echo) == {"p-root", "p-child"}


def test_trailing_slash_does_not_match_sibling_with_shared_prefix(tmp_path):
    # /mnt/nfs/x/proj/ must not match a sibling named /mnt/nfs/x/proj-extra.
    registry_db = _new_db(tmp_path)
    registry_db.upsert_project("p-sibling", "/mnt/nfs/x/proj-extra", "Sibling", OWNER, SOURCE_ID_NAS)

    _, echo = resolve_scope([_nas(f"{ROOT_PATH}/")], registry_db)

    assert "p-sibling" not in _selected_ids(echo)


def test_trailing_slash_term_with_no_projects_below_raises(tmp_path):
    registry_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match="no ragnart project at or below"):
        resolve_scope([_nas("/mnt/nfs/nowhere/")], registry_db)


# ----------------------------------------------------------------------
# Source qualification: bare paths, unknown sources, sources that hold no projects

@pytest.mark.parametrize("bare_term", [ROOT_PATH, f"{ROOT_PATH}/", "/mnt/nfs/x/"])
def test_bare_path_term_is_refused_as_unqualified(tmp_path, bare_term):
    registry_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match="a path term must name its source, e.g. nas:<path>"):
        resolve_scope([bare_term], registry_db)


def test_unknown_source_is_refused(tmp_path):
    registry_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match="unknown source 'nsa'"):
        resolve_scope([f"nsa:{ROOT_PATH}"], registry_db)


@pytest.mark.parametrize("source_id", [SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_OPENCODE_K11])
def test_source_that_cannot_hold_projects_is_refused(tmp_path, source_id):
    registry_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match=f"{source_id} cannot hold ragnart projects"):
        resolve_scope([f"{source_id}:{ROOT_PATH}"], registry_db)


def test_path_term_only_matches_projects_homed_in_its_source(tmp_path):
    # A project homed in another source is not matched by a nas: term, even at the same path.
    registry_db = _new_db(tmp_path)
    registry_db.upsert_project(
        "p-elsewhere", "/mnt/nfs/x/elsewhere", "Elsewhere", OWNER, SOURCE_ID_OPENCODE_K11,
    )

    with pytest.raises(ScopeError, match="no ragnart project at this path"):
        resolve_scope([_nas("/mnt/nfs/x/elsewhere")], registry_db)


def test_name_term_is_refused_until_v04(tmp_path):
    registry_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match="name: terms are not supported yet"):
        resolve_scope(["name:RAGNART"], registry_db)


# ----------------------------------------------------------------------
# tag

def test_tag_term_matches_by_project_tags(tmp_path):
    registry_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope(["tag:kind=customer"], registry_db)

    assert sorted(_filter_ids(query_filter)) == ["p-other", "p-root"]
    assert _selected_ids(echo) == {"p-root", "p-other"}
    assert echo["global"] is False


def test_tag_term_with_no_matching_project_raises(tmp_path):
    registry_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match="carries this tag"):
        resolve_scope(["tag:kind=vendor"], registry_db)


@pytest.mark.parametrize("bad_term", ["tag:kind", "tag:=customer", "tag:kind=", "tag:"])
def test_malformed_tag_term_raises(tmp_path, bad_term):
    registry_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match="tag:<key>=<value>"):
        resolve_scope([bad_term], registry_db)


# ----------------------------------------------------------------------
# union, errors, global

def test_repeated_terms_union(tmp_path):
    registry_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope([_nas(ROOT_PATH), "tag:kind=customer"], registry_db)

    # p-root is matched by both terms and must appear once.
    assert sorted(_filter_ids(query_filter)) == ["p-other", "p-root"]
    assert sorted(p["id"] for p in echo["projects"]) == ["p-other", "p-root"]


@pytest.mark.parametrize("bad_terms", [[], [""], ["   "], [_nas(ROOT_PATH), ""]])
def test_empty_or_missing_terms_raise(tmp_path, bad_terms):
    registry_db = _new_db(tmp_path)

    with pytest.raises(ScopeError):
        resolve_scope(bad_terms, registry_db)


def test_selection_terms_never_include_global(tmp_path):
    registry_db = _new_db(tmp_path)

    selections = [
        [_nas(ROOT_PATH)],
        [_nas(f"{ROOT_PATH}/")],
        [_nas("/mnt/nfs/x/")],
        ["tag:kind=customer"],
        [_nas(ROOT_PATH), "tag:kind=customer"],
    ]
    for terms in selections:
        query_filter, echo = resolve_scope(terms, registry_db)
        assert GLOBAL_PROJECT_ID not in _filter_ids(query_filter), terms
        assert GLOBAL_PROJECT_ID not in _selected_ids(echo), terms
        assert echo["global"] is False, terms
        assert echo["all"] is False, terms


# ----------------------------------------------------------------------
# RegistryDB helper used by scope

def test_project_ids_by_tag_matches_exact_key_and_value(tmp_path):
    registry_db = _new_db(tmp_path)

    assert registry_db.project_ids_by_tag("kind", "customer") == ["p-other", "p-root"]
    assert registry_db.project_ids_by_tag("kind", "internal") == ["p-child"]
    # A value that is a prefix of an existing value must not match.
    assert registry_db.project_ids_by_tag("kind", "custom") == []
    assert registry_db.project_ids_by_tag("other", "customer") == []
