"""
Tests for scope resolution (ragnart/scope.py) and the two StateDB helpers it relies on.

Every test runs on a real StateDB in tmp_path with a small fixed set of ragnart
projects. No mocks, no Qdrant, no NFS. The projects are seeded through the public
upsert_project / replace_project_tags calls, the same ones the marker sync uses.

Seeded layout (the reserved 'global' row is present from the migrations):

    /mnt/nfs/x/proj          id p-root   tags kind=customer
    /mnt/nfs/x/proj/sub      id p-child  tags kind=internal
    /mnt/nfs/y/other         id p-other  tags kind=customer
"""

from __future__ import annotations

import pytest

from ragnart.schema import FIELD_PROJECT_ID, GLOBAL_PROJECT_ID
from ragnart.scope import ScopeError, resolve_scope
from ragnart.state import StateDB

OWNER = "tester"
ROOT_PATH = "/mnt/nfs/x/proj"
CHILD_PATH = "/mnt/nfs/x/proj/sub"
OTHER_PATH = "/mnt/nfs/y/other"


def _new_db(tmp_path) -> StateDB:
    """Return a fresh StateDB with the three test projects and their tags seeded."""
    state_db = StateDB(str(tmp_path / "rag_state.db"))
    seed = [
        ("p-root", ROOT_PATH, "Root", {"kind": "customer"}),
        ("p-child", CHILD_PATH, "Child", {"kind": "internal"}),
        ("p-other", OTHER_PATH, "Other", {"kind": "customer"}),
    ]
    for project_id, path, name, tags in seed:
        state_db.upsert_project(
            project_id, path, name, parent=None, owner=OWNER, inherit_access=True, mode="crawl",
        )
        state_db.replace_project_tags(project_id, tags)
    return state_db


def _selected_ids(echo: dict) -> set[str]:
    """Return the set of project ids in a scope echo."""
    return {p["id"] for p in echo["projects"]}


def _filter_ids(query_filter) -> list[str]:
    """Return the project_id values a non-None scope filter restricts to."""
    assert len(query_filter.must) == 1
    condition = query_filter.must[0]
    assert condition.key == FIELD_PROJECT_ID
    return list(condition.match.any)


# ----------------------------------------------------------------------
# all

def test_all_returns_no_filter_and_echoes_every_project_plus_global(tmp_path):
    state_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope(["all"], state_db)

    assert query_filter is None
    assert echo["all"] is True
    assert echo["global"] is True
    assert _selected_ids(echo) == {"p-root", "p-child", "p-other"}
    # The echo carries the public view only.
    root = next(p for p in echo["projects"] if p["id"] == "p-root")
    assert root == {"id": "p-root", "path": ROOT_PATH, "name": "Root"}


def test_all_with_other_terms_still_returns_no_filter(tmp_path):
    state_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope(["all", f"{ROOT_PATH}"], state_db)

    assert query_filter is None
    assert echo["all"] is True


# ----------------------------------------------------------------------
# exact path

def test_exact_path_term_matches_one_project(tmp_path):
    state_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope([ROOT_PATH], state_db)

    assert _filter_ids(query_filter) == ["p-root"]
    assert echo == {
        "all": False,
        "global": False,
        "projects": [{"id": "p-root", "path": ROOT_PATH, "name": "Root"}],
    }


def test_exact_path_term_does_not_match_descendants(tmp_path):
    state_db = _new_db(tmp_path)

    _, echo = resolve_scope([ROOT_PATH], state_db)

    assert "p-child" not in _selected_ids(echo)


# ----------------------------------------------------------------------
# trailing slash

def test_trailing_slash_term_matches_project_and_descendants(tmp_path):
    state_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope([f"{ROOT_PATH}/"], state_db)

    assert sorted(_filter_ids(query_filter)) == ["p-child", "p-root"]
    assert _selected_ids(echo) == {"p-root", "p-child"}


def test_trailing_slash_term_on_non_project_base_returns_projects_below(tmp_path):
    # /mnt/nfs/x is not itself a ragnart project, but two projects sit below it.
    state_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope(["/mnt/nfs/x/"], state_db)

    assert sorted(_filter_ids(query_filter)) == ["p-child", "p-root"]
    assert _selected_ids(echo) == {"p-root", "p-child"}


def test_trailing_slash_does_not_match_sibling_with_shared_prefix(tmp_path):
    # /mnt/nfs/x/proj/ must not match a sibling named /mnt/nfs/x/proj-extra.
    state_db = _new_db(tmp_path)
    state_db.upsert_project(
        "p-sibling", "/mnt/nfs/x/proj-extra", "Sibling",
        parent=None, owner=OWNER, inherit_access=True, mode="crawl",
    )

    _, echo = resolve_scope([f"{ROOT_PATH}/"], state_db)

    assert "p-sibling" not in _selected_ids(echo)


def test_trailing_slash_term_with_no_projects_below_raises(tmp_path):
    state_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match="no ragnart project at or below"):
        resolve_scope(["/mnt/nfs/nowhere/"], state_db)


# ----------------------------------------------------------------------
# tag

def test_tag_term_matches_by_project_tags(tmp_path):
    state_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope(["tag:kind=customer"], state_db)

    assert sorted(_filter_ids(query_filter)) == ["p-other", "p-root"]
    assert _selected_ids(echo) == {"p-root", "p-other"}
    assert echo["global"] is False


def test_tag_term_with_no_matching_project_raises(tmp_path):
    state_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match="carries this tag"):
        resolve_scope(["tag:kind=vendor"], state_db)


@pytest.mark.parametrize("bad_term", ["tag:kind", "tag:=customer", "tag:kind=", "tag:"])
def test_malformed_tag_term_raises(tmp_path, bad_term):
    state_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match="tag:<key>=<value>"):
        resolve_scope([bad_term], state_db)


# ----------------------------------------------------------------------
# union, errors, global

def test_repeated_terms_union(tmp_path):
    state_db = _new_db(tmp_path)

    query_filter, echo = resolve_scope([ROOT_PATH, "tag:kind=customer"], state_db)

    # p-root is matched by both terms and must appear once.
    assert sorted(_filter_ids(query_filter)) == ["p-other", "p-root"]
    assert sorted(p["id"] for p in echo["projects"]) == ["p-other", "p-root"]


def test_unknown_path_raises_scope_error(tmp_path):
    state_db = _new_db(tmp_path)

    with pytest.raises(ScopeError, match="no ragnart project at this path"):
        resolve_scope(["/mnt/nfs/x/not-a-project"], state_db)


@pytest.mark.parametrize("bad_terms", [[], [""], ["   "], [ROOT_PATH, ""]])
def test_empty_or_missing_terms_raise(tmp_path, bad_terms):
    state_db = _new_db(tmp_path)

    with pytest.raises(ScopeError):
        resolve_scope(bad_terms, state_db)


def test_selection_terms_never_include_global(tmp_path):
    state_db = _new_db(tmp_path)

    selections = [
        [ROOT_PATH],
        [f"{ROOT_PATH}/"],
        ["/mnt/nfs/x/"],
        ["tag:kind=customer"],
        [ROOT_PATH, "tag:kind=customer"],
    ]
    for terms in selections:
        query_filter, echo = resolve_scope(terms, state_db)
        assert GLOBAL_PROJECT_ID not in _filter_ids(query_filter), terms
        assert GLOBAL_PROJECT_ID not in _selected_ids(echo), terms
        assert echo["global"] is False, terms
        assert echo["all"] is False, terms


# ----------------------------------------------------------------------
# StateDB helpers used by scope

def test_project_ids_by_tag_matches_exact_key_and_value(tmp_path):
    state_db = _new_db(tmp_path)

    assert state_db.project_ids_by_tag("kind", "customer") == ["p-other", "p-root"]
    assert state_db.project_ids_by_tag("kind", "internal") == ["p-child"]
    # A value that is a prefix of an existing value must not match.
    assert state_db.project_ids_by_tag("kind", "custom") == []
    assert state_db.project_ids_by_tag("other", "customer") == []


def test_item_counts_by_project_counts_only_completed_rows(tmp_path):
    state_db = _new_db(tmp_path)
    completed_a = "/mnt/nfs/x/proj/a.md"
    completed_b = "/mnt/nfs/x/proj/b.md"
    pending_c = "/mnt/nfs/x/proj/c.md"
    for file_path in (completed_a, completed_b, pending_c):
        state_db.discover_or_update(file_path, OWNER, 1000.0, origin_path=file_path)
    state_db.mark_completed(completed_a, chunk_count=2, completed_at="2026-01-01T00:00:00+00:00")
    state_db.mark_completed(completed_b, chunk_count=2, completed_at="2026-01-01T00:00:00+00:00")
    state_db.set_project_id(completed_a, "p-root")
    state_db.set_project_id(completed_b, "p-root")
    state_db.set_project_id(pending_c, "p-root")

    counts = state_db.item_counts_by_project()

    # pending_c is not completed, so it is not counted.
    assert counts == {"p-root": 2}


def test_item_counts_by_project_empty_when_nothing_completed(tmp_path):
    state_db = _new_db(tmp_path)

    assert state_db.item_counts_by_project() == {}
