"""
Scope resolution for the search API's `scope` query parameter (RESEARCH C1).

The grammar, one term per `scope` value. Repeated terms are unioned.

    all                 every ragnart project plus global items (no project filter)
    <path>              the ragnart project at exactly this path
    <path>/             the ragnart project at this path (if any) and every ragnart
                        project below it
    tag:<key>=<value>   every ragnart project carrying that tag

`global` is never part of a selection. It is only included by `all`, and the echo
reports it through the `global` flag. Path terms never match the `global` row,
because that row has no path.

resolve_scope() returns the Qdrant filter to apply (None means unrestricted) and an
echo dict that the API returns so the caller can see what was actually searched.
"""

from __future__ import annotations

from qdrant_client.models import FieldCondition, Filter, MatchAny

from .schema import FIELD_PROJECT_ID, GLOBAL_PROJECT_ID
from .state import StateDB

# The single term that selects everything (global items plus all ragnart projects).
ALL_TERM = "all"

# Prefix of a tag term. The text after it must be `<key>=<value>`.
TAG_PREFIX = "tag:"


class ScopeError(ValueError):
    """
    A scope term is empty, unknown, or names no ragnart project.

    The API maps this to HTTP 422 and passes the message through to the caller.
    """


def resolve_scope(scope_terms: list[str], state_db: StateDB) -> tuple[Filter | None, dict]:
    """
    Turn a list of scope terms into a Qdrant filter and an echo of the selection.

    Every term is validated, even when `all` is present, so a bad term always
    fails the request.

    Args:
        scope_terms: The raw `scope` values, in request order. Must not be empty.
        state_db:    The StateDB holding the projects and tags tables.

    Returns:
        (filter, echo). `filter` is None when `all` was one of the terms, which
        means no project_id restriction. Otherwise it restricts project_id to the
        matched ids. `echo` is {"all": bool, "global": bool, "projects": [...]},
        where each project is {"id", "path", "name"}.

    Raises:
        ScopeError: if there are no terms, a term is empty or unknown, or a path
                    or tag term matches no ragnart project.
    """
    if not scope_terms:
        raise ScopeError("scope is required: use 'all', a project path, '<path>/', or 'tag:<key>=<value>'")

    # Every ragnart project, in a stable order. The reserved global row is excluded.
    projects = [p for p in state_db.list_projects() if p["id"] != GLOBAL_PROJECT_ID]

    select_all = False
    matched_ids: set[str] = set()
    for term in scope_terms:
        if term.strip() == "":
            raise ScopeError("empty scope term: use 'all', a project path, '<path>/', or 'tag:<key>=<value>'")
        if term == ALL_TERM:
            select_all = True
        elif term.startswith(TAG_PREFIX):
            matched_ids |= _ids_for_tag_term(term, state_db)
        elif term.endswith("/"):
            matched_ids |= _ids_for_subtree_term(term, projects)
        else:
            matched_ids |= _ids_for_exact_path_term(term, projects)

    if select_all:
        # `all` searches everything, so there is no project restriction.
        return None, {
            "all": True,
            "global": True,
            "projects": [_echo_project(p) for p in projects],
        }

    selected = [p for p in projects if p["id"] in matched_ids]
    query_filter = Filter(
        must=[FieldCondition(key=FIELD_PROJECT_ID, match=MatchAny(any=sorted(matched_ids)))]
    )
    return query_filter, {
        "all": False,
        "global": False,
        "projects": [_echo_project(p) for p in selected],
    }


def _ids_for_exact_path_term(term: str, projects: list[dict]) -> set[str]:
    """Return the id of the single ragnart project whose path equals `term` exactly."""
    ids = {p["id"] for p in projects if p["path"] == term}
    if not ids:
        raise ScopeError(f"{term}: no ragnart project at this path")
    return ids


def _ids_for_subtree_term(term: str, projects: list[dict]) -> set[str]:
    """
    Return the ids of the projects at or below the path named by a trailing-slash term.

    The base path (term without its trailing slash) may or may not be a project
    itself. Its descendants are the projects whose path starts with `base + "/"`.
    Zero matches is an error, so a typo does not silently search nothing.
    """
    base = term.rstrip("/")
    ids = {
        p["id"]
        for p in projects
        if p["path"] == base or p["path"].startswith(base + "/")
    }
    if not ids:
        raise ScopeError(f"{term}: no ragnart project at or below this path")
    return ids


def _ids_for_tag_term(term: str, state_db: StateDB) -> set[str]:
    """Return the ids of every ragnart project carrying the tag named by `tag:<key>=<value>`."""
    body = term[len(TAG_PREFIX):]
    key, separator, value = body.partition("=")
    if not separator or not key or not value:
        raise ScopeError(f"{term}: tag scope must be tag:<key>=<value> with a non-empty key and value")
    ids = state_db.project_ids_by_tag(key, value)
    if not ids:
        raise ScopeError(f"{term}: no ragnart project carries this tag")
    return set(ids)


def _echo_project(project: dict) -> dict:
    """Return the public view of one project for the scope echo."""
    return {"id": project["id"], "path": project["path"], "name": project["name"]}
