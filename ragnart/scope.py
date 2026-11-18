"""
Scope resolution for the search API's `scope` query parameter (RESEARCH C1, design §8.8).

The grammar, one term per `scope` value. Repeated terms are unioned.

    all                       every ragnart project plus global items (no project filter)
    <source>:<path>           the ragnart project at exactly this path, in this source
    <source>:<path>/          the ragnart project at this path (if any) and every
                              ragnart project below it, in this source
    tag:<key>=<value>         every ragnart project carrying that tag

A path term names its source first. The text before the first ':' is the source id
(for example `nas`), and the rest is the path. A path term with no ':' is refused: a
bare path does not say which store it is in. The source must be registered, and its
connector must be able to hold ragnart projects (only `nas` can, in v0.3).

`name:` terms are reserved for v0.4 and are refused for now.

`global` is never part of a selection. It is only included by `all`, and the echo
reports it through the `global` flag. Path terms never match the `global` row,
because that row has no path.

resolve_scope() returns the Qdrant filter to apply (None means unrestricted) and an
echo dict that the API returns so the caller can see what was actually searched.
The registry is needed for every term, `all` included, because the echo of `all`
lists every project. A caller whose registry is missing therefore gets no scope at all.
"""

from __future__ import annotations

from qdrant_client.models import FieldCondition, Filter, MatchAny

from .registry import RegistryDB
from .schema import FIELD_PROJECT_ID, GLOBAL_PROJECT_ID, SOURCES_HOLDING_PROJECTS

# The single term that selects everything (global items plus all ragnart projects).
ALL_TERM = "all"

# Prefix of a tag term. The text after it must be `<key>=<value>`.
TAG_PREFIX = "tag:"

# Prefix of a name term. Reserved: name terms are not built until v0.4.
NAME_PREFIX = "name:"


class ScopeError(ValueError):
    """
    A scope term is empty, unknown, or names no ragnart project.

    The API maps this to HTTP 422 and passes the message through to the caller.
    """


def resolve_scope(scope_terms: list[str], registry_db: RegistryDB) -> tuple[Filter | None, dict]:
    """
    Turn a list of scope terms into a Qdrant filter and an echo of the selection.

    Every term is validated, even when `all` is present, so a bad term always
    fails the request.

    Args:
        scope_terms: The raw `scope` values, in request order. Must not be empty.
        registry_db: The RegistryDB holding the projects, the tags and the sources.

    Returns:
        (filter, echo). `filter` is None when `all` was one of the terms, which
        means no project_id restriction. Otherwise it restricts project_id to the
        matched ids. `echo` is {"all": bool, "global": bool, "projects": [...]},
        where each project is {"id", "path", "name", "source_id"}.

    Raises:
        ScopeError: if there are no terms, a term is empty, a name term is used, a
                    path term is not qualified by a usable source, or a path or tag
                    term matches no ragnart project.
    """
    if not scope_terms:
        raise ScopeError(
            "scope is required: use 'all', '<source>:<path>', '<source>:<path>/', or 'tag:<key>=<value>'"
        )

    # Every ragnart project, in a stable order. The reserved global row is excluded.
    projects = [p for p in registry_db.list_projects() if p["id"] != GLOBAL_PROJECT_ID]
    source_ids = {source["id"] for source in registry_db.list_sources()}

    select_all = False
    matched_ids: set[str] = set()
    for term in scope_terms:
        if term.strip() == "":
            raise ScopeError(
                "empty scope term: use 'all', '<source>:<path>', '<source>:<path>/', or 'tag:<key>=<value>'"
            )
        if term == ALL_TERM:
            select_all = True
        elif term.startswith(TAG_PREFIX):
            matched_ids |= _ids_for_tag_term(term, registry_db)
        elif term.startswith(NAME_PREFIX):
            raise ScopeError("name: terms are not supported yet")
        else:
            matched_ids |= _ids_for_path_term(term, projects, source_ids)

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


def _ids_for_path_term(term: str, projects: list[dict], source_ids: set[str]) -> set[str]:
    """
    Return the ids of the projects a `<source>:<path>` or `<source>:<path>/` term selects.

    The text before the first ':' is the source. It must be a registered source whose
    connector can hold ragnart projects. Only projects whose home source is that source
    are then matched, by exact path or by subtree (for a trailing slash).
    """
    source, separator, path = term.partition(":")
    if not separator:
        raise ScopeError(f"{term}: a path term must name its source, e.g. nas:<path>")
    if source not in source_ids:
        raise ScopeError(f"{term}: unknown source '{source}'")
    # The capability check reads the schema set, not the connector table (see SOURCES_HOLDING_PROJECTS).
    if source not in SOURCES_HOLDING_PROJECTS:
        raise ScopeError(f"{term}: {source} cannot hold ragnart projects")

    # Only the projects homed in this source can match this term.
    homed = [p for p in projects if p["home_source_id"] == source]
    if path.endswith("/"):
        return _ids_for_subtree_term(term, path, homed)
    return _ids_for_exact_path_term(term, path, homed)


def _ids_for_exact_path_term(term: str, path: str, projects: list[dict]) -> set[str]:
    """Return the id of the single ragnart project whose path equals `path` exactly."""
    ids = {p["id"] for p in projects if p["path"] == path}
    if not ids:
        raise ScopeError(f"{term}: no ragnart project at this path")
    return ids


def _ids_for_subtree_term(term: str, path: str, projects: list[dict]) -> set[str]:
    """
    Return the ids of the projects at or below the path named by a trailing-slash term.

    `path` is the path after the source, with its trailing slash. The base path (without
    the slash) may or may not be a project itself. Its descendants are the projects whose
    path starts with `base + "/"`. Zero matches is an error, so a typo does not silently
    search nothing.
    """
    base = path.rstrip("/")
    ids = {
        p["id"]
        for p in projects
        if p["path"] == base or p["path"].startswith(base + "/")
    }
    if not ids:
        raise ScopeError(f"{term}: no ragnart project at or below this path")
    return ids


def _ids_for_tag_term(term: str, registry_db: RegistryDB) -> set[str]:
    """Return the ids of every ragnart project carrying the tag named by `tag:<key>=<value>`."""
    body = term[len(TAG_PREFIX):]
    key, separator, value = body.partition("=")
    if not separator or not key or not value:
        raise ScopeError(f"{term}: tag scope must be tag:<key>=<value> with a non-empty key and value")
    ids = registry_db.project_ids_by_tag(key, value)
    if not ids:
        raise ScopeError(f"{term}: no ragnart project carries this tag")
    return set(ids)


def _echo_project(project: dict) -> dict:
    """Return the public view of one project for the scope echo, with the source it is homed in."""
    return {
        "id": project["id"],
        "path": project["path"],
        "name": project["name"],
        "source_id": project["home_source_id"],
    }
