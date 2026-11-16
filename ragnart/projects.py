"""
Ragnart project markers (`.ragnart/project.toml`) and path-based project matching.

A directory containing `.ragnart/project.toml` is a ragnart project. Every item
is attached to the closest enclosing ragnart project by path, or to the `global`
project when no ragnart project encloses it.

Path matching is verbatim string comparison. No ``realpath`` call is made and no
normalisation is applied beyond stripping a trailing ``/`` from a project path.
A session launched through a symlinked path therefore does not attach.
"""

from __future__ import annotations

import logging
import tomllib

logger = logging.getLogger(__name__)

# Keys in `project.toml` with a fixed meaning. Every other key becomes a tag.
RESERVED_PROJECT_KEYS = frozenset(
    {
        "name",
        "id",
        "inherit_access",
        "mode",
        "scope",
        "grounding",
        "capture",
        "inbox_expiry_days",
    }
)


def parse_project_toml(toml_path: str) -> dict | None:
    """
    Parse a `project.toml` file into a project description.

    Returns a dict with keys ``name``, ``id``, ``inherit_access`` (default True),
    ``mode`` (default None) and ``tags`` (every non-reserved key, value as str).

    Returns None and logs an ERROR naming the path and the reason when the file
    is not valid TOML, or when ``name`` or ``id`` is missing or not a string.
    A directory with an invalid marker stays outside the project model, the same
    way an invalid `.ragignore` is handled.
    """
    # tomllib requires a binary file handle.
    with open(toml_path, "rb") as fh:
        try:
            data = tomllib.load(fh)
        except tomllib.TOMLDecodeError as exc:
            logger.error("project marker %s is not valid TOML: %s", toml_path, exc)
            return None

    for required in ("name", "id"):
        if not isinstance(data.get(required), str):
            logger.error(
                "project marker %s: '%s' must be present and a string",
                toml_path,
                required,
            )
            return None

    # Non-reserved keys are free-form tags for the project.
    tags = {
        key: str(value)
        for key, value in data.items()
        if key not in RESERVED_PROJECT_KEYS
    }

    return {
        "name": data["name"],
        "id": data["id"],
        "inherit_access": data.get("inherit_access", True),
        "mode": data.get("mode"),
        "tags": tags,
    }


def _is_self_or_descendant(path: str, project_path: str) -> bool:
    """
    True when `path` equals `project_path` or lies below it as a directory.

    The trailing ``/`` is stripped from `project_path` before the prefix test, so
    the root project ``/`` still matches every absolute path.
    """
    return path == project_path or path.startswith(project_path.rstrip("/") + "/")


def resolve_parent(path: str, other_paths: list[str]) -> str | None:
    """
    Return the longest entry of `other_paths` that is a proper ancestor of `path`.

    An entry equal to `path` is not a parent of it. Returns None when no entry
    encloses `path`.
    """
    best: str | None = None
    for other in other_paths:
        if other == path:
            continue
        if not _is_self_or_descendant(path, other):
            continue
        # Keep the longest enclosing path, the closest ancestor.
        if best is None or len(other) > len(best):
            best = other
    return best


def project_id_for_path(
    path: str, projects: list[tuple[str, str]], global_id: str
) -> str:
    """
    Return the id of the project that attaches `path`, or `global_id`.

    `projects` is a list of ``(id, path)`` pairs. A project attaches `path` when
    the project path equals `path` or encloses it. When several projects match,
    the one with the longest path wins.
    """
    best_id: str | None = None
    best_len = -1
    for project_id, project_path in projects:
        if not _is_self_or_descendant(path, project_path):
            continue
        # Longest project path wins, so the closest enclosing project is chosen.
        if len(project_path) > best_len:
            best_id = project_id
            best_len = len(project_path)
    return best_id if best_id is not None else global_id
