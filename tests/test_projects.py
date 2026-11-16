"""
Tests for ragnart/projects.py: project marker parsing and path matching.
"""

from __future__ import annotations

import logging

from ragnart.projects import (
    RESERVED_PROJECT_KEYS,
    parse_project_toml,
    project_id_for_path,
    resolve_parent,
)


def _write_toml(tmp_path, text: str) -> str:
    """Write `text` to a project.toml under tmp_path and return its path."""
    marker = tmp_path / "project.toml"
    marker.write_text(text, encoding="utf-8")
    return str(marker)


def test_parse_valid_toml_returns_name_id_and_tags(tmp_path):
    # Non-reserved keys (here `kind`) must come back as tags.
    path = _write_toml(
        tmp_path,
        'name = "Acme"\nid = "acme"\nkind = "customer"\n',
    )

    parsed = parse_project_toml(path)

    assert parsed is not None
    assert parsed["name"] == "Acme"
    assert parsed["id"] == "acme"
    assert parsed["tags"] == {"kind": "customer"}
    # Defaults when the reserved keys are absent.
    assert parsed["inherit_access"] is True
    assert parsed["mode"] is None


def test_parse_reserved_keys_are_not_tags(tmp_path):
    path = _write_toml(
        tmp_path,
        'name = "RAGNART"\nid = "ragnart"\nmode = "crawl"\n'
        'grounding = "open"\ncapture = false\n',
    )

    parsed = parse_project_toml(path)

    assert parsed is not None
    assert parsed["mode"] == "crawl"
    assert parsed["tags"] == {}
    assert {"name", "id", "mode", "grounding", "capture"} <= RESERVED_PROJECT_KEYS


def test_parse_missing_name_or_id_logs_and_returns_none(tmp_path, caplog):
    path = _write_toml(tmp_path, 'id = "acme"\n')

    with caplog.at_level(logging.ERROR):
        result = parse_project_toml(path)

    assert result is None
    assert any(
        record.levelno == logging.ERROR and path in record.getMessage()
        for record in caplog.records
    )


def test_parse_malformed_toml_logs_and_returns_none(tmp_path, caplog):
    path = _write_toml(tmp_path, 'name = "unterminated\n')

    with caplog.at_level(logging.ERROR):
        result = parse_project_toml(path)

    assert result is None
    assert any(
        record.levelno == logging.ERROR and path in record.getMessage()
        for record in caplog.records
    )


def test_resolve_parent_picks_longest_ancestor():
    other_paths = [
        "/nas",
        "/nas/clients",
        "/nas/clients/acme",
        "/nas/clientsx",  # shares a string prefix but is not an ancestor
    ]

    parent = resolve_parent("/nas/clients/acme/projects/x", other_paths)

    assert parent == "/nas/clients/acme"


def test_resolve_parent_returns_none_for_unrelated_paths():
    other_paths = ["/nas/clients/acme", "/home/florian/other"]

    assert resolve_parent("/nas/clientsx/y", other_paths) is None
    # A path is not its own parent.
    assert resolve_parent("/nas/clients/acme", ["/nas/clients/acme"]) is None


def test_project_id_for_path_matches_exact_and_descendant():
    projects = [("acme", "/nas/clients/acme")]

    # Exact match on the project path itself.
    assert project_id_for_path("/nas/clients/acme", projects, "global") == "acme"
    # Proper descendant.
    assert (
        project_id_for_path("/nas/clients/acme/docs/a.md", projects, "global")
        == "acme"
    )
    # A sibling sharing a string prefix does not match.
    assert (
        project_id_for_path("/nas/clients/acmecorp/a.md", projects, "global")
        == "global"
    )


def test_project_id_for_path_longest_match_wins():
    projects = [
        ("outer", "/nas/clients"),
        ("inner", "/nas/clients/acme"),
    ]

    assert (
        project_id_for_path("/nas/clients/acme/a.md", projects, "global") == "inner"
    )
    assert (
        project_id_for_path("/nas/clients/other/a.md", projects, "global") == "outer"
    )


def test_project_id_for_path_falls_back_to_global():
    projects = [("acme", "/nas/clients/acme")]

    assert project_id_for_path("/home/florian/x.md", projects, "global") == "global"
    # An empty project list is also a fallback to global.
    assert project_id_for_path("/nas/clients/acme/a.md", [], "global") == "global"
