"""
Tests for project-marker discovery in scan_nfs_roots (ragnart/scanner.py).

A directory holding `.ragnart/project.toml` is a ragnart project. These tests run the
real scanner over a small NFS-shaped tree in tmp_path, with a real StateDB on a
temporary SQLite file. No live NFS, Qdrant or OpenCode is contacted: the config has no
opencode or claude_chats section.
"""

from __future__ import annotations

import logging
import os

from ragnart.scanner import scan_nfs_roots
from ragnart.state import StateDB

INCLUDE = [".md", ".txt"]
OWNER = "tester"

VALID_MARKER = 'name = "Acme"\nid = "acme-uuid"\nkind = "customer"\n'
MALFORMED_MARKER = 'name = "Broken"\nid = \n'  # not valid TOML


def _write(path, text: str = "") -> None:
    """Create a file, and its parent directories, with the given text."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


def _config(root) -> dict:
    """A config with one user root, the way ragnart.yaml lays it out."""
    return {
        "rag": {"scanner": {"include_extensions": list(INCLUDE)}},
        "users": {"tester@example.com": {"owner": OWNER, "nfs_roots": [str(root)]}},
    }


def _scan(tmp_path, root):
    """Run the real scanner over root. Return (result, admitted absolute paths)."""
    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        result = scan_nfs_roots(state_db, _config(root), user_filter=OWNER)
        admitted = {row["file_path"] for row in state_db._conn.execute("SELECT file_path FROM ingestion_queue")}
    finally:
        state_db.close()
    return result, admitted


# ----------------------------------------------------------------------------
# Discovery
# ----------------------------------------------------------------------------


def test_valid_marker_is_returned_with_its_fields(tmp_path) -> None:
    root = tmp_path / "nas"
    project = root / "acme"
    _write(str(project / ".ragnart" / "project.toml"), VALID_MARKER)

    result, _admitted = _scan(tmp_path, root)

    assert result["project_markers"] == [
        {
            "path": str(project),
            "owner": OWNER,
            "name": "Acme",
            "id": "acme-uuid",
            "inherit_access": True,
            "mode": None,
            "tags": {"kind": "customer"},
        }
    ]


def test_marker_directory_files_are_still_scanned_as_content(tmp_path) -> None:
    root = tmp_path / "nas"
    project = root / "acme"
    _write(str(project / ".ragnart" / "project.toml"), VALID_MARKER)
    _write(str(project / "notes.md"), "the project notes")

    _result, admitted = _scan(tmp_path, root)

    assert str(project / "notes.md") in admitted


def test_ragnart_directory_is_never_walked_as_content(tmp_path) -> None:
    # notes.md inside .ragnart has an included extension, so it would be admitted if the
    # walk entered the marker directory. It must not be.
    root = tmp_path / "nas"
    project = root / "acme"
    _write(str(project / ".ragnart" / "project.toml"), VALID_MARKER)
    _write(str(project / ".ragnart" / "notes.md"), "configuration, not content")

    _result, admitted = _scan(tmp_path, root)

    assert str(project / ".ragnart" / "notes.md") not in admitted


def test_root_directory_can_itself_be_a_marked_project(tmp_path) -> None:
    root = tmp_path / "nas"
    _write(str(root / ".ragnart" / "project.toml"), VALID_MARKER)

    result, _admitted = _scan(tmp_path, root)

    assert [marker["path"] for marker in result["project_markers"]] == [str(root)]


def test_nested_markers_are_both_returned(tmp_path) -> None:
    # Attachment (not this module) decides which marker is the closest parent. The
    # scanner's job is only to report every marker it can trust.
    root = tmp_path / "nas"
    _write(str(root / "outer" / ".ragnart" / "project.toml"), 'name = "Outer"\nid = "outer-id"\n')
    _write(str(root / "outer" / "inner" / ".ragnart" / "project.toml"), 'name = "Inner"\nid = "inner-id"\n')

    result, _admitted = _scan(tmp_path, root)

    paths = {marker["path"] for marker in result["project_markers"]}
    assert paths == {str(root / "outer"), str(root / "outer" / "inner")}


def test_directory_without_a_marker_yields_no_marker(tmp_path) -> None:
    root = tmp_path / "nas"
    _write(str(root / "plain" / "doc.md"), "no marker here")

    result, _admitted = _scan(tmp_path, root)

    assert result["project_markers"] == []


# ----------------------------------------------------------------------------
# Failure handling
# ----------------------------------------------------------------------------


def test_malformed_marker_is_logged_and_excluded_but_files_are_still_scanned(tmp_path, caplog) -> None:
    root = tmp_path / "nas"
    broken = root / "broken"
    _write(str(broken / ".ragnart" / "project.toml"), MALFORMED_MARKER)
    _write(str(broken / "doc.md"), "ordinary document")

    with caplog.at_level(logging.ERROR, logger="ragnart.projects"):
        result, admitted = _scan(tmp_path, root)

    assert result["project_markers"] == []
    assert any(
        record.levelno == logging.ERROR and str(broken / ".ragnart" / "project.toml") in record.getMessage()
        for record in caplog.records
    )
    # A bad marker keeps the directory outside the project model, but not outside the crawl.
    assert str(broken / "doc.md") in admitted


def test_marker_in_directory_excluded_by_ragignore_is_skipped(tmp_path) -> None:
    root = tmp_path / "nas"
    _write(str(root / ".ragignore"), "skipme/\n")
    _write(str(root / "skipme" / ".ragnart" / "project.toml"), VALID_MARKER)
    _write(str(root / "kept" / ".ragnart" / "project.toml"), 'name = "Kept"\nid = "kept-id"\n')

    result, _admitted = _scan(tmp_path, root)

    paths = [marker["path"] for marker in result["project_markers"]]
    assert paths == [str(root / "kept")]


def test_marker_below_an_excluded_parent_is_not_found(tmp_path) -> None:
    # An excluded parent is never entered, so no marker below it can be found, even
    # when the marker directory is not itself named by a rule.
    root = tmp_path / "nas"
    _write(str(root / ".ragignore"), "archive/\n")
    _write(str(root / "archive" / "old" / ".ragnart" / "project.toml"), 'name = "Old"\nid = "old-id"\n')

    result, _admitted = _scan(tmp_path, root)

    assert result["project_markers"] == []
