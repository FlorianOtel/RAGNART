"""
Tests for the scanner's use of RagIgnoreMatcher (ragnart/scanner.py scan_nfs_roots).

Each test builds a small NFS-shaped tree in tmp_path and runs the real scanner over it,
with a real StateDB on a temporary SQLite file. The global rules are the shipped
config/ragignore, and the nested rules are .ragignore files written into the tree.
No live NFS, Qdrant or OpenCode is contacted: the config has no opencode or
claude_chats section, so those scans return at once.
"""

from __future__ import annotations

import os

import pytest

from ragnart.scanner import is_includable, scan_nfs_roots
from ragnart.state import StateDB

INCLUDE = [".md", ".pdf", ".txt"]


def _write(path, text: str = "") -> None:
    """Create a file, and its parent directories, with the given text."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


def _config(root, include=INCLUDE, **scanner_extra) -> dict:
    """A config with one user root, the way ragnart.yaml lays it out."""
    scanner = {"include_extensions": list(include), **scanner_extra}
    return {
        "rag": {"scanner": scanner},
        "users": {"tester@example.com": {"owner": "tester", "nfs_roots": [str(root)]}},
    }


def _scan(tmp_path, root, config=None):
    """Run the real scanner over root, with a real StateDB. Return (result, admitted paths)."""
    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        result = scan_nfs_roots(state_db, config or _config(root), user_filter="tester")
        admitted = {row["file_path"] for row in state_db._conn.execute("SELECT file_path FROM ingestion_queue")}
    finally:
        state_db.close()
    return result, admitted


def _rel(root, admitted) -> set[str]:
    """Express admitted absolute paths relative to root, with '/' separators."""
    return {os.path.relpath(p, root).replace(os.sep, "/") for p in admitted}


# ----------------------------------------------------------------------------
# Admission across global and nested rules
# ----------------------------------------------------------------------------


def test_scan_admits_only_non_excluded_included_files(tmp_path) -> None:
    root = tmp_path / "nas"
    _write(str(root / "a.md"))
    _write(str(root / "b.pdf"))
    _write(str(root / "photo.jpg"))              # extension not on the whitelist
    _write(str(root / "node_modules" / "x.md"))  # global rule: node_modules/
    _write(str(root / "Library" / "y.md"))        # global rule: Library/
    _write(str(root / "._foo.pptx"))              # global rule: *._*
    _write(str(root / "~$lock.docx"))             # global rule: *~$*

    result, admitted = _scan(tmp_path, root)

    assert _rel(root, admitted) == {"a.md", "b.pdf"}
    assert result["scanned"] == 2
    assert result["skipped_roots"] == []


def test_nested_ragignore_excludes_only_below_its_directory(tmp_path) -> None:
    root = tmp_path / "nas"
    _write(str(root / "proj" / ".ragignore"), "secret.md\n")
    _write(str(root / "proj" / "secret.md"))        # excluded by proj/.ragignore
    _write(str(root / "proj" / "keep.md"))
    _write(str(root / "other" / "secret.md"))       # outside proj/, so admitted

    _, admitted = _scan(tmp_path, root)

    assert _rel(root, admitted) == {"proj/keep.md", "other/secret.md"}


def test_excluded_directory_is_pruned_so_its_own_ragignore_cannot_reinclude(tmp_path) -> None:
    root = tmp_path / "nas"
    _write(str(root / ".ragignore"), "build/\n")
    _write(str(root / "build" / ".ragignore"), "!keep.md\n")  # never read: build/ is pruned
    _write(str(root / "build" / "keep.md"))

    _, admitted = _scan(tmp_path, root)

    assert admitted == set()


def test_ragignore_file_itself_is_never_returned(tmp_path) -> None:
    """A .ragignore has no whitelisted extension, so the scanner never admits it."""
    root = tmp_path / "nas"
    _write(str(root / ".ragignore"), "*.log\n")
    _write(str(root / "sub" / ".ragignore"), "*.tmp\n")
    _write(str(root / "a.md"))

    _, admitted = _scan(tmp_path, root)

    assert _rel(root, admitted) == {"a.md"}
    assert not any(os.path.basename(p) == ".ragignore" for p in admitted)


def test_ragignore_file_is_not_returned_even_when_extension_list_is_broad(tmp_path) -> None:
    """Still true when the whitelist contains every extension the NAS holds."""
    root = tmp_path / "nas"
    _write(str(root / ".ragignore"), "")
    _write(str(root / "a.md"))

    _, admitted = _scan(tmp_path, root, _config(root, include=[".md", ".ragignore"]))

    # ".ragignore" is not a suffix that Path() reports, so it can never match the whitelist.
    assert not any(os.path.basename(p) == ".ragignore" for p in admitted)


def test_extension_whitelist_is_case_insensitive(tmp_path) -> None:
    root = tmp_path / "nas"
    _write(str(root / "LOUD.MD"))

    _, admitted = _scan(tmp_path, root)

    assert _rel(root, admitted) == {"LOUD.MD"}


# ----------------------------------------------------------------------------
# Removed ragnart.yaml keys are refused, not ignored
# ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    ["exclude_dir_names", "exclude_dir_suffixes", "exclude_file_patterns"],
)
def test_removed_exclude_key_raises_and_names_it(tmp_path, key) -> None:
    root = tmp_path / "nas"
    root.mkdir()
    config = _config(root, **{key: ["Library/"]})

    with pytest.raises(ValueError) as info:
        _scan(tmp_path, root, config)

    message = str(info.value)
    assert key in message
    assert "config/ragignore" in message
    assert ".ragignore" in message


def test_all_removed_keys_are_named_together(tmp_path) -> None:
    root = tmp_path / "nas"
    root.mkdir()
    config = _config(
        root,
        exclude_dir_names=["Library/"],
        exclude_dir_suffixes=[".dist-info"],
        exclude_file_patterns=["~$"],
    )

    with pytest.raises(ValueError) as info:
        _scan(tmp_path, root, config)

    for key in ("exclude_dir_names", "exclude_dir_suffixes", "exclude_file_patterns"):
        assert key in str(info.value)


def test_missing_include_extensions_is_still_refused(tmp_path) -> None:
    root = tmp_path / "nas"
    root.mkdir()
    config = {
        "rag": {"scanner": {}},
        "users": {"tester@example.com": {"owner": "tester", "nfs_roots": [str(root)]}},
    }

    with pytest.raises(ValueError, match="include_extensions"):
        _scan(tmp_path, root, config)


# ----------------------------------------------------------------------------
# is_includable (the CLI's inclusion helper)
# ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/x/report.PDF", True),
        ("/x/notes.md", True),
        ("/x/photo.jpg", False),
        ("/x/.ragignore", False),   # no suffix at all
        ("/x/noext", False),
    ],
)
def test_is_includable(path, expected) -> None:
    assert is_includable(path, frozenset([".pdf", ".md"])) is expected


# ----------------------------------------------------------------------------
# Global rules file lookup
# ----------------------------------------------------------------------------


def test_default_global_rules_resolve_to_repo_config_not_cwd(tmp_path, monkeypatch) -> None:
    """The shipped config/ragignore is found from the package, whatever the working directory."""
    from ragnart.ragignore import DEFAULT_GLOBAL_PATH

    monkeypatch.chdir(tmp_path)
    assert os.path.isfile(DEFAULT_GLOBAL_PATH)
    assert os.path.normpath(DEFAULT_GLOBAL_PATH).endswith(os.path.join("config", "ragignore"))
