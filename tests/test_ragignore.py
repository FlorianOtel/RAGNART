"""
Tests for ragnart/ragignore.py (RagIgnoreMatcher).

Each table row builds a small directory tree in tmp_path and states the expected
outcome for some paths. The same tree is also built as a git worktree, using
`.gitignore` files in place of `.ragignore` and the same global file as
`core.excludesFile`. `git check-ignore -v --no-index` is then run as the oracle.
Its answers must match both the expectations and our matcher. The git cross-check
is skipped when no `git` binary is on PATH.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass, field

import pytest

from ragnart.ragignore import RagIgnoreMatcher, Verdict

GLOBAL = "GLOBAL"
"""Marker in an explain expectation meaning 'the global config/ragignore file'."""


@dataclass(frozen=True)
class Case:
    """One row of the parametrized table."""

    name: str
    # Paths to create. A trailing '/' makes a directory; anything else is an empty file.
    paths: tuple[str, ...] = ()
    # Ignore-file text keyed by directory relative to the tree root ("" = root).
    ignore_files: dict[str, str] = field(default_factory=dict)
    # Text of the global rules file (lowest precedence).
    global_text: str = ""
    # (relative path, expected ignored). A trailing '/' means a directory.
    checks: tuple[tuple[str, bool], ...] = ()
    # (relative path, expected source, expected lineno, expected pattern text).
    # `source` is an ignore file's path relative to the tree root, or GLOBAL.
    explains: tuple[tuple[str, str | None, int | None, str | None], ...] = ()


CASES = [
    Case(
        name="negation_overrides_earlier_exclude",
        paths=("a.log", "keep.log", "sub/a.log", "sub/keep.log"),
        ignore_files={"": "*.log\n!keep.log\n"},
        checks=(("a.log", True), ("keep.log", False), ("sub/a.log", True), ("sub/keep.log", False)),
        explains=(
            ("keep.log", ".ragignore", 2, "!keep.log"),
            ("a.log", ".ragignore", 1, "*.log"),
        ),
    ),
    Case(
        name="leading_slash_anchors_to_root",
        paths=("only.txt", "sub/only.txt"),
        ignore_files={"": "/only.txt\n"},
        checks=(("only.txt", True), ("sub/only.txt", False)),
    ),
    Case(
        name="leading_slash_in_nested_file_anchors_to_its_directory",
        paths=("sub/x.md", "sub/deep/x.md", "x.md"),
        ignore_files={"sub": "/x.md\n"},
        checks=(("sub/x.md", True), ("sub/deep/x.md", False), ("x.md", False)),
    ),
    Case(
        name="middle_slash_anchors_to_directory",
        paths=("docs/private/inner.txt", "x/docs/private/y.txt"),
        ignore_files={"": "docs/private\n"},
        checks=(
            ("docs/private/", True),
            ("docs/private/inner.txt", True),
            ("x/docs/private/", False),
            ("x/docs/private/y.txt", False),
        ),
    ),
    Case(
        name="bare_name_matches_at_any_depth",
        paths=("tmp.txt", "a/b/tmp.txt", "a/tmp.txt.bak"),
        ignore_files={"": "tmp.txt\n"},
        checks=(("tmp.txt", True), ("a/b/tmp.txt", True), ("a/tmp.txt.bak", False)),
    ),
    Case(
        name="doublestar_prefix_matches_any_depth",
        paths=("cache.bin", "a/cache.bin", "a/b/c/cache.bin"),
        ignore_files={"": "**/cache.bin\n"},
        checks=(("cache.bin", True), ("a/cache.bin", True), ("a/b/c/cache.bin", True)),
    ),
    Case(
        name="doublestar_middle_matches_zero_or_more_segments",
        paths=("a/b", "a/x/y/b", "c/a/b"),
        ignore_files={"": "a/**/b\n"},
        checks=(("a/b", True), ("a/x/y/b", True), ("c/a/b", False)),
    ),
    Case(
        name="trailing_slash_is_directory_only",
        paths=("build/x.txt", "tree/build"),
        ignore_files={"": "build/\n"},
        checks=(("build/", True), ("build/x.txt", True), ("tree/build", False)),
    ),
    Case(
        name="last_matching_pattern_wins_within_one_file",
        paths=("a.txt", "b.txt", "c.txt"),
        ignore_files={"": "*.txt\n!a.txt\n!b.txt\nb.txt\n"},
        checks=(("a.txt", False), ("b.txt", True), ("c.txt", True)),
        explains=(("a.txt", ".ragignore", 2, "!a.txt"), ("b.txt", ".ragignore", 4, "b.txt")),
    ),
    Case(
        name="deeper_file_overrides_shallower_file",
        paths=("keep.log", "other.log", "sub/keep.log", "sub/other.log", "sub/deep/keep.log"),
        ignore_files={"": "*.log\n", "sub": "!keep.log\n", "sub/deep": "keep.log\n"},
        checks=(
            ("keep.log", True),
            ("other.log", True),
            ("sub/keep.log", False),
            ("sub/other.log", True),
            ("sub/deep/keep.log", True),
        ),
        explains=(
            ("sub/keep.log", "sub/.ragignore", 1, "!keep.log"),
            ("sub/deep/keep.log", "sub/deep/.ragignore", 1, "keep.log"),
        ),
    ),
    Case(
        name="negation_inside_excluded_directory_has_no_effect",
        paths=("build/keep.txt",),
        ignore_files={"": "build/\n", "build": "!keep.txt\n"},
        checks=(("build/", True), ("build/keep.txt", True)),
        # The excluded ancestor's pattern is reported, not the unread nested file.
        explains=(("build/keep.txt", ".ragignore", 1, "build/"),),
    ),
    Case(
        name="negation_in_parent_cannot_reinclude_under_excluded_directory",
        paths=("build/keep.txt",),
        ignore_files={"": "build/\n!build/keep.txt\n"},
        checks=(("build/keep.txt", True),),
    ),
    Case(
        name="direct_file_match_beats_directory_negation",
        paths=("build/x.txt",),
        # `x.txt` matches the file itself. `!build/` only re-includes the directory,
        # which git never applies to a file beneath it, so the file stays ignored.
        ignore_files={"": "x.txt\nbuild/\n!build/\n"},
        checks=(("build/", False), ("build/x.txt", True)),
    ),
    Case(
        name="last_match_applies_to_directories",
        paths=("a/x.txt", "b"),
        ignore_files={"": "!*\na*\na\n"},
        checks=(("a/", True), ("a/x.txt", True), ("b", False)),
    ),
    Case(
        name="doublestar_suffix_matches_contents_not_directory",
        paths=("a/x.txt",),
        ignore_files={"": "a/**\n"},
        checks=(("a/", False), ("a/x.txt", True)),
    ),
    Case(
        name="negating_the_directory_reincludes_its_contents",
        paths=("build/x.txt",),
        ignore_files={"": "build/\n!build/\n"},
        checks=(("build/", False), ("build/x.txt", False)),
    ),
    Case(
        name="backslash_escapes_hash_and_bang",
        paths=("#hash.txt", "!bang.txt", "hash.txt"),
        ignore_files={"": "\\#hash.txt\n\\!bang.txt\n"},
        checks=(("#hash.txt", True), ("!bang.txt", True), ("hash.txt", False)),
    ),
    Case(
        name="trailing_unescaped_spaces_are_stripped",
        paths=("stripme.txt", "spacey ", "spacey"),
        ignore_files={"": "stripme.txt   \nspacey\\ \n"},
        checks=(("stripme.txt", True), ("spacey ", True), ("spacey", False)),
    ),
    Case(
        name="blank_comment_and_lone_bang_lines_are_inert",
        paths=("x.txt", "# a comment", "a comment"),
        ignore_files={"": "\n# a comment\n   \n!\nx.txt\n"},
        checks=(("x.txt", True), ("# a comment", False), ("a comment", False)),
    ),
    Case(
        name="global_file_is_lowest_precedence",
        paths=("a.bak", "keep.bak", "secret.txt"),
        global_text="*.bak\nsecret.txt\n",
        ignore_files={"": "!keep.bak\n"},
        checks=(("a.bak", True), ("keep.bak", False), ("secret.txt", True)),
        explains=(
            ("a.bak", GLOBAL, 1, "*.bak"),
            ("keep.bak", ".ragignore", 1, "!keep.bak"),
            ("secret.txt", GLOBAL, 2, "secret.txt"),
        ),
    ),
    Case(
        name="posix_class_alpha_matches_one_letter",
        paths=("a", "Z", "7", "-", "ab", "sub/q"),
        ignore_files={"": "[[:alpha:]]\n"},
        checks=(("a", True), ("Z", True), ("7", False), ("-", False), ("ab", False), ("sub/q", True)),
        explains=(("a", ".ragignore", 1, "[[:alpha:]]"),),
    ),
    Case(
        name="negated_posix_class_digit",
        paths=("x1", "xa", "x-", "x_"),
        ignore_files={"": "x[![:digit:]]\n"},
        checks=(("x1", False), ("xa", True), ("x-", True), ("x_", True)),
    ),
    Case(
        name="posix_class_mixed_with_range",
        paths=("A", "1", "3", "b"),
        ignore_files={"": "[[:upper:]0-2]\n"},
        checks=(("A", True), ("1", True), ("3", False), ("b", False)),
    ),
    Case(
        name="unknown_posix_class_matches_nothing",
        paths=("a",),
        ignore_files={"": "[[:bogus:]]\n"},
        checks=(("a", False),),
        explains=(("a", None, None, None),),
    ),
    Case(
        name="unterminated_bracket_matches_nothing",
        paths=("[abc", "abc"),
        ignore_files={"": "[abc\n"},
        checks=(("[abc", False), ("abc", False)),
        explains=(("[abc", None, None, None),),
    ),
    Case(
        name="question_mark_matches_one_non_slash_character",
        paths=("abc", "abbc", "ac", "a.c"),
        ignore_files={"": "a?c\n"},
        checks=(("abc", True), ("abbc", False), ("ac", False), ("a.c", True)),
    ),
    Case(
        name="bracket_leading_close_bracket_is_literal",
        paths=("]", "a", "b", "c"),
        ignore_files={"": "[]ab]\n"},
        checks=(("]", True), ("a", True), ("b", True), ("c", False)),
    ),
    Case(
        name="negated_bracket_leading_close_bracket_is_literal",
        paths=("]", "a", "c"),
        ignore_files={"": "[!]ab]\n"},
        checks=(("]", False), ("a", False), ("c", True)),
    ),
    Case(
        name="bracket_range",
        paths=("a", "b", "c", "d"),
        ignore_files={"": "[a-c]\n"},
        checks=(("a", True), ("b", True), ("c", True), ("d", False)),
    ),
    Case(
        name="backslash_before_ordinary_character_is_literal_character",
        paths=("a", "b"),
        ignore_files={"": "\\a\n"},
        checks=(("a", True), ("b", False)),
    ),
    Case(
        name="leading_escaped_space_is_kept",
        paths=(" x", "x"),
        ignore_files={"": "\\ x\n"},
        checks=((" x", True), ("x", False)),
    ),
    Case(
        name="doublestar_inside_segment_acts_like_single_star",
        paths=("ab", "axxb", "a/b", "a/x/b"),
        ignore_files={"": "a**b\n"},
        checks=(("ab", True), ("axxb", True), ("a/b", False), ("a/x/b", False)),
    ),
    Case(
        name="doublestar_trailing_slash_is_directory_only_below_a",
        paths=("a/x.txt", "a/d/y.txt"),
        ignore_files={"": "a/**/\n"},
        checks=(("a", False), ("a/x.txt", False), ("a/d/", True), ("a/d/y.txt", True)),
    ),
    Case(
        name="no_match_reports_no_source",
        paths=("plain.txt",),
        checks=(("plain.txt", False),),
        explains=(("plain.txt", None, None, None),),
    ),
]


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------


def _build_tree(root: os.PathLike, paths: tuple[str, ...], ignore_files: dict[str, str], ignore_name: str) -> None:
    """Create the paths and the ignore files under `root`."""
    os.makedirs(root, exist_ok=True)
    for rel in paths:
        if rel.endswith("/"):
            os.makedirs(os.path.join(root, rel.rstrip("/")), exist_ok=True)
        else:
            full = os.path.join(root, rel)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            open(full, "w", encoding="utf-8").close()
    for dir_rel, text in ignore_files.items():
        directory = os.path.join(root, dir_rel)
        os.makedirs(directory, exist_ok=True)
        with open(os.path.join(directory, ignore_name), "w", encoding="utf-8") as handle:
            handle.write(text)


def _split(rel: str) -> tuple[str, bool]:
    """Split a table path into (path without trailing slash, is_directory)."""
    return (rel.rstrip("/"), rel.endswith("/"))


def _git_check_ignore(repo: os.PathLike, global_path: str, rels: list[str]) -> list[tuple[str, int, str]]:
    """
    Ask git for the deciding rule of each path, using `check-ignore -v -n`.

    Returns one (source, lineno, pattern) per input path, in order. A path with
    no matching rule gives ("", 0, ""). Git reports the source as relative to the
    repo for in-tree files, and as the configured path for the global file.
    """
    cmd = [
        "git",
        "-c",
        f"core.excludesFile={global_path}",
        "check-ignore",
        "-v",
        "-n",
        "--no-index",
        "--",
        *rels,
    ]
    result = subprocess.run(cmd, cwd=repo, capture_output=True, text=True, check=False)
    lines = result.stdout.splitlines()
    assert len(lines) == len(rels), f"git gave {len(lines)} lines for {len(rels)} paths: {result.stderr}"
    parsed = []
    for line in lines:
        # Each line is `source:lineno:pattern<TAB>path`. Split off the path first,
        # then the source, so a ':' inside the pattern does not confuse us.
        left, _path = line.split("\t", 1)
        if left == "::":
            parsed.append(("", 0, ""))
            continue
        source, lineno, pattern = left.split(":", 2)
        parsed.append((source, int(lineno), pattern))
    return parsed


def _ours(matcher: RagIgnoreMatcher, root: str, rel: str) -> Verdict:
    """Our explain() for a table path."""
    path, is_dir = _split(rel)
    return matcher.explain(os.path.join(root, path), is_dir=is_dir)


def _expected_source(source: str | None, rag_root: str, global_path: str) -> str | None:
    """Turn a table source ('.ragignore', 'sub/.ragignore', GLOBAL, None) into an absolute path."""
    if source is None:
        return None
    if source == GLOBAL:
        return os.path.abspath(global_path)
    return os.path.join(rag_root, source)


def _git_source_to_ours(source: str, rag_root: str, global_path: str) -> str | None:
    """Map a git source string onto the absolute path our matcher reports."""
    if source == "":
        return None
    if source == os.path.abspath(global_path):
        return source
    # In-tree git files are `.gitignore`. Our twin files are `.ragignore`, at the
    # same relative place.
    assert source.endswith(".gitignore"), f"unexpected git source {source!r}"
    rel_dir = source[: -len(".gitignore")]
    return os.path.join(rag_root, rel_dir + ".ragignore")


def _git_ignored_names(repo: os.PathLike, names: list[str]) -> set[str]:
    """
    Ask git which of the given file names (no '/') are ignored, using `check-ignore -z`.

    Names go over stdin, NUL-separated, so any byte except '/' and NUL is allowed,
    including newline, which the `-v` line format cannot carry.
    """
    data = "".join(name + "\0" for name in names).encode("utf-8", "surrogateescape")
    result = subprocess.run(
        ["git", "check-ignore", "--no-index", "-z", "--stdin"],
        cwd=repo,
        input=data,
        capture_output=True,
        check=False,
    )
    return {name for name in result.stdout.decode("utf-8", "surrogateescape").split("\0") if name}


# ----------------------------------------------------------------------------
# Table test
# ----------------------------------------------------------------------------

GIT_AVAILABLE = shutil.which("git") is not None


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_table(case: Case, tmp_path) -> None:
    rag_root = tmp_path / "rag"
    git_root = tmp_path / "git"
    global_file = tmp_path / "global-ragignore"
    global_file.write_text(case.global_text, encoding="utf-8")
    global_path = str(global_file)

    _build_tree(rag_root, case.paths, case.ignore_files, ".ragignore")
    matcher = RagIgnoreMatcher(str(rag_root), global_path=global_path)
    rag_root_str = os.path.normpath(str(rag_root))

    # 1. Our matcher against the expectations in the table.
    for rel, expected in case.checks:
        path, is_dir = _split(rel)
        full = os.path.join(rag_root_str, path)
        if is_dir:
            got = matcher.is_dir_excluded(os.path.dirname(full), os.path.basename(full))
        else:
            got = matcher.is_path_excluded(full)
        assert got == expected, f"{case.name}: {rel!r} ignored={got}, expected {expected}"

    for rel, source, lineno, text in case.explains:
        verdict = _ours(matcher, rag_root_str, rel)
        expected = Verdict(
            verdict.ignored,
            _expected_source(source, rag_root_str, global_path),
            lineno,
            text,
        )
        assert verdict == expected, f"{case.name}: explain({rel!r}) = {verdict}, expected {expected}"

    if not GIT_AVAILABLE:
        pytest.skip("git not on PATH: expectations checked, git cross-check skipped")

    # 2. Git as the oracle. The same tree is rebuilt with `.gitignore` names.
    _build_tree(git_root, case.paths, case.ignore_files, ".gitignore")
    subprocess.run(["git", "init", "-q", str(git_root)], check=True, capture_output=True)

    all_rels = list(dict.fromkeys([rel for rel, _ in case.checks] + [rel for rel, *_ in case.explains]))
    # Paths go to git without the trailing slash. Git stats each one, so it
    # knows whether it is a directory.
    git_paths = [_split(rel)[0] for rel in all_rels]
    git_results = _git_check_ignore(git_root, global_path, git_paths)

    # The table's expected ignored flags must also agree with git.
    for rel, expected in case.checks:
        source, lineno, pattern = git_results[all_rels.index(rel)]
        # A '!' pattern is a re-inclusion. With no pattern at all, nothing matched.
        git_ignored = bool(pattern) and not pattern.startswith("!")
        assert git_ignored == expected, (
            f"{case.name}: git says {rel!r} ignored={git_ignored}, table expects {expected}"
        )

    # Our full verdict, including the deciding pattern, must equal git's.
    for index, rel in enumerate(all_rels):
        ours = _ours(matcher, rag_root_str, rel)
        source, lineno, pattern = git_results[index]
        git_ignored = bool(pattern) and not pattern.startswith("!")
        git_verdict = (
            git_ignored,
            _git_source_to_ours(source, rag_root_str, global_path),
            lineno or None,
            pattern or None,
        )
        # The pattern text is compared exactly. Both sides have trailing unescaped
        # spaces already stripped.
        ours_verdict = (
            ours.ignored,
            ours.source_file,
            ours.lineno,
            ours.pattern,
        )
        assert ours_verdict == git_verdict, f"{case.name}: {rel!r} ours={ours_verdict} git={git_verdict}"


# ----------------------------------------------------------------------------
# Focused tests
# ----------------------------------------------------------------------------



def test_symlink_is_matched_by_walked_path_not_realpath(tmp_path) -> None:
    """
    A symlink is judged by the name it was walked under, never by its target.

    `link` points at `real/`. The rule `real/` excludes the real directory, but
    the walk reaches it as `link`, and so `link/x.txt` is not excluded.
    """
    rag_root = tmp_path / "rag"
    _build_tree(rag_root, ("real/x.txt",), {"": "real/\n"}, ".ragignore")
    os.symlink(rag_root / "real", rag_root / "link", target_is_directory=True)
    global_file = tmp_path / "global"
    global_file.write_text("", encoding="utf-8")
    matcher = RagIgnoreMatcher(str(rag_root), global_path=str(global_file))

    assert matcher.is_path_excluded(str(rag_root / "real" / "x.txt")) is True
    assert matcher.is_path_excluded(str(rag_root / "link" / "x.txt")) is False


def test_missing_global_file_fails_loudly(tmp_path) -> None:
    """A missing global file is an error, not a silent loss of the global exclusions."""
    (tmp_path / "rag").mkdir()
    with pytest.raises(FileNotFoundError):
        RagIgnoreMatcher(str(tmp_path / "rag"), global_path=str(tmp_path / "nope"))


def test_path_outside_root_is_rejected(tmp_path) -> None:
    """Paths must lie below the crawl root."""
    (tmp_path / "rag").mkdir()
    global_file = tmp_path / "global"
    global_file.write_text("", encoding="utf-8")
    matcher = RagIgnoreMatcher(str(tmp_path / "rag"), global_path=str(global_file))
    with pytest.raises(ValueError):
        matcher.is_path_excluded(str(tmp_path / "elsewhere.txt"))


def test_shipped_global_file_excludes_expected_names(tmp_path) -> None:
    """The real config/ragignore excludes the migrated names and nothing ordinary."""
    (tmp_path / "rag").mkdir()
    matcher = RagIgnoreMatcher(str(tmp_path / "rag"))  # default global path
    root = tmp_path / "rag"
    assert matcher.is_dir_excluded(str(root / "a"), "node_modules") is True
    assert matcher.is_dir_excluded(str(root / "x"), ".git") is True
    assert matcher.is_dir_excluded(str(root / "x"), "pkg.dist-info") is True
    assert matcher.is_path_excluded(str(root / "x" / "._foo.pptx")) is True
    assert matcher.is_path_excluded(str(root / "x" / "~$foo.pptx")) is True
    assert matcher.is_path_excluded(str(root / "x" / "notes.md")) is False
    assert matcher.explain(str(root / "x" / "._foo.pptx")).source_file == os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "config", "ragignore")
    )


# ----------------------------------------------------------------------------
# POSIX character classes, checked character by character against git
# ----------------------------------------------------------------------------

POSIX_CLASS_NAMES = (
    "alnum", "alpha", "blank", "cntrl", "digit", "graph",
    "lower", "print", "punct", "space", "upper", "xdigit",
)


@pytest.mark.skipif(not GIT_AVAILABLE, reason="git not on PATH")
@pytest.mark.parametrize("negated", [False, True], ids=["plain", "negated"])
@pytest.mark.parametrize("class_name", POSIX_CLASS_NAMES)
def test_posix_class_matches_every_ascii_character_like_git(class_name, negated, tmp_path) -> None:
    """
    For every ASCII character c, `x<c>` is ignored by `x[[:cls:]]` exactly when git says so.

    Names are "x" followed by one character, so the class is the only thing being
    tested. Every ASCII value except '/' is tried, because '/' cannot be in a file
    name. The negated form `x[![:cls:]]` is covered too.
    """
    bang = "!" if negated else ""
    pattern = f"x[{bang}[:{class_name}:]]"
    names = [f"x{chr(code)}" for code in range(1, 128) if chr(code) != "/"]

    git_repo = tmp_path / "git"
    git_repo.mkdir()
    subprocess.run(["git", "init", "-q", str(git_repo)], check=True, capture_output=True)
    (git_repo / ".gitignore").write_text(pattern + "\n", encoding="utf-8")
    git_ignored = _git_ignored_names(git_repo, names)

    rag_root = tmp_path / "rag"
    rag_root.mkdir()
    (rag_root / ".ragignore").write_text(pattern + "\n", encoding="utf-8")
    global_file = tmp_path / "global"
    global_file.write_text("", encoding="utf-8")
    matcher = RagIgnoreMatcher(str(rag_root), global_path=str(global_file))
    ours_ignored = {name for name in names if matcher.is_path_excluded(os.path.join(str(rag_root), name))}

    assert ours_ignored == git_ignored, (
        f"{pattern}: only ours={sorted(ours_ignored - git_ignored)!r}, "
        f"only git={sorted(git_ignored - ours_ignored)!r}"
    )
