"""
Tests for config/ragignore.example, the self-documented .ragignore template.

Four claims are checked:

1. As shipped, the file is a no-op. Every non-blank line is a comment, and loaded
   as a .ragignore it excludes nothing.
2. Every example line, activated, behaves as its comment says. An example line is
   a line that starts with the marker "#> ". Activating one means removing exactly
   that marker, so trailing spaces in an example survive.
3. Each activated example gives the same verdict from RagIgnoreMatcher as from
   `git check-ignore --no-index`, for every probe path.
4. The file is never read as an ignore file, and its extension is never indexed.

The examples are grouped into UNITS. A unit is one example, or several examples
that only make sense together (a negation needs the rule it negates). Coverage is
checked both ways: every example line in the file belongs to a unit, and every
unit line is a literal line of the file.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from ragnart.ragignore import (
    DEFAULT_GLOBAL_PATH,
    NO_MATCH,
    RAGIGNORE_FILENAME,
    RagIgnoreMatcher,
    Verdict,
    _compile_line,
)
from ragnart.scanner import include_extensions_from_config, is_includable

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_PATH = REPO_ROOT / "config" / "ragignore.example"
RAGNART_YAML = REPO_ROOT / "ragnart.yaml"

MARKER = "#> "
"""The prefix that marks an example line. Removing it activates the line."""

GIT_AVAILABLE = shutil.which("git") is not None

PROBES: tuple[str, ...] = (
    # A trailing '/' marks a directory. Every other entry is an empty file.
    # Probes are created on disk, so git and the matcher see the same tree.
    "build/",
    "build/out.o",
    "target/",
    "target/app.jar",
    "generated-reports/",
    "generated-reports/2026-11.md",
    "dumps/",
    "dumps/a.jsonl",
    "a.log",
    "keep.log",
    "sub/",
    "sub/b.log",
    "sub/keep.log",
    "cache",
    "sub/cache/",
    "sub/cache/x.txt",
    "private/",
    "private/secret.txt",
    "sub/private/",
    "sub/private/s.txt",
    "archive/",
    "archive/2025/",
    "archive/2025/x.txt",
    "docs/",
    "docs/draft.md",
    "docs/internal.md",
    "docs/a/",
    "docs/a/b/",
    "docs/a/b/internal.md",
    "sub/docs/",
    "sub/docs/draft.md",
    "sub/docs/internal.md",
    "dir/",
    "dir/keep.md",
    "dir/other.md",
    "dir/sub/",
    "dir/sub/x.md",
    "notes/",
    "notes/README.md",
    "notes/other.md",
    "README.md",
    "TODO.md",
    "sub/TODO.md",
    "#draft.md",
    "!bang.txt",
    "scratch ",
    "Thumbs.db",
    "sub/Thumbs.db",
    "x.tmp",
    "sub/y.tmp",
    "draft-a.md",
    "draft-x.md",
    "draft-10.md",
    "scan-a.pdf",
    "scan-b.pdf",
    "scan-1.pdf",
    "photo-q.jpg",
    "photo-5.jpg",
    "part-7.bin",
    "part-x.bin",
)


@dataclass
class Unit:
    """
    One example, or several examples that only make sense together.

    ``examples`` are the activated lines, exactly as they appear in the file with
    the marker removed. ``ignored`` is the set of probe paths the comment says are
    excluded. ``files`` maps a directory (relative to the root, "" for the root) to
    the text of its ignore file. Leave it None for a single root file holding
    ``examples``; set it when the unit needs more context, such as the rule that a
    later negation undoes.
    """

    name: str
    examples: tuple[str, ...]
    ignored: frozenset[str]
    files: dict[str, str] | None = None

    def root_files(self) -> dict[str, str]:
        """The ignore files to build for this unit."""
        if self.files is not None:
            return self.files
        return {"": "\n".join(self.examples) + "\n"}


UNITS: list[Unit] = [
    Unit(
        "comment",
        ("# A comment line. This example is also ignored.",),
        frozenset(),
    ),
    Unit("escape_hash", (r"\#draft.md",), frozenset({"#draft.md"})),
    Unit("escape_bang", (r"\!bang.txt",), frozenset({"!bang.txt"})),
    # The backslash keeps the trailing space in the pattern. It matches "scratch ".
    Unit("trailing_space_escaped", ("scratch\\ ",), frozenset({"scratch "})),
    Unit("wildcard_star", ("*.tmp",), frozenset({"x.tmp", "sub/y.tmp"})),
    Unit("wildcard_question", ("draft-?.md",), frozenset({"draft-a.md", "draft-x.md"})),
    Unit("class_list", ("scan-[abc].pdf",), frozenset({"scan-a.pdf", "scan-b.pdf"})),
    Unit("class_range", ("photo-[a-z].jpg",), frozenset({"photo-q.jpg"})),
    Unit("class_negated", ("draft-[!abc].md",), frozenset({"draft-x.md"})),
    Unit("posix_class", ("part-[[:digit:]].bin",), frozenset({"part-7.bin"})),
    Unit(
        "no_slash_any_depth",
        ("Thumbs.db",),
        frozenset({"Thumbs.db", "sub/Thumbs.db"}),
    ),
    Unit("leading_slash_anchor", ("/TODO.md",), frozenset({"TODO.md"})),
    Unit("middle_slash_anchor", ("docs/draft.md",), frozenset({"docs/draft.md"})),
    Unit(
        "trailing_slash_dirs_only",
        ("cache/",),
        frozenset({"sub/cache/", "sub/cache/x.txt"}),
    ),
    Unit(
        "leading_double_star",
        ("**/private/",),
        frozenset({"private/", "private/secret.txt", "sub/private/", "sub/private/s.txt"}),
    ),
    Unit(
        "trailing_double_star",
        ("archive/**",),
        frozenset({"archive/2025/", "archive/2025/x.txt"}),
    ),
    Unit(
        "middle_double_star",
        ("docs/**/internal.md",),
        frozenset({"docs/internal.md", "docs/a/b/internal.md"}),
    ),
    Unit(
        "negation_pair",
        ("*.log", "!keep.log"),
        frozenset({"a.log", "sub/b.log"}),
    ),
    Unit(
        "directory_contents_pair",
        ("dir/*", "!dir/keep.md"),
        frozenset({"dir/other.md", "dir/sub/", "dir/sub/x.md"}),
    ),
    Unit(
        "opt_out_by_name",
        ("!README.md",),
        frozenset({"README.md"}),
        # A root rule excludes every README.md. The deeper file re-includes only
        # notes/README.md, so the root README.md stays excluded.
        files={"": "README.md\n", "notes": "!README.md\n"},
    ),
    Unit(
        "typical_build",
        ("build/", "target/"),
        frozenset({"build/", "build/out.o", "target/", "target/app.jar"}),
    ),
    Unit(
        "typical_generated",
        ("generated-reports/",),
        frozenset({"generated-reports/", "generated-reports/2026-11.md"}),
    ),
    Unit("typical_data", ("dumps/",), frozenset({"dumps/", "dumps/a.jsonl"})),
    Unit(
        "typical_logs",
        ("*.log",),
        frozenset({"a.log", "keep.log", "sub/b.log", "sub/keep.log"}),
    ),
]


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------


def _example_lines() -> list[str]:
    """Every line of the template, with its line terminators removed but spaces kept."""
    return EXAMPLE_PATH.read_text(encoding="utf-8").splitlines()


def _example_patterns() -> set[str]:
    """The activated text of every example line in the file (marker removed)."""
    return {line[len(MARKER):] for line in _example_lines() if line.startswith(MARKER)}


def _split(rel: str) -> tuple[str, bool]:
    """Split a probe into (path without trailing slash, is_directory)."""
    return (rel.rstrip("/"), rel.endswith("/"))


def _build_tree(root: Path, files: dict[str, str], ignore_name: str) -> None:
    """Create every probe under `root`, and the ignore files named `ignore_name`."""
    os.makedirs(root, exist_ok=True)
    for rel in PROBES:
        path, is_dir = _split(rel)
        full = os.path.join(root, path)
        if is_dir:
            os.makedirs(full, exist_ok=True)
        else:
            os.makedirs(os.path.dirname(full), exist_ok=True)
            open(full, "w", encoding="utf-8").close()
    for dir_rel, text in files.items():
        directory = os.path.join(root, dir_rel)
        os.makedirs(directory, exist_ok=True)
        with open(os.path.join(directory, ignore_name), "w", encoding="utf-8") as handle:
            handle.write(text)


def _empty_global(tmp_path: Path) -> str:
    """A global rules file with no rules, so only the tree's own files apply."""
    path = tmp_path / "empty-global-ignore"
    path.write_text("", encoding="utf-8")
    return str(path)


def _matcher_verdicts(matcher: RagIgnoreMatcher, root: str) -> dict[str, Verdict]:
    """Our explain() for every probe."""
    verdicts = {}
    for rel in PROBES:
        path, is_dir = _split(rel)
        verdicts[rel] = matcher.explain(os.path.join(root, path), is_dir=is_dir)
    return verdicts


def _ignored_by_matcher(matcher: RagIgnoreMatcher, root: str) -> frozenset[str]:
    """The probes our matcher excludes."""
    verdicts = _matcher_verdicts(matcher, root)
    return frozenset(rel for rel, verdict in verdicts.items() if verdict.ignored)


def _git_verdicts(repo: Path, excludes_file: str) -> dict[str, tuple[str, int, str]]:
    """
    Ask git for the deciding rule of every probe, with `check-ignore -v -n`.

    Returns (source, lineno, pattern) per probe. A probe with no matching rule
    gives ("", 0, ""). The global excludes file is set explicitly, so the user's
    own global ignore rules cannot change the answer.
    """
    rels = list(PROBES)
    # Directories go to git without their trailing slash, as the walk presents
    # them. With a slash, git would match `dir/*` against the literal "dir/" and
    # report a directory the walk never excludes. The probes exist on disk, so git
    # still knows which entries are directories.
    git_args = [_split(rel)[0] for rel in rels]
    cmd = [
        "git",
        "-c",
        f"core.excludesFile={excludes_file}",
        "check-ignore",
        "-v",
        "-n",
        "--no-index",
        "--",
        *git_args,
    ]
    result = subprocess.run(cmd, cwd=repo, capture_output=True, text=True, check=False)
    lines = result.stdout.splitlines()
    assert len(lines) == len(rels), f"git gave {len(lines)} lines for {len(rels)} paths: {result.stderr}"
    parsed: dict[str, tuple[str, int, str]] = {}
    for rel, line in zip(rels, lines):
        # Each line is `source:lineno:pattern<TAB>path`. Split off the path first, so
        # a ':' inside the pattern does not confuse the parse.
        left, _path = line.split("\t", 1)
        if left == "::":
            parsed[rel] = ("", 0, "")
            continue
        source, lineno, pattern = left.split(":", 2)
        parsed[rel] = (source, int(lineno), pattern)
    return parsed


def _run_both(tmp_path: Path, files: dict[str, str]) -> dict[str, tuple[Verdict, Verdict]]:
    """
    Build the same tree twice, once with `.ragignore` and once with `.gitignore`.

    Returns, for every probe, (our verdict, git's verdict translated into our terms).
    Git's `.gitignore` source maps onto the `.ragignore` file at the same place.
    """
    rag_root = tmp_path / "rag"
    git_root = tmp_path / "git"
    excludes = _empty_global(tmp_path)

    _build_tree(rag_root, files, RAGIGNORE_FILENAME)
    _build_tree(git_root, files, ".gitignore")
    subprocess.run(["git", "init", "-q", str(git_root)], check=True, capture_output=True)

    matcher = RagIgnoreMatcher(str(rag_root), global_path=excludes)
    ours = _matcher_verdicts(matcher, str(rag_root))
    git = _git_verdicts(git_root, excludes)

    pairs: dict[str, tuple[Verdict, Verdict]] = {}
    for rel in PROBES:
        source, lineno, pattern = git[rel]
        if source == "":
            expected = NO_MATCH
        else:
            # A matched negation is reported with ignored=False, as in our Verdict.
            expected = Verdict(
                not pattern.startswith("!"),
                os.path.join(str(rag_root), source[: -len(".gitignore")] + RAGIGNORE_FILENAME),
                lineno,
                pattern,
            )
        pairs[rel] = (ours[rel], expected)
    return pairs


# ----------------------------------------------------------------------------
# 1. The shipped file is a no-op
# ----------------------------------------------------------------------------


def test_every_nonblank_line_is_a_comment() -> None:
    """Nothing in the template is an active rule: every non-blank line starts with '#'."""
    active = [
        (number, line)
        for number, line in enumerate(_example_lines(), start=1)
        if line.strip() and not line.startswith("#")
    ]
    assert active == [], f"active (non-comment) lines in the template: {active}"


def test_no_line_compiles_to_a_rule() -> None:
    """The matcher's own line compiler yields no rule for any line of the template."""
    compiled = [
        number
        for number, line in enumerate(_example_lines(), start=1)
        if _compile_line(line, number) is not None
    ]
    assert compiled == [], f"lines that compile to a rule: {compiled}"


def test_as_ragignore_the_template_excludes_nothing(tmp_path: Path) -> None:
    """Copied into a tree as .ragignore, the template excludes no probe."""
    root = tmp_path / "rag"
    text = EXAMPLE_PATH.read_text(encoding="utf-8")
    _build_tree(root, {"": text}, RAGIGNORE_FILENAME)
    matcher = RagIgnoreMatcher(str(root), global_path=_empty_global(tmp_path))

    assert _ignored_by_matcher(matcher, str(root)) == frozenset()
    verdicts = _matcher_verdicts(matcher, str(root))
    assert all(verdict == NO_MATCH for verdict in verdicts.values())


def test_control_a_real_star_rule_excludes_everything(tmp_path: Path) -> None:
    """Control for the test above: the same tree with an active '*' rule excludes every probe."""
    root = tmp_path / "rag"
    _build_tree(root, {"": "*\n"}, RAGIGNORE_FILENAME)
    matcher = RagIgnoreMatcher(str(root), global_path=_empty_global(tmp_path))

    assert _ignored_by_matcher(matcher, str(root)) == frozenset(PROBES)


# ----------------------------------------------------------------------------
# 2. Each activated example does what its comment says
# ----------------------------------------------------------------------------


def test_every_example_line_has_a_unit() -> None:
    """Coverage in both directions: the units and the file list the same example lines."""
    in_file = _example_patterns()
    in_units = {line for unit in UNITS for line in unit.examples}
    assert in_units == in_file, (
        f"in the file only: {sorted(in_file - in_units)}; "
        f"in a unit only: {sorted(in_units - in_file)}"
    )


@pytest.mark.parametrize("unit", UNITS, ids=lambda unit: unit.name)
def test_activated_example_excludes_what_its_comment_says(unit: Unit, tmp_path: Path) -> None:
    """Our matcher excludes exactly the probes the unit's comment names."""
    assert unit.ignored <= set(PROBES), f"{unit.name}: expected probe not in PROBES"
    root = tmp_path / "rag"
    _build_tree(root, unit.root_files(), RAGIGNORE_FILENAME)
    matcher = RagIgnoreMatcher(str(root), global_path=_empty_global(tmp_path))

    got = _ignored_by_matcher(matcher, str(root))
    assert got == unit.ignored, (
        f"{unit.name}: excludes {sorted(got)}, comment says {sorted(unit.ignored)}"
    )


@pytest.mark.skipif(not GIT_AVAILABLE, reason="git not on PATH: git cross-check skipped")
@pytest.mark.parametrize("unit", UNITS, ids=lambda unit: unit.name)
def test_activated_example_matches_git_check_ignore(unit: Unit, tmp_path: Path) -> None:
    """For every probe, our verdict (ignored, source, line, pattern) equals git's."""
    pairs = _run_both(tmp_path, unit.root_files())
    mismatches = {rel: pair for rel, pair in pairs.items() if pair[0] != pair[1]}
    assert not mismatches, f"{unit.name}: ours vs git differ, (ours, git): {mismatches}"


@pytest.mark.skipif(not GIT_AVAILABLE, reason="git not on PATH: git cross-check skipped")
def test_pitfall_directory_then_negation_does_not_reinclude(tmp_path: Path) -> None:
    """The wrong pattern the template warns about: 'dir/' then '!dir/keep.md' keeps keep.md excluded."""
    pairs = _run_both(tmp_path, {"": "dir/\n!dir/keep.md\n"})
    ours, git = pairs["dir/keep.md"]
    assert ours.ignored, f"matcher re-included dir/keep.md: {ours}"
    assert git.ignored, f"git re-included dir/keep.md: {git}"
    assert ours == git


# ----------------------------------------------------------------------------
# 3. Not an ignore file for the scanner, and never indexed
# ----------------------------------------------------------------------------


def test_scanner_reads_only_dot_ragignore_and_the_global_file(tmp_path: Path) -> None:
    """A copy named like the template, with an active rule, is not read as an ignore file."""
    assert RAGIGNORE_FILENAME == ".ragignore"
    assert Path(DEFAULT_GLOBAL_PATH).resolve() == (REPO_ROOT / "config" / "ragignore").resolve()
    assert Path(DEFAULT_GLOBAL_PATH).resolve() != EXAMPLE_PATH.resolve()

    root = tmp_path / "rag"
    _build_tree(root, {"": "*\n"}, "ragignore.example")
    matcher = RagIgnoreMatcher(str(root), global_path=_empty_global(tmp_path))

    assert _ignored_by_matcher(matcher, str(root)) == frozenset()


def test_example_extension_is_never_indexed() -> None:
    """'.example' is not on the include whitelist, so the template is never queued."""
    config = yaml.safe_load(RAGNART_YAML.read_text(encoding="utf-8"))
    include_exts = include_extensions_from_config(config)

    assert EXAMPLE_PATH.suffix == ".example"
    assert EXAMPLE_PATH.suffix not in include_exts
    assert not is_includable(str(EXAMPLE_PATH), include_exts)
