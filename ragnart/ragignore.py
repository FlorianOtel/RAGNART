"""
Gitignore-compatible exclusion rules for the NAS crawl (`.ragignore`).

Rules come from two sources, lowest precedence first:

1. The global file ``config/ragignore`` (analogous to git's ``core.excludesFile``).
   Its patterns are matched relative to the crawled root.
2. ``.ragignore`` files inside the crawled tree. A file in directory ``D`` applies
   only to paths under ``D``, and its patterns are matched relative to ``D``,
   exactly as git matches a ``.gitignore`` relative to its own directory.

Precedence follows git:

- Within one file, the last matching pattern wins.
- A file in a deeper directory beats a file in a shallower one.
- The global file is lowest.
- A directory that is excluded is never descended into, so nothing under it can
  be re-included. This is enforced here by resolving an excluded ancestor before
  looking at the path's own rules.

Each gitignore line is translated here into a regular expression, which is matched
against the whole relative path with ``fullmatch``. The translation follows git's
wildmatch rules directly. pathspec's matchers are not used, because they are built
for search, and their directory handling does not reproduce git's results.

Symlinks are matched by the path as the walk presents it. ``realpath`` is never
called.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import NamedTuple

RAGIGNORE_FILENAME = ".ragignore"
"""The name of a per-directory ignore file inside the crawled tree."""

DEFAULT_GLOBAL_PATH = os.path.join(
    # This module lives at <repo>/ragnart/ragignore.py, so the repo is one level up.
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config",
    "ragignore",
)
"""The global rules file, <repo>/config/ragignore."""


class Verdict(NamedTuple):
    """
    The decision for one path, shaped like ``git check-ignore -v`` output.

    ``ignored``: True if the path is excluded.

    ``source_file``: absolute path of the file whose pattern decided the outcome,
    or None if no pattern matched at all.

    ``lineno``: 1-based line number of that pattern in ``source_file``, or None.

    ``pattern``: the pattern line from ``source_file``, exactly as git reports it
    (trailing unescaped spaces stripped), or None.

    A matched negation (``!``) gives ``ignored=False`` with its source and line
    filled in. This matches ``git check-ignore -v``, which also reports negations.
    """

    ignored: bool
    source_file: str | None
    lineno: int | None
    pattern: str | None


NO_MATCH = Verdict(False, None, None, None)
"""The verdict for a path that no pattern mentions."""


# ----------------------------------------------------------------------------
# Translating one gitignore line into a regular expression
# ----------------------------------------------------------------------------


def _trim_trailing_spaces(text: str) -> str:
    """
    Remove trailing spaces, except a space that is escaped with a backslash.

    This follows git's trim_trailing_spaces. A backslash escapes the character
    after it, so an escaped space ("\\ ") survives, and so does any space before
    it.
    """
    keep = 0  # length of the prefix to keep so far
    i = 0
    while i < len(text):
        if text[i] == "\\" and i + 1 < len(text):
            # The escaped character is kept whatever it is, space included.
            i += 2
            keep = i
            continue
        if text[i] != " ":
            keep = i + 1
        i += 1
    return text[:keep]


# POSIX character classes that git's wildmatch accepts inside a bracket, as in
# "[[:alpha:]]". Each value is the body of a regex bracket expression, so it can be
# pasted straight into the items list. Git uses the C locale, so a class only ever
# contains ASCII characters. The exact sets were cross-checked against git
# check-ignore, one character at a time, in tests/test_ragignore.py.
_POSIX_CLASSES: dict[str, str] = {
    "alnum": "a-zA-Z0-9",
    "alpha": "a-zA-Z",
    "blank": " \\t",
    "cntrl": "\\x01-\\x1f\\x7f",
    "digit": "0-9",
    "graph": "\\x21-\\x7e",
    "lower": "a-z",
    "print": "\\x20-\\x7e",
    "punct": "\\x21-\\x2f\\x3a-\\x40\\x5b-\\x60\\x7b-\\x7e",
    "space": " \\t\\n\\r",
    "upper": "A-Z",
    "xdigit": "0-9a-fA-F",
}


def _bracket_to_regex(glob: str, start: int) -> tuple[str, int] | None:
    """
    Translate a bracket expression such as ``[a-c]``, ``[!x]`` or ``[[:digit:]]``.

    ``start`` is the index of the opening '['. Returns the regex and the index
    just past the closing ']', or None if the bracket is never closed. The caller
    then aborts the whole pattern, as git does.

    Raises ValueError for an unknown POSIX class name, or for a "[:" with no ']'
    after it. Git's wildmatch aborts the whole pattern in both cases, so the
    caller makes the line match nothing.
    """
    i = start + 1
    negated = False
    if i < len(glob) and glob[i] in "!^":
        negated = True
        i += 1
    items: list[str] = []
    first = True  # a ']' right after the opening '[' is a literal, not the end
    while i < len(glob):
        ch = glob[i]
        if ch == "]" and not first:
            # A bracket never matches '/', because '/' separates path segments.
            # The (?!/) guard enforces this for negated classes too.
            prefix = "(?!/)[^" if negated else "(?!/)["
            return prefix + "".join(items) + "]", i + 1
        first = False
        if ch == "[" and glob.startswith(":", i + 1):
            # A POSIX class, "[:name:]", inside the bracket. Git looks for the first
            # ']' after the "[:" and accepts the class only if the text before that
            # ']' ends with ':'. Otherwise the '[' is an ordinary character.
            close = glob.find("]", i + 2)
            if close == -1:
                raise ValueError("'[:' with no closing ']'")
            body = glob[i + 2 : close]  # text between "[:" and "]", including the ':'
            if body.endswith(":"):
                name = body[:-1]
                if name not in _POSIX_CLASSES:
                    raise ValueError(f"unknown POSIX class {name!r}")
                items.append(_POSIX_CLASSES[name])
                i = close + 1
                continue
            # Not a class: fall through and treat the '[' as a literal below.
        if ch == "\\" and i + 1 < len(glob):
            items.append(re.escape(glob[i + 1]))
            i += 2
        elif ch == "-":
            # A range such as a-z: keep the hyphen so the regex sees a range.
            items.append("-")
            i += 1
        else:
            items.append(re.escape(ch))
            i += 1
    return None


def _glob_to_regex(glob: str) -> str:
    """
    Translate a gitignore glob into a regex body, to be used with fullmatch.

    The glob has no leading '!' and no trailing '/'. It may contain '/'.

    Rules, as in git's wildmatch with pathname matching:

    - '*' matches any run of characters except '/'.
    - '?' matches one character except '/'.
    - '[...]' is a bracket expression. It never matches '/'. An unclosed '[' makes
      the whole pattern invalid, and ValueError is raised.
    - '**' as a whole path segment has special meaning:
        - "**/" at the start or the middle matches zero or more directories.
        - "/**" at the end matches everything below that point.
    - '**' anywhere else acts like '*'.
    - A backslash makes the next character literal.
    """
    parts: list[str] = []
    i = 0
    n = len(glob)
    while i < n:
        ch = glob[i]
        if ch == "*":
            is_whole_segment_double_star = (
                glob.startswith("**", i)
                and (i == 0 or glob[i - 1] == "/")
                and (i + 2 == n or glob[i + 2] == "/")
            )
            if is_whole_segment_double_star and i + 2 == n:
                # Trailing "/**", or a lone "**": everything below this point.
                parts.append(".*")
                i += 2
            elif is_whole_segment_double_star:
                # "**/": zero or more directories. The slash is consumed here, and
                # the literal '/' before it was already emitted.
                parts.append("(?:.*/)?")
                i += 3
            else:
                # A run of stars that is not a whole "**" segment is one '*'.
                while i < n and glob[i] == "*":
                    i += 1
                parts.append("[^/]*")
        elif ch == "?":
            parts.append("[^/]")
            i += 1
        elif ch == "[":
            bracket = _bracket_to_regex(glob, i)
            if bracket is None:
                # An unclosed '[' is not a literal. git's wildmatch aborts the whole
                # pattern on it, so the pattern matches nothing. The raise makes
                # _compile_line drop the line.
                raise ValueError("'[' with no closing ']'")
            else:
                regex, i = bracket
                parts.append(regex)
        elif ch == "\\":
            if i + 1 >= n:
                # A trailing backslash escapes nothing. git's wildmatch aborts on
                # it, so the whole line matches nothing. The caller handles this.
                raise ValueError("pattern ends with a backslash")
            parts.append(re.escape(glob[i + 1]))
            i += 2
        else:
            parts.append(re.escape(ch))
            i += 1
    return "".join(parts)


@dataclass(frozen=True)
class _Rule:
    """One effective pattern line from an ignore file."""

    # True for an exclusion pattern, False for a `!` negation.
    include: bool
    # True for a pattern ending in '/', which applies to directories only.
    dir_only: bool
    # Matches the relative path, from `fullmatch`. Directories are matched by their
    # bare path. A directory-only rule is only ever tried on directories.
    regex: re.Pattern
    # 1-based line number in the file the rule came from.
    lineno: int
    # The line as git reports it: without its line terminator, and with trailing
    # unescaped spaces stripped.
    text: str


def _compile_line(text: str, lineno: int) -> _Rule | None:
    """
    Compile one line of an ignore file, or return None if it matches nothing.

    Blank lines, '#' comments, and lines that reduce to nothing are inert.
    Their results are None, the same as git.
    """
    # Comments and blank lines are checked on the raw line, as git does. An escaped
    # hash, "\#", starts with a backslash and so is not a comment.
    if text == "" or text.startswith("#"):
        return None

    # Git strips trailing unescaped spaces from the line before anything else, and
    # reports the stripped line as the pattern. The stored text is that stripped line.
    trimmed = _trim_trailing_spaces(text)
    body = trimmed
    include = True
    if body.startswith("!"):
        include = False
        body = body[1:]

    dir_only = body.endswith("/")
    if dir_only:
        body = body[:-1]

    # A slash anywhere except the trailing one anchors the pattern to its directory.
    anchored = "/" in body
    if body.startswith("/"):
        body = body[1:]
    if body == "":
        return None

    try:
        glob_regex = _glob_to_regex(body)
    except ValueError:
        # git aborts on this pattern (for example an unclosed '['), so it matches
        # nothing. The line is dropped, the same as a blank line.
        return None

    # An unanchored pattern matches the basename at any depth below its directory.
    # The optional prefix "(?:.*/)?" covers all directories above that basename.
    regex_text = glob_regex if anchored else "(?:.*/)?" + glob_regex
    return _Rule(
        include=include,
        dir_only=dir_only,
        regex=re.compile(regex_text),
        lineno=lineno,
        text=trimmed,
    )


# ----------------------------------------------------------------------------
# Ignore files and the matcher
# ----------------------------------------------------------------------------


class _IgnoreFile:
    """The effective rules of one ignore file, kept in file order."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._rules: list[_Rule] = []
        # surrogateescape lets a non-UTF-8 byte in a rules file pass through
        # instead of aborting the crawl.
        with open(path, encoding="utf-8", errors="surrogateescape") as handle:
            for lineno, raw in enumerate(handle, start=1):
                rule = _compile_line(raw.rstrip("\r\n"), lineno)
                if rule is not None:
                    self._rules.append(rule)

    def last_match(self, rel_path: str, is_dir: bool) -> _Rule | None:
        """
        Return the last rule in this file that matches the path, or None.

        ``rel_path`` is relative to this file's directory, with '/' separators
        and no trailing slash. ``is_dir`` says whether the path is a directory.
        The result may be a negation, in which case the returned rule has
        ``include=False``.
        """
        # Scan from the end, because the last matching pattern wins.
        for rule in reversed(self._rules):
            if rule.dir_only and not is_dir:
                continue
            if rule.regex.fullmatch(rel_path) is not None:
                return rule
        return None


class RagIgnoreMatcher:
    """
    Decides whether paths under one crawl root are excluded.

    Build one matcher per root. Paths passed in must lie below ``root``. A path
    is never realpath'd; it is matched exactly as given.

    Use ``is_dir_excluded`` for directories during an ``os.walk`` pruning step.
    Use ``is_path_excluded`` for non-directory paths. Use ``explain`` to get the
    deciding pattern for logging.
    """

    def __init__(self, root: str, global_path: str = DEFAULT_GLOBAL_PATH) -> None:
        self.root = os.path.normpath(root)
        # The global file is parsed eagerly. A missing file raises
        # FileNotFoundError here, instead of silently dropping the global
        # exclusions.
        self._global = _IgnoreFile(os.path.abspath(global_path))
        # Caches keep a large NAS crawl from re-reading the same ignore file or
        # re-resolving the same directory for every file under it.
        self._ignore_files: dict[str, _IgnoreFile | None] = {}
        self._dir_verdicts: dict[str, Verdict] = {}

    # ------------------------------------------------------------------
    # Public surface
    # ------------------------------------------------------------------

    def is_dir_excluded(self, dirpath: str, dirname: str) -> bool:
        """
        True if the directory ``dirpath/dirname`` is excluded.

        This is also True when an ancestor below the root is excluded. The walk
        must not descend into an excluded directory, so its own ``.ragignore``
        is never read and cannot re-include anything under it.
        """
        return self.explain(os.path.join(dirpath, dirname), is_dir=True).ignored

    def is_path_excluded(self, fullpath: str) -> bool:
        """True if the non-directory ``fullpath`` is excluded."""
        return self.explain(fullpath, is_dir=False).ignored

    def explain(self, fullpath: str, is_dir: bool = False) -> Verdict:
        """
        Return the full decision for ``fullpath``, with its deciding pattern.

        When an excluded ancestor decides the outcome, that ancestor's pattern
        is returned, as ``git check-ignore -v`` does.
        """
        return self._resolve(self._normalise(fullpath), is_dir)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _normalise(self, fullpath: str) -> str:
        """Normalise the path textually and check that it lies below the root."""
        path = os.path.normpath(fullpath)
        rel = os.path.relpath(path, self.root)
        if rel == "." or rel == ".." or rel.startswith(".." + os.sep):
            raise ValueError(f"{fullpath!r} is not below the crawl root {self.root!r}")
        return path

    def _resolve(self, path: str, is_dir: bool) -> Verdict:
        """Decide one normalised path. Excluded ancestors take priority."""
        parent = os.path.dirname(path)
        if parent != self.root:
            # Git never consults the rules for a path whose parent directory is
            # excluded. An excluded ancestor therefore decides the outcome before
            # this path's own rules are looked at.
            parent_verdict = self._dir_verdict(parent)
            if parent_verdict.ignored:
                return parent_verdict
        verdict = self._last_match(path, is_dir, parent)
        return verdict if verdict is not None else NO_MATCH

    def _dir_verdict(self, dirpath: str) -> Verdict:
        """Return the cached verdict for a directory, resolving it on first use."""
        cached = self._dir_verdicts.get(dirpath)
        if cached is None:
            cached = self._resolve(dirpath, is_dir=True)
            self._dir_verdicts[dirpath] = cached
        return cached

    def _last_match(self, path: str, is_dir: bool, parent: str) -> Verdict | None:
        """Find the last matching rule across all applicable files."""
        # The rule files that can apply to `path` sit in its ancestor directories,
        # from root down to `parent`. Go deepest first, so a closer file beats a
        # file further up.
        for base in reversed(self._directories_from_root_to(parent)):
            ignore_file = self._ignore_file_in(base)
            if ignore_file is None:
                continue
            rule = ignore_file.last_match(self._relative_to(path, base), is_dir)
            if rule is not None:
                return Verdict(rule.include, ignore_file.path, rule.lineno, rule.text)

        # The global file is the lowest precedence, so it is checked last.
        rule = self._global.last_match(self._relative_to(path, self.root), is_dir)
        if rule is not None:
            return Verdict(rule.include, self._global.path, rule.lineno, rule.text)
        return None

    def _directories_from_root_to(self, directory: str) -> list[str]:
        """Return every directory from the root down to `directory`, both included."""
        chain = [self.root]
        rel = os.path.relpath(directory, self.root)
        if rel != ".":
            current = self.root
            for part in rel.split(os.sep):
                current = os.path.join(current, part)
                chain.append(current)
        return chain

    def _ignore_file_in(self, base: str) -> _IgnoreFile | None:
        """Return the parsed `.ragignore` in `base`, or None if there is none."""
        if base not in self._ignore_files:
            candidate = os.path.join(base, RAGIGNORE_FILENAME)
            self._ignore_files[base] = _IgnoreFile(candidate) if os.path.isfile(candidate) else None
        return self._ignore_files[base]

    @staticmethod
    def _relative_to(path: str, base: str) -> str:
        """Express `path` relative to `base`, with '/' separators and no trailing slash."""
        return os.path.relpath(path, base).replace(os.sep, "/")
