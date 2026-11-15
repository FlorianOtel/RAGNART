"""
Read-only command-line interface for RAGNART.

Run it from the repository checkout as `python -m ragnart <command>`. Once the
package is installed, `ragnart <command>` runs the same code (pyproject.toml
[project.scripts]).

Every command is read-only. None of them writes to the queue database, to Qdrant
or to the NAS. The queue is opened through a `mode=ro` URI (StateDB(read_only=True)),
and the NAS is only listed and read for its metadata.

Commands, each named after the git command it mirrors:
  check-ignore  explain which .ragignore rule excludes each path   (git check-ignore)
  ls-files      list indexed, ignored, untracked or queued files   (git ls-files)
  status        queue counts and Qdrant point counts               (git status)

Exit codes follow git's conventions:
  0    success. For check-ignore: at least one path matched a rule.
  1    check-ignore only: no path matched a rule.
  128  fatal error: unreadable config, path outside every root, unopenable database.
  2    usage error, reported by argparse. Git uses 129 here; this CLI does not.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections.abc import Iterator
from pathlib import Path

import yaml

from .ragignore import RagIgnoreMatcher, Verdict
from .scanner import include_extensions_from_config, is_includable, nfs_roots_from_config
from .state import StateDB
from .status import print_status

# This module lives at <repo>/ragnart/cli.py, so the repo root is one level up.
REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "ragnart.yaml"

# Defaults mirror utils/rag_status.py, which reads the same config keys.
_DEFAULT_DB_BASE = "/mnt/nfs/__Backups/RAGNART--databases"
_DEFAULT_QDRANT_URL = "http://192.168.1.93:6333"

EXIT_OK = 0
EXIT_NO_MATCH = 1
EXIT_FATAL = 128


class FatalError(Exception):
    """A failure that makes a command stop with git's fatal exit code (128)."""


# ---------------------------------------------------------------------------
# Config, database and root helpers
# ---------------------------------------------------------------------------

def load_config() -> dict:
    """
    Load <repo>/ragnart.yaml. Read at call time, so tests can point CONFIG_PATH elsewhere.
    """
    try:
        with open(CONFIG_PATH) as f:
            return yaml.safe_load(f)
    except (OSError, yaml.YAMLError) as exc:
        raise FatalError(f"cannot read config {CONFIG_PATH}: {exc}") from exc


def state_db_path(config: dict) -> str:
    """Return the queue database path, as the operator script builds it."""
    base = config.get("db_base_path", _DEFAULT_DB_BASE)
    return f"{base}/sqlite/rag_state.db"


def open_queue_read_only(config: dict) -> StateDB:
    """
    Open the queue database read-only. A missing or unreadable file is fatal.

    The file is never created here, unlike a normal StateDB open.
    """
    path = state_db_path(config)
    try:
        return StateDB(path, read_only=True)
    except sqlite3.Error as exc:
        raise FatalError(f"cannot open queue database read-only: {path}: {exc}") from exc


def build_matchers(config: dict) -> dict[str, RagIgnoreMatcher]:
    """
    Return one RagIgnoreMatcher per configured root, keyed by the normalised root path.

    Insertion order is the config order (see configured_roots).
    """
    matchers: dict[str, RagIgnoreMatcher] = {}
    for normalised in configured_roots(config):
        try:
            matchers[normalised] = RagIgnoreMatcher(normalised)
        except OSError as exc:
            # The global config/ragignore is read eagerly; a missing file is fatal.
            raise FatalError(f"cannot read ignore rules for {normalised}: {exc}") from exc
    return matchers


def configured_roots(config: dict) -> list[str]:
    """
    Return the normalised NFS root paths from the config, in config order, without duplicates.

    Roots and owners come from nfs_roots_from_config, the same helper scan_nfs_roots uses.
    """
    roots: list[str] = []
    for root, _owner in nfs_roots_from_config(config):
        normalised = os.path.normpath(root)
        if normalised not in roots:
            roots.append(normalised)
    return roots


def containing_root(path: str, roots: list[str]) -> str | None:
    """
    Return the configured root that is `path` or contains it, or None if there is none.

    `path` must be absolute and normalised. When several roots contain it, the longest
    one wins, because it is the most specific.
    """
    containing = [r for r in roots if path == r or path.startswith(r + os.sep)]
    return max(containing, key=len) if containing else None


def logical_cwd() -> str:
    """
    Return the working directory as the shell spells it, following the `pwd -L` rule.

    os.getcwd() always returns the physical path, with symlinks resolved. When the
    operator entered the directory through a symlink, the shell keeps the logical
    spelling in $PWD. That spelling is used only when it is absolute and really is the
    current directory (same inode as "."). A stale or missing $PWD falls back to
    os.getcwd().
    """
    pwd = os.environ.get("PWD")
    if pwd and os.path.isabs(pwd):
        try:
            if os.path.samefile(pwd, "."):
                return pwd
        except OSError:
            # $PWD names a path that cannot be stat'ed; it is not trusted.
            pass
    return os.getcwd()


def resolve_path(given: str, roots: list[str]) -> str:
    """
    Return `given` as the NAS walk spells it, under the configured root's own spelling.

    A relative path is joined with the logical working directory (see logical_cwd), not
    the physical one, so `cd` through a symlink into a root keeps the root's logical
    spelling. The result is normalised lexically with os.path.normpath, not with
    os.path.realpath. This is deliberate: `..` is resolved against the logical path,
    as the shell does, so `L/..` means the parent of the symlink L, not the parent of
    its target. An absolute path is only normalised, so it is returned unchanged.

    When the result lies under a root it is returned as it is. When it does not, the
    path is resolved with os.path.realpath and mapped back onto the root's configured
    spelling. A path that is outside every root after both tries is fatal.
    """
    if os.path.isabs(given):
        absolute = os.path.normpath(given)
    else:
        absolute = os.path.normpath(os.path.join(logical_cwd(), given))
    if containing_root(absolute, roots) is not None:
        return absolute

    real = os.path.realpath(given)
    # Among the roots that contain the resolved path, the most specific one is used.
    # Each root is compared in its own resolved form, because the root may be a symlink.
    best: tuple[str, str] | None = None  # (configured root, its resolved form)
    for root in roots:
        real_root = os.path.realpath(root)
        if real == real_root or real.startswith(real_root + os.sep):
            if best is None or len(real_root) > len(best[1]):
                best = (root, real_root)
    if best is None:
        raise FatalError(f"{given}: is outside every configured root")
    root, real_root = best
    return root + real[len(real_root):]


def root_for(absolute: str, roots: list[str]) -> str:
    """
    Return the configured root that contains `absolute`.

    `absolute` must be absolute and normalised, as resolve_path returns it. A path that
    is itself a root is refused, since it is not a path inside one.
    """
    if absolute in roots:
        raise FatalError(f"{absolute}: is a configured root, not a path inside one")
    root = containing_root(absolute, roots)
    if root is None:
        raise FatalError(f"{absolute}: is outside every configured root")
    return root


def _walk_files(
    start: str,
    matcher: RagIgnoreMatcher,
    descend_excluded: bool,
    visited_real_dirs: set[str],
) -> Iterator[str]:
    """
    Yield every file path under the directory `start`, in os.walk order.

    Directories are pruned as walk_includable describes. `visited_real_dirs` is shared
    by the caller, so a real directory is walked only once across all targets.
    """
    for dirpath, dirnames, filenames in os.walk(start, topdown=True, followlinks=True):
        # Directory dedupe: a real directory reached twice is walked once.
        real_dirpath = os.path.realpath(dirpath)
        if real_dirpath in visited_real_dirs:
            dirnames.clear()
            continue
        visited_real_dirs.add(real_dirpath)

        if not descend_excluded:
            dirnames[:] = [
                d for d in dirnames if not matcher.is_dir_excluded(dirpath, d)
            ]

        for filename in filenames:
            yield os.path.join(dirpath, filename)


def walk_includable(
    matchers: dict[str, RagIgnoreMatcher],
    include_exts: frozenset[str],
    descend_excluded: bool,
    targets: list[str] | None = None,
) -> Iterator[tuple[str, Verdict]]:
    """
    Yield (path, verdict) for every includable file under the configured roots, or under `targets`.

    `targets` are walked paths as resolve_path returns them: a root, a directory under
    one, or a file. Each target is walked with the matcher of the root that contains it.
    The matcher reads every .ragignore from that root down to the target, and it treats
    an excluded ancestor as deciding every path below it, so a subtree walk gives the
    same verdicts as a full walk. When `targets` is None, every configured root is walked.

    The walk is read-only and mirrors scan_nfs_roots: it follows symlinks, skips a
    real directory or file it has already seen, and warns about roots that are not
    directories. Unlike the scanner it does not write anything.

    When `descend_excluded` is False, excluded directories are pruned, so their files
    are not yielded. When True, the walk goes into them, and each file's verdict then
    names the rule that excludes it. Files under an excluded directory come back with
    ignored=True.
    """
    roots = list(matchers)
    if targets is None:
        scopes = [(root, root) for root in roots]
    else:
        scopes = [(containing_root(target, roots), target) for target in targets]

    visited_real_dirs: set[str] = set()
    visited_real_files: set[str] = set()
    # Targets can overlap, for example "." and "sub". Each path is yielded once.
    emitted: set[str] = set()

    for root, start in scopes:
        if not os.path.isdir(root):
            print(f"ragnart: warning: NFS root not accessible, skipping: {root}", file=sys.stderr)
            continue
        matcher = matchers[root]

        if os.path.isfile(start):
            candidates = [start]
        else:
            # A missing target yields nothing, as git ls-files does for a pathspec that matches nothing.
            candidates = _walk_files(start, matcher, descend_excluded, visited_real_dirs)

        for filepath in candidates:
            if filepath in emitted or not is_includable(filepath, include_exts):
                continue
            verdict = matcher.explain(filepath)
            # As in the scanner, only admitted files take part in the file dedupe.
            # An excluded file is yielded without being recorded, so a later
            # admitted path to the same real file is not lost.
            if not verdict.ignored:
                real_filepath = os.path.realpath(filepath)
                if real_filepath in visited_real_files:
                    continue
                visited_real_files.add(real_filepath)
            emitted.add(filepath)
            yield filepath, verdict


def in_scope(file_path: str, targets: list[str] | None) -> bool:
    """
    True if a queue row's path is one of `targets`, or lies under a target directory.

    With no targets every row is in scope. The comparison is exact on the path or on
    the path plus a separator, so "notes.txt" does not match "notes.txt.bak".
    """
    if targets is None:
        return True
    return any(file_path == t or file_path.startswith(t + os.sep) for t in targets)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_check_ignore(args: argparse.Namespace) -> int:
    """Explain, for each path, the rule that excludes it (git check-ignore)."""
    # Git's checks come first: it refuses these before looking at any path.
    if not args.paths:
        raise FatalError("no path specified")
    if args.non_matching and not args.verbose:
        raise FatalError("--non-matching is only valid with --verbose")

    config = load_config()
    matchers = build_matchers(config)
    roots = list(matchers)

    # Resolve every path before printing anything. A fatal error on any path then
    # leaves stdout empty, as git does.
    decisions: list[tuple[str, Verdict]] = []
    for given in args.paths:
        # The same resolver as ls-files, so a symlinked spelling such as ~/Gin-AI resolves too.
        absolute = resolve_path(given, roots)
        root = root_for(absolute, roots)
        # A directory is checked as a directory, so a `dir/` rule matches it.
        verdict = matchers[root].explain(absolute, is_dir=os.path.isdir(absolute))
        decisions.append((given, verdict))

    # Git's exit status is 0 when at least one path matched a rule. For -v this
    # includes a matched negation (`!pattern`), which is not an exclusion.
    any_match = False
    for given, verdict in decisions:
        if args.verbose:
            if verdict.source_file is not None:
                any_match = True
                print(f"{verdict.source_file}:{verdict.lineno}:{verdict.pattern}\t{given}")
            elif args.non_matching:
                # Git's marker for a path that no rule mentions.
                print(f"::\t{given}")
        elif verdict.ignored:
            any_match = True
            print(given)

    return EXIT_OK if any_match else EXIT_NO_MATCH


def cmd_ls_files(args: argparse.Namespace) -> int:
    """List files, as git ls-files does. The default lists the indexed files."""
    config = load_config()
    roots = configured_roots(config)

    # Resolve every path before reading anything, so a path outside every root is
    # fatal with nothing printed, as in check-ignore. No paths means no limit.
    targets: list[str] | None = None
    if args.paths:
        targets = [resolve_path(given, roots) for given in args.paths]

    if args.ignored or args.others:
        # These two walk the NAS, so they need the rules and the inclusion whitelist.
        matchers = build_matchers(config)
        include_exts = include_extensions_from_config(config)

    if args.ignored:
        # Includable on-disk files that a .ragignore rule excludes. The walk goes
        # into excluded directories, so their files are listed too.
        for path in sorted(
            p for p, v in walk_includable(matchers, include_exts, True, targets) if v.ignored
        ):
            print(path)
        return EXIT_OK

    db = open_queue_read_only(config)
    try:
        if args.others:
            # Includable, not excluded, on disk, and absent from the queue entirely.
            # Pruning excluded directories is safe, because nothing under them is
            # wanted here.
            queued = {row["file_path"] for row in db.list_rows()}
            for path in sorted(
                p for p, v in walk_includable(matchers, include_exts, False, targets)
                if not v.ignored and p not in queued
            ):
                print(path)
        elif args.stage:
            # Every queue row, with its status: the analogue of the index state.
            for row in db.list_rows():
                if in_scope(row["file_path"], targets):
                    print(f"{row['status']}\t{row['file_path']}")
        else:
            # The default: indexed files, which are the completed queue rows.
            for row in db.list_rows(status="completed"):
                if in_scope(row["file_path"], targets):
                    print(row["file_path"])
    finally:
        db.close()
    return EXIT_OK


def cmd_status(args: argparse.Namespace) -> int:
    """Show queue and Qdrant counts, as git status does for the working tree."""
    config = load_config()
    qdrant_url = config.get("rag", {}).get("qdrant_url", _DEFAULT_QDRANT_URL)
    db = open_queue_read_only(config)
    try:
        print_status(db, qdrant_url=qdrant_url, user=args.user, show_ignored=args.ignored)
    finally:
        db.close()
    return EXIT_OK


# ---------------------------------------------------------------------------
# Argument parsing and entry point
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with one subcommand per command."""
    parser = argparse.ArgumentParser(
        prog="ragnart",
        description="Read-only inspection of the RAGNART NAS corpus. Nothing is written.",
    )
    sub = parser.add_subparsers(dest="command", required=True, metavar="<command>")

    check = sub.add_parser(
        "check-ignore",
        help="explain which .ragignore rule excludes each path (mirrors git check-ignore)",
        description="Show which rule excludes each path. Without -v, print only excluded paths.",
    )
    check.add_argument(
        "-v", "--verbose", action="store_true",
        help="print source:line:pattern<TAB>path for each path a rule matches",
    )
    check.add_argument(
        "-n", "--non-matching", action="store_true",
        help="with -v, also print paths no rule matches, as ::<TAB>path",
    )
    check.add_argument("paths", nargs="*", metavar="path", help="paths to check, absolute or relative to cwd")
    check.set_defaults(handler=cmd_check_ignore)

    ls = sub.add_parser(
        "ls-files",
        help="list indexed files by default, or --ignored/--others/--stage, under optional paths (mirrors git ls-files)",
        description=(
            "With no flag, list indexed files (completed queue rows). "
            "--ignored lists on-disk files a .ragignore rule excludes. "
            "--others lists on-disk files that are neither excluded nor queued. "
            "--stage lists every queue row as status<TAB>path. "
            "With <path> arguments, only files under those paths are listed; a path may be "
            "a directory or a file, absolute or relative to cwd. A path outside every "
            "configured root is fatal (exit 128). Like git ls-files, a path that matches "
            "nothing prints nothing and is not an error."
        ),
    )
    modes = ls.add_mutually_exclusive_group()
    modes.add_argument("--ignored", action="store_true", help="on-disk includable files excluded by a rule")
    modes.add_argument("--others", action="store_true", help="on-disk includable files not excluded and not queued")
    modes.add_argument("--stage", action="store_true", help="every queue row with its status")
    ls.add_argument(
        "paths", nargs="*", metavar="<path>",
        help="limit the listing to files under these paths (a directory or a file), absolute or relative to cwd",
    )
    ls.set_defaults(handler=cmd_ls_files)

    status = sub.add_parser(
        "status",
        help="queue counts and Qdrant point counts (mirrors git status)",
        description="Show queue counts and Qdrant point counts. Same display as utils/rag_status.py.",
    )
    status.add_argument("--user", metavar="OWNER", help="filter counts by owner (e.g. florian)")
    status.add_argument("--ignored", action="store_true", help="also list ignored files with retry count and reason")
    status.set_defaults(handler=cmd_status)

    return parser


def main(argv: list[str] | None = None) -> int:
    """
    Run one command and return its exit code. The console script passes this to sys.exit.

    Fatal errors are reported on stderr and return 128, so the caller never sees a traceback
    for an expected failure.
    """
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except FatalError as exc:
        print(f"ragnart: fatal: {exc}", file=sys.stderr)
        return EXIT_FATAL
    except ValueError as exc:
        # Raised by scanner.include_extensions_from_config for a stale or missing key.
        print(f"ragnart: fatal: config: {exc}", file=sys.stderr)
        return EXIT_FATAL
    except sqlite3.Error as exc:
        print(f"ragnart: fatal: queue database: {exc}", file=sys.stderr)
        return EXIT_FATAL
    except BrokenPipeError:
        # The reader went away, as with `ragnart ls-files | head`. Point stdout at
        # devnull so the interpreter's final flush does not print a second error.
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return EXIT_OK
