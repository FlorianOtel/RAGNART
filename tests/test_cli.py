"""
Tests for the read-only `ragnart` CLI (ragnart/cli.py) and `python -m ragnart`.

Every test works on a temporary NAS tree, a temporary config and a temporary SQLite
database. Qdrant is replaced by a fake for the status tests, so no live service is
touched. The real config/ragignore is used, because it is part of the repo.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

# tests/test_cli.py -> parent is tests/, parent.parent is the repo root.
REPO_ROOT = Path(__file__).resolve().parent.parent

from ragnart import cli
from ragnart import status as status_module
from ragnart.state import StateDB


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def env(tmp_path, monkeypatch):
    """
    A NAS root with a .ragignore, some files, a temp queue database and a temp config.

    The .ragignore file has a comment on line 1, then these rules:
      line 2: *.log      (excludes every log file)
      line 3: !keep.log  (re-includes keep.log)
      line 4: build/     (excludes the build directory and everything under it)

    Files on disk: notes.txt, doc.md, secret.log, keep.log, build/out.txt
    """
    nas = tmp_path / "nas"
    (nas / "build").mkdir(parents=True)
    (nas / ".ragignore").write_text("# test rules\n*.log\n!keep.log\nbuild/\n")
    for name in ("notes.txt", "doc.md", "secret.log", "keep.log", "build/out.txt"):
        (nas / name).write_text("x")

    db_base = tmp_path / "db"
    config = {
        "db_base_path": str(db_base),
        "rag": {
            "qdrant_url": "http://127.0.0.1:9",
            "scanner": {"include_extensions": [".txt", ".md", ".log"]},
        },
        "users": {"someone@example.com": {"owner": "florian", "nfs_roots": [str(nas)]}},
        "shared": {"owner": "la-familia", "nfs_roots": []},
    }
    config_path = tmp_path / "ragnart.yaml"
    config_path.write_text(yaml.safe_dump(config))
    monkeypatch.setattr(cli, "CONFIG_PATH", config_path)

    db_path = db_base / "sqlite" / "rag_state.db"
    return SimpleNamespace(nas=nas, db_path=db_path, tmp=tmp_path)


def _seed_queue(db_path, rows: dict[str, str]) -> None:
    """Create the queue database with one row per (file_path -> status) entry."""
    db = StateDB(str(db_path))
    for file_path, status in rows.items():
        db.discover_or_update(file_path, "florian", 1.0)
        if status == "completed":
            db.mark_completed(file_path)
        elif status == "ignored":
            db.mark_ignored(file_path, "test reason")
        elif status == "processing":
            db.mark_processing(file_path)
        # 'pending' is the default state after discover_or_update().
    db.close()


def _run(capsys, *argv: str) -> tuple[int, str, str]:
    """Run the CLI in-process and return (exit code, stdout, stderr)."""
    rc = cli.main(list(argv))
    captured = capsys.readouterr()
    return rc, captured.out, captured.err


# ---------------------------------------------------------------------------
# check-ignore
# ---------------------------------------------------------------------------

def test_check_ignore_without_verbose_prints_only_excluded_paths(env, capsys):
    secret = str(env.nas / "secret.log")
    notes = str(env.nas / "notes.txt")

    rc, out, _ = _run(capsys, "check-ignore", secret, notes)

    assert rc == 0
    assert out == secret + "\n"


def test_check_ignore_verbose_prints_absolute_source_line_and_pattern(env, capsys):
    secret = str(env.nas / "secret.log")
    rules = os.path.join(str(env.nas), ".ragignore")

    rc, out, _ = _run(capsys, "check-ignore", "-v", secret)

    assert rc == 0
    assert out == f"{rules}:2:*.log\t{secret}\n"


def test_check_ignore_negation_is_reported_by_verbose_but_not_as_excluded(env, capsys):
    keep = str(env.nas / "keep.log")
    rules = os.path.join(str(env.nas), ".ragignore")

    # Without -v a negated path is not an exclusion, so nothing is printed and exit is 1.
    rc_plain, out_plain, _ = _run(capsys, "check-ignore", keep)
    assert (rc_plain, out_plain) == (1, "")

    # With -v the negation is shown, as git shows it. Git also exits 0 for it.
    rc_verbose, out_verbose, _ = _run(capsys, "check-ignore", "-v", keep)
    assert rc_verbose == 0
    assert out_verbose == f"{rules}:3:!keep.log\t{keep}\n"


def test_check_ignore_no_match_exits_one_with_no_output(env, capsys):
    notes = str(env.nas / "notes.txt")

    rc, out, _ = _run(capsys, "check-ignore", "-v", notes)

    assert (rc, out) == (1, "")


def test_check_ignore_non_matching_marker_with_verbose_and_non_matching(env, capsys):
    notes = str(env.nas / "notes.txt")
    secret = str(env.nas / "secret.log")
    rules = os.path.join(str(env.nas), ".ragignore")

    rc, out, _ = _run(capsys, "check-ignore", "-v", "-n", notes, secret)

    # Lines are in argument order. A path no rule mentions gets git's '::' marker.
    assert out == f"::\t{notes}\n{rules}:2:*.log\t{secret}\n"
    assert rc == 0


def test_check_ignore_non_matching_alone_exits_one(env, capsys):
    # Git's '::' lines do not count as a match, so the exit code stays 1.
    notes = str(env.nas / "notes.txt")

    rc, out, _ = _run(capsys, "check-ignore", "-v", "-n", notes)

    assert rc == 1
    assert out == f"::\t{notes}\n"


def test_check_ignore_directory_is_checked_as_a_directory(env, capsys):
    build = str(env.nas / "build")
    rules = os.path.join(str(env.nas), ".ragignore")

    rc, out, _ = _run(capsys, "check-ignore", "-v", build)

    # The `build/` rule has a trailing slash, so it matches only because build is a directory.
    assert rc == 0
    assert out == f"{rules}:4:build/\t{build}\n"


def test_check_ignore_path_under_excluded_directory_names_the_directory_rule(env, capsys):
    # The file itself matches no rule. Its excluded ancestor's rule decides, as in git.
    inner = str(env.nas / "build" / "out.txt")
    rules = os.path.join(str(env.nas), ".ragignore")

    rc, out, _ = _run(capsys, "check-ignore", "-v", inner)

    assert rc == 0
    assert out == f"{rules}:4:build/\t{inner}\n"


def test_check_ignore_path_outside_every_root_is_fatal_with_no_stdout(env, capsys):
    outside = str(env.tmp / "elsewhere.txt")
    inside = str(env.nas / "secret.log")

    rc, out, err = _run(capsys, "check-ignore", inside, outside)

    # Fatal errors are checked before any output, so the inside path is not printed either.
    assert rc == 128
    assert out == ""
    assert "outside every configured root" in err


def test_check_ignore_root_itself_is_fatal(env, capsys):
    rc, out, err = _run(capsys, "check-ignore", str(env.nas))

    assert rc == 128
    assert "is a configured root" in err


def test_check_ignore_non_matching_without_verbose_is_fatal(env, capsys):
    rc, out, err = _run(capsys, "check-ignore", "-n", str(env.nas / "notes.txt"))

    assert rc == 128
    assert "only valid with --verbose" in err


def test_check_ignore_without_paths_is_fatal(env, capsys):
    rc, out, err = _run(capsys, "check-ignore")

    assert rc == 128
    assert "no path specified" in err


# ---------------------------------------------------------------------------
# ls-files
# ---------------------------------------------------------------------------

def test_ls_files_ignored_lists_includable_files_excluded_by_rules(env, capsys):
    rc, out, _ = _run(capsys, "ls-files", "--ignored")

    expected = sorted([
        str(env.nas / "secret.log"),
        str(env.nas / "build" / "out.txt"),
    ])
    assert rc == 0
    assert out.splitlines() == expected


def test_ls_files_others_lists_on_disk_files_not_excluded_and_not_queued(env, capsys):
    # notes.txt is queued, so it is not "other". gone.txt is queued but not on disk.
    _seed_queue(env.db_path, {
        str(env.nas / "notes.txt"): "pending",
        str(env.nas / "gone.txt"): "completed",
    })

    rc, out, _ = _run(capsys, "ls-files", "--others")

    expected = sorted([str(env.nas / "doc.md"), str(env.nas / "keep.log")])
    assert rc == 0
    assert out.splitlines() == expected


def test_ls_files_default_lists_completed_rows_only(env, capsys):
    _seed_queue(env.db_path, {
        str(env.nas / "notes.txt"): "pending",
        str(env.nas / "doc.md"): "ignored",
        str(env.nas / "keep.log"): "processing",
        str(env.nas / "gone.txt"): "completed",
    })

    rc, out, _ = _run(capsys, "ls-files")

    assert rc == 0
    assert out == str(env.nas / "gone.txt") + "\n"


def test_ls_files_stage_prints_every_row_with_its_status(env, capsys):
    notes = str(env.nas / "notes.txt")
    doc = str(env.nas / "doc.md")
    _seed_queue(env.db_path, {notes: "pending", doc: "ignored"})

    rc, out, _ = _run(capsys, "ls-files", "--stage")

    assert rc == 0
    # Rows come out sorted by file_path, so the order is doc.md then notes.txt.
    assert out.splitlines() == [f"ignored\t{doc}", f"pending\t{notes}"]


def test_ls_files_missing_database_is_fatal_and_is_not_created(env, capsys):
    assert not env.db_path.exists()

    rc, out, err = _run(capsys, "ls-files", "--stage")

    assert rc == 128
    assert "cannot open queue database read-only" in err
    # The read-only open must not create the file or its directory.
    assert not env.db_path.exists()
    assert not env.db_path.parent.exists()


def test_ls_files_modes_are_mutually_exclusive(env, capsys):
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["ls-files", "--ignored", "--stage"])

    assert excinfo.value.code == 2


# ---------------------------------------------------------------------------
# ls-files with <path> arguments
# ---------------------------------------------------------------------------

def test_ls_files_ignored_subtree_walk_is_limited_to_the_path(env, capsys, monkeypatch):
    # Record every directory os.walk is started on. Only the subtree may be walked.
    starts: list[str] = []
    real_walk = os.walk

    def recording_walk(top, *args, **kwargs):
        starts.append(top)
        return real_walk(top, *args, **kwargs)

    monkeypatch.setattr(cli.os, "walk", recording_walk)

    rc, out, _ = _run(capsys, "ls-files", "--ignored", str(env.nas / "build"))

    assert rc == 0
    assert out.splitlines() == [str(env.nas / "build" / "out.txt")]
    assert starts == [str(env.nas / "build")]


def test_ls_files_ignored_subtree_applies_ancestor_ragignore(env, capsys):
    # The root .ragignore has `*.log`. It must still apply when only sub/ is walked.
    sub = env.nas / "sub"
    sub.mkdir()
    (sub / "x.log").write_text("x")
    (sub / "y.txt").write_text("y")

    rc, out, _ = _run(capsys, "ls-files", "--ignored", str(sub))

    assert rc == 0
    assert out.splitlines() == [str(sub / "x.log")]


def test_ls_files_ignored_subtree_applies_global_config_rules(env, capsys):
    # config/ragignore excludes `*.dist-info/`. The global file must apply to a subtree walk too.
    pkg = env.nas / "pkg.dist-info"
    pkg.mkdir()
    (pkg / "METADATA.txt").write_text("x")

    rc, out, _ = _run(capsys, "ls-files", "--ignored", str(pkg))

    assert rc == 0
    assert out.splitlines() == [str(pkg / "METADATA.txt")]


def test_ls_files_excluded_ancestor_makes_every_file_below_it_ignored(env, capsys):
    # extra.md matches no rule itself. Its excluded ancestor build/ makes it ignored, as in git.
    extra = env.nas / "build" / "extra.md"
    extra.write_text("x")

    rc, out, _ = _run(capsys, "ls-files", "--ignored", str(extra))
    assert rc == 0
    assert out.splitlines() == [str(extra)]

    rc, out, _ = _run(capsys, "ls-files", "--ignored", str(env.nas / "build"))
    assert out.splitlines() == sorted([str(extra), str(env.nas / "build" / "out.txt")])


def test_ls_files_others_under_excluded_directory_prints_nothing(env, capsys):
    _seed_queue(env.db_path, {})

    rc, out, _ = _run(capsys, "ls-files", "--others", str(env.nas / "build"))

    assert (rc, out) == (0, "")


def test_ls_files_others_file_path_is_listed_when_not_queued(env, capsys):
    _seed_queue(env.db_path, {})

    rc, out, _ = _run(capsys, "ls-files", "--others", str(env.nas / "doc.md"))

    assert rc == 0
    assert out.splitlines() == [str(env.nas / "doc.md")]


def test_ls_files_relative_paths_resolve_against_cwd(env, capsys, monkeypatch):
    monkeypatch.chdir(env.nas)

    rc, out, _ = _run(capsys, "ls-files", "--ignored", "build")
    assert rc == 0
    assert out.splitlines() == [str(env.nas / "build" / "out.txt")]

    rc, out, _ = _run(capsys, "ls-files", "--ignored", "secret.log")
    assert out.splitlines() == [str(env.nas / "secret.log")]

    # "." is the root itself, so it gives the same listing as no path at all.
    _, dot_out, _ = _run(capsys, "ls-files", "--ignored", ".")
    _, none_out, _ = _run(capsys, "ls-files", "--ignored")
    assert dot_out == none_out


def test_ls_files_file_path_argument_lists_only_that_file(env, capsys):
    rc, out, _ = _run(capsys, "ls-files", "--ignored", str(env.nas / "secret.log"))
    assert rc == 0
    assert out.splitlines() == [str(env.nas / "secret.log")]

    # notes.txt is not excluded, so a path to it lists nothing under --ignored.
    rc, out, _ = _run(capsys, "ls-files", "--ignored", str(env.nas / "notes.txt"))
    assert (rc, out) == (0, "")


def test_ls_files_root_itself_gives_the_full_listing(env, capsys):
    _, full_out, _ = _run(capsys, "ls-files", "--ignored")

    rc, root_out, _ = _run(capsys, "ls-files", "--ignored", str(env.nas))

    assert rc == 0
    assert root_out == full_out


def test_ls_files_overlapping_paths_print_each_file_once(env, capsys):
    _, full_out, _ = _run(capsys, "ls-files", "--ignored")

    rc, out, _ = _run(capsys, "ls-files", "--ignored", str(env.nas), str(env.nas / "build"))

    assert rc == 0
    assert out == full_out


def test_ls_files_symlinked_spelling_maps_onto_the_configured_root(env, capsys):
    # A path entered through a symlink into a root is reported with the root's own spelling.
    link = env.tmp / "link"
    link.symlink_to(env.nas)

    rc, out, _ = _run(capsys, "ls-files", "--ignored", str(link / "build"))
    assert rc == 0
    assert out.splitlines() == [str(env.nas / "build" / "out.txt")]

    # The symlinked root lists the same files as the real one: count parity.
    _, via_link, _ = _run(capsys, "ls-files", "--ignored", str(link))
    _, via_real, _ = _run(capsys, "ls-files", "--ignored", str(env.nas))
    assert via_link == via_real


def test_check_ignore_accepts_symlinked_spelling_of_a_path(env, capsys):
    link = env.tmp / "link"
    link.symlink_to(env.nas)
    rules = os.path.join(str(env.nas), ".ragignore")

    rc, out, _ = _run(capsys, "check-ignore", "-v", str(link / "secret.log"))

    assert rc == 0
    assert out == f"{rules}:2:*.log\t{link / 'secret.log'}\n"


def test_ls_files_default_and_stage_filter_rows_by_directory_prefix(env, capsys):
    build_a = str(env.nas / "build" / "a.txt")
    build_b = str(env.nas / "build" / "b.txt")
    _seed_queue(env.db_path, {
        build_a: "completed",
        build_b: "pending",
        str(env.nas / "buildings.txt"): "completed",  # shares a prefix but is not under build/
        str(env.nas / "notes.txt"): "completed",
    })

    rc, out, _ = _run(capsys, "ls-files", str(env.nas / "build"))
    assert rc == 0
    assert out == build_a + "\n"

    rc, out, _ = _run(capsys, "ls-files", "--stage", str(env.nas / "build"))
    assert rc == 0
    assert out.splitlines() == [f"completed\t{build_a}", f"pending\t{build_b}"]


def test_ls_files_default_and_stage_filter_rows_by_exact_file_path(env, capsys):
    notes = str(env.nas / "notes.txt")
    _seed_queue(env.db_path, {
        notes: "completed",
        notes + ".bak": "completed",
    })

    rc, out, _ = _run(capsys, "ls-files", notes)
    assert rc == 0
    assert out == notes + "\n"

    rc, out, _ = _run(capsys, "ls-files", "--stage", notes)
    assert out == f"completed\t{notes}\n"


def test_ls_files_path_matching_nothing_inside_a_root_prints_nothing(env, capsys):
    # As with git ls-files, a path that matches nothing is not an error.
    _seed_queue(env.db_path, {})

    rc, out, err = _run(capsys, "ls-files", "--ignored", str(env.nas / "missing.txt"))
    assert (rc, out, err) == (0, "", "")

    rc, out, err = _run(capsys, "ls-files", "--stage", str(env.nas / "missing.txt"))
    assert (rc, out, err) == (0, "", "")


@pytest.mark.parametrize("flags", [[], ["--ignored"], ["--others"], ["--stage"]])
def test_ls_files_path_outside_every_root_is_fatal_with_no_stdout(env, capsys, flags):
    _seed_queue(env.db_path, {str(env.nas / "notes.txt"): "completed"})
    outside = str(env.tmp / "elsewhere")
    inside = str(env.nas / "secret.log")

    # The inside path comes first, yet nothing is printed because the error is found first.
    rc, out, err = _run(capsys, "ls-files", *flags, inside, outside)

    assert rc == 128
    assert out == ""
    assert "outside every configured root" in err


def test_ls_files_help_names_the_path_argument(capsys):
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["ls-files", "--help"])

    assert excinfo.value.code == 0
    assert "[<path> ...]" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Relative paths from a directory entered through a symlink (logical cwd)
# ---------------------------------------------------------------------------

@pytest.fixture
def linked_env(env):
    """
    Extend `env` with a symlink inside the root that points OUTSIDE it.

    Layout:
      nas/L            -> tmp/outside          (symlink, target outside every root)
      tmp/outside/sub/ holds x.log and notes.txt
      nas/sub2/        a real directory inside the root, used for the fallback tests

    Returns (logical, physical): the logical spelling of the sub directory under the
    root, and its physical path, which os.getcwd() reports after a `cd` into it.
    """
    outside = env.tmp / "outside"
    (outside / "sub").mkdir(parents=True)
    (outside / "sub" / "x.log").write_text("x")
    (outside / "sub" / "notes.txt").write_text("x")
    (env.nas / "L").symlink_to(outside)
    (env.nas / "sub2").mkdir()
    logical = env.nas / "L" / "sub"
    physical = outside / "sub"
    return SimpleNamespace(env=env, logical=logical, physical=physical)


def test_relative_paths_resolve_through_logical_pwd(linked_env, capsys, monkeypatch):
    # The shell entered the directory via the symlink, so PWD keeps the logical spelling.
    # os.getcwd() reports the physical path, which lies outside the root.
    monkeypatch.chdir(linked_env.physical)
    monkeypatch.setenv("PWD", str(linked_env.logical))
    rules = os.path.join(str(linked_env.env.nas), ".ragignore")

    rc, out, err = _run(capsys, "ls-files", "--ignored", "./")
    assert rc == 0, err
    assert out.splitlines() == [str(linked_env.logical / "x.log")]

    rc, out, err = _run(capsys, "check-ignore", "-v", "./x.log")
    assert rc == 0, err
    assert out == f"{rules}:2:*.log\t./x.log\n"


def test_dotdot_is_resolved_lexically_against_the_logical_path(linked_env, capsys, monkeypatch):
    # From L/sub, `..` is L (the symlink), not the parent of the physical target.
    # L lies inside the root, so the listing is the whole of L, as the shell would give.
    monkeypatch.chdir(linked_env.physical)
    monkeypatch.setenv("PWD", str(linked_env.logical))

    rc, out, err = _run(capsys, "ls-files", "--ignored", "..")
    assert rc == 0, err
    # `..` is nas/L, so the walk starts there and reaches nas/L/sub/x.log.
    assert out.splitlines() == [str(linked_env.logical / "x.log")]


def test_stale_pwd_falls_back_to_getcwd(linked_env, capsys, monkeypatch):
    # The physical cwd lies inside the root here, so the fallback gives a usable answer.
    # PWD points at the temp directory, which is not the current directory.
    nas = linked_env.env.nas
    monkeypatch.chdir(nas / "sub2")
    monkeypatch.setenv("PWD", str(linked_env.env.tmp))

    rc, out, err = _run(capsys, "ls-files", "--ignored", "./")

    assert rc == 0, err
    assert out == ""
    assert cli.resolve_path("./", list(cli.configured_roots(cli.load_config()))) == str(nas / "sub2")


def test_missing_pwd_falls_back_to_getcwd(linked_env, capsys, monkeypatch):
    nas = linked_env.env.nas
    monkeypatch.chdir(nas / "sub2")
    monkeypatch.delenv("PWD", raising=False)

    rc, out, err = _run(capsys, "ls-files", "--ignored", "./")

    assert rc == 0, err
    assert out == ""


def test_missing_pwd_through_symlink_is_fatal_as_before(linked_env, capsys, monkeypatch):
    # With no PWD the physical cwd is the only spelling left. It lies outside the root,
    # so the call is refused as it was before the logical-cwd fix.
    monkeypatch.chdir(linked_env.physical)
    monkeypatch.delenv("PWD", raising=False)

    rc, out, err = _run(capsys, "ls-files", "--ignored", "./")

    assert rc == 128
    assert out == ""
    assert "outside every configured root" in err


def test_logical_cwd_rejects_relative_and_mismatched_pwd(linked_env, monkeypatch):
    monkeypatch.chdir(linked_env.physical)

    # A relative PWD is never trusted.
    monkeypatch.setenv("PWD", "relative/path")
    assert cli.logical_cwd() == os.getcwd()

    # An absolute PWD that is not the current directory is not trusted.
    monkeypatch.setenv("PWD", str(linked_env.env.tmp))
    assert cli.logical_cwd() == os.getcwd()

    # The logical spelling is returned when it names the current directory.
    monkeypatch.setenv("PWD", str(linked_env.logical))
    assert cli.logical_cwd() == str(linked_env.logical)


def test_absolute_paths_are_unchanged_by_pwd(env, capsys, monkeypatch):
    # An absolute path ignores PWD entirely, even a PWD that is wrong.
    monkeypatch.setenv("PWD", str(env.tmp))

    rc, out, _ = _run(capsys, "check-ignore", "-v", str(env.nas / "secret.log"))

    assert rc == 0
    assert out.endswith(f"\t{env.nas / 'secret.log'}\n")


# ---------------------------------------------------------------------------
# Read-only queue access
# ---------------------------------------------------------------------------

def test_read_only_statedb_refuses_writes(env):
    _seed_queue(env.db_path, {str(env.nas / "notes.txt"): "pending"})

    db = StateDB(str(env.db_path), read_only=True)
    try:
        # Reads work.
        assert db.get_counts()["pending"] == 1
        # Writes must fail at the SQLite level.
        with pytest.raises(sqlite3.OperationalError):
            db._conn.execute("DELETE FROM ingestion_queue")
    finally:
        db.close()

    # The row is still there, so the DELETE did not happen.
    check = sqlite3.connect(str(env.db_path))
    try:
        assert check.execute("SELECT COUNT(*) FROM ingestion_queue").fetchone()[0] == 1
    finally:
        check.close()


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

class _FakeQdrant:
    """Stands in for the Qdrant client. It reports a fixed point count and never connects."""

    def __init__(self, total: int, per_owner: int) -> None:
        self._total = total
        self._per_owner = per_owner

    def count(self, name, count_filter=None, exact=False):
        # With a filter the owner-scoped count is returned, otherwise the total.
        return SimpleNamespace(count=self._per_owner if count_filter else self._total)


@pytest.fixture
def fake_qdrant(monkeypatch):
    """Replace the Qdrant client that print_status uses, with a fake that answers 7 and 3."""
    fake = _FakeQdrant(total=7, per_owner=3)
    monkeypatch.setattr(status_module, "get_client", lambda url, timeout=60: fake)
    monkeypatch.setattr(status_module, "collection_exists", lambda client, name: True)
    return fake


def test_status_prints_counts_and_qdrant_points(env, capsys, fake_qdrant):
    _seed_queue(env.db_path, {
        str(env.nas / "notes.txt"): "pending",
        str(env.nas / "doc.md"): "completed",
        str(env.nas / "secret.log"): "ignored",
    })

    rc, out, _ = _run(capsys, "status")

    assert rc == 0
    assert "Ingestion queue (all users):" in out
    assert "  pending    : 1" in out
    assert "  completed  : 1" in out
    assert "  ignored    : 1" in out
    assert "  total      : 3" in out
    assert "  total points    : 7" in out


def test_status_user_filter_adds_owner_points(env, capsys, fake_qdrant):
    _seed_queue(env.db_path, {str(env.nas / "notes.txt"): "pending"})

    rc, out, _ = _run(capsys, "status", "--user", "florian")

    assert rc == 0
    assert "Ingestion queue (owner=florian):" in out
    assert "  florian         : 3" in out


def test_status_ignored_flag_lists_reason(env, capsys, fake_qdrant):
    secret = str(env.nas / "secret.log")
    _seed_queue(env.db_path, {secret: "ignored"})

    rc, out, _ = _run(capsys, "status", "--ignored")

    assert rc == 0
    assert "Ignored files (1)" in out
    assert f"  [0 retries] {secret}" in out
    assert "→ test reason" in out


def test_status_missing_database_is_fatal(env, capsys, fake_qdrant):
    rc, out, err = _run(capsys, "status")

    assert rc == 128
    assert "cannot open queue database read-only" in err


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def test_module_entry_point_uses_the_same_main_as_the_console_script():
    # Importing ragnart.__main__ does not run the CLI, because of its __name__ guard.
    import ragnart.__main__ as module_entry

    assert module_entry.main is cli.main


def test_python_dash_m_runs_the_cli_from_the_checkout():
    # A real subprocess, as an operator would run it. --help exits 0 without touching data.
    result = subprocess.run(
        [sys.executable, "-m", "ragnart", "check-ignore", "--help"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=180,
    )

    assert result.returncode == 0, result.stderr
    assert "usage: ragnart check-ignore" in result.stdout
