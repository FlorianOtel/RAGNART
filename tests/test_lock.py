"""
Tests for ragnart/lock.py, the single-writer ingestion lock.

Every lock file in this module lives under pytest's tmp_path. The live lock at
`rag.ingest_lock` in ragnart.yaml is never opened, and no service is contacted.
Contention is tested with a real forked process holding the lock, because POSIX
record locks are per process and a second acquire in the same process would succeed.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

from ragnart.lock import (
    ENV_HOLDERS,
    EXIT_LOCK_HELD,
    acquire_or_exit,
    held_by_ancestor,
    lock_path_for,
    lock_path_from_config,
    nas_ingest_lock_path_from_config,
)

REPO_ROOT = Path(__file__).resolve().parent.parent

# The holder line must look exactly like `pid N on HOST, CMD, since ISO_TIME`.
HOLDER_RE = re.compile(r"^pid (\d+) on (\S+), (.*), since (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})$")


def _holder_child(lock_path: str, ready) -> None:
    """Child process body: take the lock, announce it, and keep holding it until terminated."""
    acquire_or_exit(lock_path)
    ready.set()
    time.sleep(60)


@pytest.fixture
def holder(tmp_path, monkeypatch):
    """
    Start a forked child that holds the lock on a file under tmp_path.

    Yields (lock_path, child_pid). The env var is cleared so the child and the test
    are both treated as having no lock ancestor.
    """
    monkeypatch.delenv(ENV_HOLDERS, raising=False)
    lock_path = str(tmp_path / "rag-ingest.lock")
    ctx = multiprocessing.get_context("fork")
    ready = ctx.Event()
    child = ctx.Process(target=_holder_child, args=(lock_path, ready))
    child.start()
    try:
        assert ready.wait(timeout=10), "holder child did not take the lock in time"
        yield lock_path, child.pid
    finally:
        child.terminate()
        child.join(timeout=10)


def _holders(lock_path: str, pid: int) -> str:
    """The ENV_HOLDERS value that names `pid` as the holder of `lock_path`."""
    return json.dumps({lock_path: pid})


def _write_holder_file(lock_path: str, pid: int) -> None:
    """Write a holder line that names `pid`, the same way a real holder would."""
    Path(lock_path).write_text(
        f"pid {pid} on {socket.gethostname()}, fake-cmd, since 2026-11-15T10:00:00+00:00\n",
        encoding="utf-8",
    )


def test_acquire_writes_pid_host_cmd_and_time(tmp_path, monkeypatch):
    monkeypatch.delenv(ENV_HOLDERS, raising=False)
    lock_path = str(tmp_path / "rag-ingest.lock")

    fd = acquire_or_exit(lock_path)

    assert isinstance(fd, int)
    text = Path(lock_path).read_text(encoding="utf-8")
    # Exactly one line, in the documented format, naming this process and host.
    assert text.count("\n") == 1
    match = HOLDER_RE.match(text.rstrip("\n"))
    assert match is not None, f"holder line has the wrong format: {text!r}"
    assert int(match.group(1)) == os.getpid()
    assert match.group(2) == socket.gethostname()
    # The command is the pytest argv, so it is never empty.
    assert match.group(3)


def test_second_process_contention_exits_with_constant_naming_holder(holder, capsys):
    lock_path, holder_pid = holder

    with pytest.raises(SystemExit) as excinfo:
        acquire_or_exit(lock_path)

    assert excinfo.value.code == EXIT_LOCK_HELD
    err = capsys.readouterr().err
    assert err.startswith(f"ragnart: another writer holds the ingestion lock ({lock_path}): ")
    assert f"pid {holder_pid} on " in err


def test_contender_does_not_truncate_holders_file(holder):
    lock_path, _holder_pid = holder
    before = Path(lock_path).read_bytes()
    assert before, "the holder should have written its line before the contender ran"

    with pytest.raises(SystemExit):
        acquire_or_exit(lock_path)

    assert Path(lock_path).read_bytes() == before


def test_held_by_ancestor_true_when_env_pid_matches_lock_file(tmp_path, monkeypatch):
    # The parent of the pytest process is a real ancestor that is alive.
    ancestor_pid = os.getppid()
    lock_path = str(tmp_path / "rag-ingest.lock")
    _write_holder_file(lock_path, ancestor_pid)
    monkeypatch.setenv(ENV_HOLDERS, _holders(lock_path, ancestor_pid))

    assert held_by_ancestor(lock_path) is True


def test_held_by_ancestor_false_when_env_var_absent(tmp_path, monkeypatch):
    lock_path = str(tmp_path / "rag-ingest.lock")
    _write_holder_file(lock_path, os.getppid())
    monkeypatch.delenv(ENV_HOLDERS, raising=False)

    assert held_by_ancestor(lock_path) is False


def test_held_by_ancestor_false_when_recorded_pid_differs_forged(tmp_path, monkeypatch):
    # The env var names a real ancestor, but the lock file records a different pid.
    lock_path = str(tmp_path / "rag-ingest.lock")
    _write_holder_file(lock_path, os.getpid())
    monkeypatch.setenv(ENV_HOLDERS, _holders(lock_path, os.getppid()))

    assert held_by_ancestor(lock_path) is False


def test_held_by_ancestor_false_when_recorded_pid_not_alive(tmp_path, monkeypatch):
    # 999999 is far above the usual pid range, so no live process should have it.
    lock_path = str(tmp_path / "rag-ingest.lock")
    _write_holder_file(lock_path, 999999)
    monkeypatch.setenv(ENV_HOLDERS, _holders(lock_path, 999999))

    assert held_by_ancestor(lock_path) is False


def test_held_by_ancestor_false_when_recorded_pid_is_not_an_ancestor(tmp_path, monkeypatch):
    # The pid is alive and recorded correctly, but it is this process itself.
    # A process is not its own ancestor, so the walk must not accept it.
    lock_path = str(tmp_path / "rag-ingest.lock")
    _write_holder_file(lock_path, os.getpid())
    monkeypatch.setenv(ENV_HOLDERS, _holders(lock_path, os.getpid()))

    assert held_by_ancestor(lock_path) is False


def _write_config(tmp_path: Path, lock_path: str) -> Path:
    """Write a minimal ragnart.yaml-shaped file that points rag.ingest_lock at lock_path."""
    config_path = tmp_path / "ragnart.yaml"
    config_path.write_text(yaml.safe_dump({"rag": {"ingest_lock": lock_path}}), encoding="utf-8")
    return config_path


def test_run_subcommand_sets_env_var_and_child_does_not_reacquire(tmp_path, monkeypatch):
    monkeypatch.delenv(ENV_HOLDERS, raising=False)
    lock_path = str(tmp_path / "rag-ingest.lock")
    config_path = _write_config(tmp_path, lock_path)

    # The inner snippet tries to take the same lock. It should get None (no new
    # lock) and find the wrapper's pid recorded for this lock in ENV_HOLDERS.
    snippet = (
        "import json, os\n"
        "from ragnart.lock import acquire_or_exit, ENV_HOLDERS\n"
        f"result = acquire_or_exit({lock_path!r})\n"
        f"print(result, json.loads(os.environ[ENV_HOLDERS])[{lock_path!r}])\n"
    )
    wrapper = subprocess.Popen(
        [sys.executable, "-m", "ragnart.lock", "--config", str(config_path),
         "run", "--", sys.executable, "-c", snippet],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    out, err = wrapper.communicate(timeout=60)

    assert wrapper.returncode == 0, err
    assert out.split() == ["None", str(wrapper.pid)]
    # The lock file still names the wrapper, not the child.
    assert f"pid {wrapper.pid} on " in Path(lock_path).read_text(encoding="utf-8")


def test_run_propagates_child_exit_code(tmp_path, monkeypatch):
    monkeypatch.delenv(ENV_HOLDERS, raising=False)
    config_path = _write_config(tmp_path, str(tmp_path / "rag-ingest.lock"))

    result = subprocess.run(
        [sys.executable, "-m", "ragnart.lock", "--config", str(config_path),
         "run", "--", sys.executable, "-c", "import sys; sys.exit(3)"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 3, result.stderr


def test_held_subcommand_exit_code_matches_ancestor_check(tmp_path, monkeypatch):
    monkeypatch.delenv(ENV_HOLDERS, raising=False)
    config_path = _write_config(tmp_path, str(tmp_path / "rag-ingest.lock"))
    held_cmd = [sys.executable, "-m", "ragnart.lock", "--config", str(config_path), "held"]

    # Without a wrapper above it, `held` must report "not held" (exit 1).
    direct = subprocess.run(held_cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=60)
    assert direct.returncode == 1, direct.stderr

    # Run under the wrapper, the same `held` call sees its ancestor and exits 0.
    under_run = subprocess.run(
        [sys.executable, "-m", "ragnart.lock", "--config", str(config_path),
         "run", "--"] + held_cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert under_run.returncode == 0, under_run.stderr


def test_lock_path_from_config_returns_configured_path():
    config = {"rag": {"ingest_lock": "/some/where/rag-ingest.lock"}}

    assert lock_path_from_config(config) == "/some/where/rag-ingest.lock"


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"rag": {}},
        {"rag": None},
        {"rag": {"ingest_lock": ""}},
    ],
)
def test_lock_path_from_config_requires_key(config):
    # No /tmp fallback: a missing key must fail loudly.
    with pytest.raises(ValueError):
        lock_path_from_config(config)


def test_nas_ingest_lock_path_is_its_own_config_key():
    """The NAS ingestion run has its own lock path (rag.nas_ingest.lock), distinct from rag.ingest_lock."""
    config = {
        "rag": {
            "ingest_lock": "/some/where/rag-ingest.lock",
            "nas_ingest": {"lock": "/some/where/rag-nas-ingest.lock"},
        },
    }

    assert nas_ingest_lock_path_from_config(config) == "/some/where/rag-nas-ingest.lock"
    assert lock_path_from_config(config) == "/some/where/rag-ingest.lock"


@pytest.mark.parametrize(
    "config",
    [
        {"rag": {"ingest_lock": "/some/where/rag-ingest.lock"}},
        {"rag": {"nas_ingest": {}}},
        {"rag": {"nas_ingest": None}},
        {"rag": {"nas_ingest": {"lock": ""}}},
    ],
)
def test_nas_ingest_lock_path_requires_its_key_and_never_falls_back_to_the_index_lock(config):
    # A config that names only rag.ingest_lock must not make the NAS run use the index lock.
    with pytest.raises(ValueError, match="rag.nas_ingest.lock"):
        nas_ingest_lock_path_from_config(config)


# ---------------------------------------------------------------------------
# The lock selector (--which index|nas) and nested holds of both locks (session B2, A.20)
# ---------------------------------------------------------------------------

def _write_two_lock_config(tmp_path: Path) -> tuple[Path, str, str]:
    """Write a ragnart.yaml-shaped file with both locks. Returns (config path, index lock path, NAS lock path)."""
    index_path = str(tmp_path / "rag-ingest.lock")
    nas_path = str(tmp_path / "rag-nas-ingest.lock")
    config_path = tmp_path / "ragnart.yaml"
    config_path.write_text(
        yaml.safe_dump({"rag": {"ingest_lock": index_path, "nas_ingest": {"lock": nas_path}}}),
        encoding="utf-8",
    )
    return config_path, index_path, nas_path


def _both_locks_snippet(index_path: str, nas_path: str) -> str:
    """Python source that prints whether this process runs under each lock's holder, as 'True|False' pairs."""
    return (
        "from ragnart.lock import held_by_ancestor\n"
        f"print(held_by_ancestor({index_path!r}), held_by_ancestor({nas_path!r}))\n"
    )


def _cli(config_path: Path, *args: str) -> list[str]:
    """The argv of `python -m ragnart.lock --config CONFIG ARGS...`."""
    return [sys.executable, "-m", "ragnart.lock", "--config", str(config_path), *args]


def test_lock_path_for_names_the_two_locks_and_refuses_any_other_name():
    config = {"rag": {"ingest_lock": "/a/rag-ingest.lock", "nas_ingest": {"lock": "/a/rag-nas-ingest.lock"}}}

    assert lock_path_for(config, "index") == "/a/rag-ingest.lock"
    assert lock_path_for(config, "nas") == "/a/rag-nas-ingest.lock"
    with pytest.raises(ValueError, match="unknown lock"):
        lock_path_for(config, "snapshot")


def test_held_by_ancestor_ignores_the_map_entry_of_another_lock(tmp_path, monkeypatch):
    """A process holding the NAS lock is not under the index lock: the map is keyed by lock path."""
    ancestor_pid = os.getppid()
    index_path = str(tmp_path / "rag-ingest.lock")
    nas_path = str(tmp_path / "rag-nas-ingest.lock")
    _write_holder_file(index_path, ancestor_pid)
    monkeypatch.setenv(ENV_HOLDERS, _holders(nas_path, ancestor_pid))

    assert held_by_ancestor(index_path) is False


def test_held_subcommand_checks_the_named_lock(tmp_path, monkeypatch):
    monkeypatch.delenv(ENV_HOLDERS, raising=False)
    config_path, _index_path, _nas_path = _write_two_lock_config(tmp_path)

    result = subprocess.run(_cli(config_path, "held", "--which", "nas"),
                            cwd=REPO_ROOT, capture_output=True, text=True, timeout=60)

    assert result.returncode == 1, result.stderr


def test_nested_index_and_nas_holds_both_see_each_other_as_ancestors(tmp_path, monkeypatch):
    """Two nested runs, index then NAS, leave the child under both locks."""
    monkeypatch.delenv(ENV_HOLDERS, raising=False)
    config_path, index_path, nas_path = _write_two_lock_config(tmp_path)
    snippet = _both_locks_snippet(index_path, nas_path)
    command = [
        *_cli(config_path, "run", "--which", "index", "--"),
        *_cli(config_path, "run", "--which", "nas", "--"),
        sys.executable, "-c", snippet,
    ]

    result = subprocess.run(command, cwd=REPO_ROOT, capture_output=True, text=True, timeout=60)

    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["True", "True"]


def test_nested_index_run_inside_a_nas_run_takes_the_index_and_finds_the_nas_lock_held(tmp_path, monkeypatch):
    """
    The run script holds the NAS lock and the snapshot inside it takes both locks. The index lock is free,
    so it is taken. The NAS lock is already held by the run script, so the nested NAS run takes nothing new.
    """
    monkeypatch.delenv(ENV_HOLDERS, raising=False)
    config_path, index_path, nas_path = _write_two_lock_config(tmp_path)
    snippet = _both_locks_snippet(index_path, nas_path)
    command = [
        *_cli(config_path, "run", "--which", "nas", "--"),
        *_cli(config_path, "run", "--which", "index", "--"),
        *_cli(config_path, "run", "--which", "nas", "--"),
        sys.executable, "-c", snippet,
    ]

    wrapper = subprocess.Popen(command, cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = wrapper.communicate(timeout=60)

    assert wrapper.returncode == 0, err
    assert out.split() == ["True", "True"]
    # The NAS lock still names the outermost NAS run, not the nested one. The index lock names its own wrapper.
    assert f"pid {wrapper.pid} on " in Path(nas_path).read_text(encoding="utf-8")


def test_a_held_index_lock_stops_the_nested_run_at_once_with_75(tmp_path, holder, monkeypatch):
    """
    A writer holds the index lock. The snapshot's first step (take the index lock) exits 75 before its body runs.

    The holder fixture takes tmp_path/rag-ingest.lock, which is the index lock path of the config written here.
    """
    config_path, index_path, _nas_path = _write_two_lock_config(tmp_path)
    lock_path, _holder_pid = holder
    assert lock_path == index_path

    result = subprocess.run(
        [*_cli(config_path, "run", "--which", "index", "--"), sys.executable, "-c", "print('body ran')"],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=60,
    )

    assert result.returncode == EXIT_LOCK_HELD
    assert "body ran" not in result.stdout
