"""
Tests for ragnart/lock.py, the single-writer ingestion lock.

Every lock file in this module lives under pytest's tmp_path. The live lock at
`rag.ingest_lock` in ragnart.yaml is never opened, and no service is contacted.
Contention is tested with a real forked process holding the lock, because POSIX
record locks are per process and a second acquire in the same process would succeed.
"""

from __future__ import annotations

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

from ragnart.lock import ENV_HOLDER, EXIT_LOCK_HELD, acquire_or_exit, held_by_ancestor, lock_path_from_config

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
    monkeypatch.delenv(ENV_HOLDER, raising=False)
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


def _write_holder_file(lock_path: str, pid: int) -> None:
    """Write a holder line that names `pid`, the same way a real holder would."""
    Path(lock_path).write_text(
        f"pid {pid} on {socket.gethostname()}, fake-cmd, since 2026-11-15T10:00:00+00:00\n",
        encoding="utf-8",
    )


def test_acquire_writes_pid_host_cmd_and_time(tmp_path, monkeypatch):
    monkeypatch.delenv(ENV_HOLDER, raising=False)
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
    monkeypatch.setenv(ENV_HOLDER, str(ancestor_pid))

    assert held_by_ancestor(lock_path) is True


def test_held_by_ancestor_false_when_env_var_absent(tmp_path, monkeypatch):
    lock_path = str(tmp_path / "rag-ingest.lock")
    _write_holder_file(lock_path, os.getppid())
    monkeypatch.delenv(ENV_HOLDER, raising=False)

    assert held_by_ancestor(lock_path) is False


def test_held_by_ancestor_false_when_recorded_pid_differs_forged(tmp_path, monkeypatch):
    # The env var names a real ancestor, but the lock file records a different pid.
    lock_path = str(tmp_path / "rag-ingest.lock")
    _write_holder_file(lock_path, os.getpid())
    monkeypatch.setenv(ENV_HOLDER, str(os.getppid()))

    assert held_by_ancestor(lock_path) is False


def test_held_by_ancestor_false_when_recorded_pid_not_alive(tmp_path, monkeypatch):
    # 999999 is far above the usual pid range, so no live process should have it.
    lock_path = str(tmp_path / "rag-ingest.lock")
    _write_holder_file(lock_path, 999999)
    monkeypatch.setenv(ENV_HOLDER, "999999")

    assert held_by_ancestor(lock_path) is False


def test_held_by_ancestor_false_when_recorded_pid_is_not_an_ancestor(tmp_path, monkeypatch):
    # The pid is alive and recorded correctly, but it is this process itself.
    # A process is not its own ancestor, so the walk must not accept it.
    lock_path = str(tmp_path / "rag-ingest.lock")
    _write_holder_file(lock_path, os.getpid())
    monkeypatch.setenv(ENV_HOLDER, str(os.getpid()))

    assert held_by_ancestor(lock_path) is False


def _write_config(tmp_path: Path, lock_path: str) -> Path:
    """Write a minimal ragnart.yaml-shaped file that points rag.ingest_lock at lock_path."""
    config_path = tmp_path / "ragnart.yaml"
    config_path.write_text(yaml.safe_dump({"rag": {"ingest_lock": lock_path}}), encoding="utf-8")
    return config_path


def test_run_subcommand_sets_env_var_and_child_does_not_reacquire(tmp_path, monkeypatch):
    monkeypatch.delenv(ENV_HOLDER, raising=False)
    lock_path = str(tmp_path / "rag-ingest.lock")
    config_path = _write_config(tmp_path, lock_path)

    # The inner snippet tries to take the same lock. It should get None (no new
    # lock) and see ENV_HOLDER set to the wrapper's pid.
    snippet = (
        "import os\n"
        "from ragnart.lock import acquire_or_exit, ENV_HOLDER\n"
        f"result = acquire_or_exit({lock_path!r})\n"
        "print(result, os.environ[ENV_HOLDER])\n"
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
    monkeypatch.delenv(ENV_HOLDER, raising=False)
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
    monkeypatch.delenv(ENV_HOLDER, raising=False)
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
