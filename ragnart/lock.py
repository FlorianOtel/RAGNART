"""
Single-writer ingestion lock for every process that writes the RAG index.

Every writer of `rag_state.db` rows or Qdrant points (sync, ingest daemon,
purge-rescan --apply, purge-corrupt --confirm, reset without --dry-run, the
snapshot script, and the whole ingestion run) takes the same exclusive lock on
the file named by `rag.ingest_lock` in ragnart.yaml. This is what makes the
single-writer rule of docs/RAGNART-design.md §10.3 true.

Design points, in short:

- Contention fails fast. A second writer prints the current holder and exits
  with EXIT_LOCK_HELD (75, sysexits.h EX_TEMPFAIL: "temporary failure, retry
  later"). It never waits, the same way `git` fails on `index.lock`.
- The lock is `fcntl.lockf` (POSIX record lock). It is per process and it is
  released by the kernel when the process exits, so a crashed writer never
  leaves a stale lock behind.
- The lock file doubles as the holder record. The holder writes one line,
  `pid N on HOST, CMD, since ISO_TIME`, into the file after it has the lock.
  A contender reads that line but never truncates or rewrites the file.
- `run -- CMD` takes the lock in a wrapper process and runs CMD as a child
  with ENV_HOLDER set to the wrapper's pid. The ingestion run script uses this
  to hold the lock for the whole run. Its child steps (sync, daemon, snapshot)
  then see that they are running under a live ancestor that holds the lock,
  and they skip re-acquiring it. A child cannot take the lock again because
  POSIX record locks never conflict with the same process's own locks, and
  because the child is a different pid.
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import os
import re
import signal
import socket
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import yaml

# Exit code for "another writer holds the ingestion lock". 75 is sysexits.h
# EX_TEMPFAIL: a temporary failure that a retry may fix. It is deliberately
# different from rag_sync_nfs.py's EXIT_DELETION_REFUSED = 1, which is a
# permanent safety refusal.
EXIT_LOCK_HELD = 75

# Environment variable that `run` sets for its child. Its value is the pid of
# the wrapper process that holds the lock.
ENV_HOLDER = "RAGNART_LOCK_HOLDER"

# Repository root: ragnart/lock.py -> parent is ragnart/, its parent is the root.
REPO_ROOT = Path(__file__).resolve().parent.parent

# Default config for the command-line entry points. Scripts call `held` and
# `run` without arguments, so this must point at the real ragnart.yaml.
DEFAULT_CONFIG_PATH = REPO_ROOT / "ragnart.yaml"

# Longest command line that is written into the holder line. Longer command
# lines are cut so that the holder line stays readable in an error message.
MAX_CMD_CHARS = 160

# The holder line always starts with this pattern. The pid is the group.
_HOLDER_PID_RE = re.compile(r"^pid (\d+) on ")

# The open file descriptor of the lock this process holds, if any. It is kept
# in a module-level variable so that the lock stays for the whole process
# lifetime. The fd is an int, so nothing closes it automatically, but keeping
# it in a named variable makes the intent explicit.
_held_fd: int | None = None


def lock_path_from_config(config: dict) -> str:
    """
    Return the lock file path from a loaded ragnart.yaml dict.

    The key `rag.ingest_lock` is required. There is no fallback to a host-local
    path such as /tmp. A per-host fallback would give each host its own lock,
    and that would silently defeat the whole purpose of the lock.

    Raises ValueError if the key is missing or empty.
    """
    try:
        path = config["rag"]["ingest_lock"]
    except (KeyError, TypeError):
        # TypeError covers a config where `rag` is null or not a mapping.
        raise ValueError("ragnart.yaml is missing the 'rag.ingest_lock' key.") from None
    if not path or not isinstance(path, str):
        raise ValueError("ragnart.yaml rag.ingest_lock must be a non-empty path string.")
    return path


def _holder_line() -> str:
    """
    Build the holder line for this process.

    Format: `pid {pid} on {host}, {cmd}, since {iso_time}`. The time is local
    time with seconds precision. Newlines are removed from the command line so
    that the holder stays on one line.
    """
    cmd = " ".join(sys.argv).replace("\n", " ")
    if len(cmd) > MAX_CMD_CHARS:
        cmd = cmd[: MAX_CMD_CHARS - 3] + "..."
    since = datetime.now().astimezone().replace(microsecond=0).isoformat()
    return f"pid {os.getpid()} on {socket.gethostname()}, {cmd}, since {since}"


def _read_holder_line(lock_path: str) -> str:
    """
    Read the holder line from the lock file. This is best-effort and never takes a lock.

    Returns an empty string if the file is missing, empty, or unreadable. The
    holder can be in the middle of rewriting the file, so an empty or partial
    read is possible. Callers must treat the result as advisory only.
    """
    try:
        with open(lock_path, encoding="utf-8", errors="replace") as handle:
            return handle.readline().strip()
    except OSError:
        return ""


def _read_holder_pid(lock_path: str) -> int | None:
    """Return the pid recorded in the holder line of the lock file, or None if there isn't one."""
    match = _HOLDER_PID_RE.match(_read_holder_line(lock_path))
    if match is None:
        return None
    return int(match.group(1))


def acquire_or_exit(lock_path: str) -> int | None:
    """
    Take the ingestion lock, or exit the process with EXIT_LOCK_HELD on contention.

    Returns:
        The lock file descriptor on success. The process keeps the lock until it exits.
        None if this process is a child of the wrapper that already holds the
        lock (see `held_by_ancestor`). In that case no new lock is taken.

    On contention this prints the holder to stderr and calls sys.exit(EXIT_LOCK_HELD).
    """
    global _held_fd

    # A child of a `run` wrapper that already holds the lock must not try to
    # take it again. Its lock would conflict with the wrapper's lock, and the
    # child would be refused.
    if held_by_ancestor(lock_path):
        return None

    # O_RDWR | O_CREAT without O_TRUNC: a contender must not wipe the holder's
    # line. Truncation happens only after the lock is actually ours.
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        # A held lock shows up as EAGAIN (BlockingIOError), and sometimes as
        # EACCES on NFS. Any other error is a real failure and is re-raised.
        if exc.errno not in (errno.EAGAIN, errno.EACCES):
            raise
        holder = _read_holder_line(lock_path) or "(holder unknown)"
        print(
            f"ragnart: another writer holds the ingestion lock ({lock_path}): {holder}",
            file=sys.stderr,
        )
        # The fd is not closed here. This process holds no lock through this fd,
        # but closing any fd on a file releases every POSIX lock this process
        # holds on that file. The process is about to exit anyway.
        sys.exit(EXIT_LOCK_HELD)

    # We hold the lock now. Replace the old holder line with ours and flush it
    # to disk so that another process can read it right away.
    os.ftruncate(fd, 0)
    os.write(fd, (_holder_line() + "\n").encode("utf-8"))
    os.fsync(fd)

    _held_fd = fd
    return fd


def _pid_alive(pid: int) -> bool:
    """Return True if a process with this pid exists. Signal 0 checks existence without sending a signal."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # The process exists but belongs to another user, so it is still alive.
        return True
    return True


def _parent_pid(pid: int) -> int | None:
    """
    Return the parent pid of `pid` from /proc, or None if it can't be read.

    /proc/<pid>/stat has the form `pid (comm) state ppid ...`. The comm field
    can contain spaces and parentheses, so the line is split after the LAST
    ')'. After that split, the first token is the state (field 3) and the
    second token is the ppid (field 4).
    """
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    after_comm = text.rsplit(")", 1)[1].split()
    try:
        return int(after_comm[1])
    except (IndexError, ValueError):
        return None


def held_by_ancestor(lock_path: str) -> bool:
    """
    Return True if this process is a descendant of the process that holds the lock.

    The check is deliberately strict. All of these must hold:
      1. The ENV_HOLDER environment variable is set and is an integer pid.
      2. The lock file's holder line records that same pid. An env var alone
         can be forged or inherited by accident, so the lock file is checked too.
      3. That pid is still alive.
      4. That pid is a real ancestor of this process, found by walking /proc.

    If any check fails, the env var is ignored and the caller must acquire the lock normally.
    This is Linux-only, which is fine because every host here runs Linux.
    """
    env_value = os.environ.get(ENV_HOLDER)
    if not env_value:
        return False
    try:
        holder_pid = int(env_value)
    except ValueError:
        return False

    if _read_holder_pid(lock_path) != holder_pid:
        return False
    if not _pid_alive(holder_pid):
        return False

    # Walk up from this process. The walk stops at pid 1 or when /proc can't be read.
    current = os.getpid()
    while current > 1:
        parent = _parent_pid(current)
        if parent is None:
            return False
        if parent == holder_pid:
            return True
        current = parent
    return False


def _cmd_run(config_path: Path, command: list[str]) -> int:
    """
    Take the lock in this wrapper, run `command` as a child, and return its exit code.

    The child is started with ENV_HOLDER set to this wrapper's pid. The wrapper
    stays alive for the whole run, so it keeps holding the lock. This is why the
    child is not exec'd.

    SIGTERM and SIGINT are forwarded to the child, so `systemctl stop` and Ctrl-C
    reach the real work. The wrapper then waits for the child and returns its exit code.
    """
    lock_path = _lock_path_from_file(config_path)
    acquire_or_exit(lock_path)

    env = dict(os.environ)
    env[ENV_HOLDER] = str(os.getpid())

    # The child is kept in a list so that the signal handler can reach it
    # before Popen returns.
    child: list[subprocess.Popen] = []

    def forward(signum: int, _frame: object) -> None:
        """Pass the signal on to the child, if it has started."""
        if child:
            child[0].send_signal(signum)

    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)

    proc = subprocess.Popen(command, env=env)
    child.append(proc)
    returncode = proc.wait()

    # A negative returncode means the child died from a signal. Use the shell
    # convention 128 + signal number so that the wrapper's own exit code says so.
    if returncode < 0:
        return 128 - returncode
    return returncode


def _lock_path_from_file(config_path: Path) -> str:
    """Load ragnart.yaml and return its lock path. Used by the command-line entry points only."""
    with open(config_path, encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    return lock_path_from_config(config)


def _build_parser():
    """Build the argument parser for `python -m ragnart.lock`."""
    parser = argparse.ArgumentParser(
        prog="python -m ragnart.lock",
        description="Ingestion lock helpers: run a command under the lock, or test whether this process is covered by it.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to ragnart.yaml (default: the one at the repository root).",
    )
    sub = parser.add_subparsers(dest="subcommand", required=True)

    run_parser = sub.add_parser(
        "run",
        help="Hold the lock while running a command. Usage: run -- CMD [ARGS...]",
    )
    run_parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="The command to run. Put `--` before it.",
    )

    sub.add_parser(
        "held",
        help="Exit 0 if this process runs under a lock holder that is its ancestor, 1 otherwise.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point for `python -m ragnart.lock`."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.subcommand == "held":
        lock_path = _lock_path_from_file(args.config)
        return 0 if held_by_ancestor(lock_path) else 1

    # `run`: argparse.REMAINDER keeps a leading `--`, so strip it here.
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("run needs a command after `--`.")
    return _cmd_run(args.config, command)


if __name__ == "__main__":
    sys.exit(main())
