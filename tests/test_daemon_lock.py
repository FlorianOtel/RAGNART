"""
Tests for the ingestion lock in utils/rag_ingest_daemon.py main().

The daemon must take the lock before it opens StateDB or Qdrant. When another
writer holds it, the daemon must exit with EXIT_LOCK_HELD and must not touch the
queue database.

Nothing here takes the real lock file. acquire_or_exit is replaced by a stub, and
StateDB is replaced by a Mock, so the production queue and lock are never touched.
main() still reads the real ragnart.yaml, but only for reading.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ragnart.lock import EXIT_LOCK_HELD

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def daemon_module():
    """Load utils/rag_ingest_daemon.py as a module. It is a script, not a package."""
    spec = importlib.util.spec_from_file_location("rag_ingest_daemon", REPO_ROOT / "utils" / "rag_ingest_daemon.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_lock_held_exits_with_constant_and_opens_no_state_db(daemon_module, monkeypatch):
    """
    When the lock is held elsewhere, main() exits with EXIT_LOCK_HELD and never opens StateDB.

    The stub mimics acquire_or_exit on contention: it calls sys.exit(EXIT_LOCK_HELD).
    The StateDB mock must never be constructed, because the queue must not be opened
    by a writer that was refused the lock.
    """

    def refuse_lock(lock_path: str) -> None:
        # Mimic the real contention path: exit with the lock-held code.
        sys.exit(EXIT_LOCK_HELD)

    state_db_factory = MagicMock(name="StateDB")
    monkeypatch.setattr(daemon_module, "acquire_or_exit", refuse_lock)
    monkeypatch.setattr(daemon_module, "StateDB", state_db_factory)
    monkeypatch.setattr(sys, "argv", ["rag_ingest_daemon.py", "--workers", "1", "--batch", "1"])

    with pytest.raises(SystemExit) as exc_info:
        daemon_module.main()

    assert exc_info.value.code == EXIT_LOCK_HELD
    state_db_factory.assert_not_called()
