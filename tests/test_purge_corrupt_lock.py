"""
Tests for the single-writer lock in utils/rag_purge_corrupt.py.

Only --confirm deletes Qdrant points, so only --confirm takes the lock. The tests
check two things: a held lock stops --confirm before the first delete, and a
dry-run (no --confirm) never calls acquire_or_exit at all.

Everything runs against a MagicMock Qdrant client and a temporary SQLite file.
No live Qdrant server is contacted, and the live queue database is never opened.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ragnart.lock import EXIT_LOCK_HELD
from ragnart.registry import RegistryDB
from ragnart.schema import FIELD_PARENT_TEXT, FIELD_SOURCE_ID, FIELD_SOURCE_PATH
from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent

# Text that the printable gate rejects, so the point is classed as corrupt.
GARBAGE = "\x01\x02\x03\x04\x05" * 40

# A source path that does not exist on disk, so the --confirm safety check does not skip it.
CORRUPT_PATH = "/nas/Florian/gone.pdf"


@pytest.fixture
def purge_corrupt(monkeypatch):
    """Load utils/rag_purge_corrupt.py by file path. utils/ is not a package."""
    spec = importlib.util.spec_from_file_location(
        "rag_purge_corrupt", REPO_ROOT / "utils" / "rag_purge_corrupt.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def main_env(purge_corrupt, tmp_path, monkeypatch):
    """
    Point main() at a temporary database and a mock Qdrant client with one corrupt point.

    main() reads ragnart.yaml for db_base_path. The yaml loader is replaced so that
    db_base_path is tmp_path and the live database is never opened.
    """
    # Create the SQLite file with the real schema, so the read-only open in main() succeeds.
    StateDB(str(tmp_path / "sqlite" / "rag_state.db")).close()
    # main() also reads the hub sources from the registry (read-only), so it must exist too.
    RegistryDB(str(tmp_path / "sqlite" / "hub_registry.db")).close()

    client = MagicMock(name="qdrant")
    # One page with one corrupt point. next_offset None ends the scroll.
    client.scroll.return_value = (
        [MagicMock(payload={FIELD_PARENT_TEXT: GARBAGE, FIELD_SOURCE_PATH: CORRUPT_PATH})],
        None,
    )
    monkeypatch.setattr(purge_corrupt, "get_client", lambda url, timeout=60: client)
    monkeypatch.setattr(
        purge_corrupt.yaml,
        "safe_load",
        # rag.ingest_lock is required by lock_path_from_config(), which runs before acquire_or_exit.
        lambda f: {
            "rag": {"qdrant_url": "http://unused:6333", "ingest_lock": str(tmp_path / "rag-ingest.lock")},
            "db_base_path": str(tmp_path),
        },
    )
    return client


def test_confirm_with_lock_held_exits_with_constant_and_deletes_nothing(
    purge_corrupt, main_env, monkeypatch
):
    # Simulate another writer holding the lock: --confirm must stop before any delete.
    def _held(lock_path):
        sys.exit(EXIT_LOCK_HELD)

    monkeypatch.setattr(purge_corrupt, "acquire_or_exit", _held)
    monkeypatch.setattr(sys, "argv", ["rag_purge_corrupt.py", "--confirm"])

    with pytest.raises(SystemExit) as exc:
        purge_corrupt.main()

    assert exc.value.code == EXIT_LOCK_HELD
    main_env.delete.assert_not_called()


def test_dry_run_never_touches_the_lock(purge_corrupt, main_env, monkeypatch):
    lock_mock = MagicMock(name="acquire_or_exit")
    monkeypatch.setattr(purge_corrupt, "acquire_or_exit", lock_mock)
    monkeypatch.setattr(sys, "argv", ["rag_purge_corrupt.py"])

    purge_corrupt.main()  # returns normally: dry-run reports and exits

    lock_mock.assert_not_called()
    main_env.delete.assert_not_called()


def test_confirm_delete_filter_names_source_id_and_source_path(purge_corrupt, main_env, monkeypatch):
    # The item filter on the delete is two conditions: the source_id derived from the path AND
    # the source_path. A NAS path belongs to the nas source.
    monkeypatch.setattr(purge_corrupt, "acquire_or_exit", MagicMock(name="acquire_or_exit"))
    monkeypatch.setattr(sys, "argv", ["rag_purge_corrupt.py", "--confirm"])

    purge_corrupt.main()

    main_env.delete.assert_called_once()
    flt = main_env.delete.call_args.kwargs["points_selector"].filter
    assert [(c.key, c.match.value) for c in flt.must] == [
        (FIELD_SOURCE_ID, "nas"),
        (FIELD_SOURCE_PATH, CORRUPT_PATH),
    ]
