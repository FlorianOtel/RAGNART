"""
Tests for collection_exists() / collection_is_alias() and the rag_reset.py reset logic.

Everything runs against a MagicMock Qdrant client and a temporary SQLite file.
No live Qdrant server is contacted, and the live queue database is never opened.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ragnart.collection import DOCUMENTS_COLLECTION, collection_exists, collection_is_alias
from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent


def _fake_qdrant(real: tuple[str, ...] = (), aliases: tuple[str, ...] = (), points: int = 0) -> MagicMock:
    """Build a mock QdrantClient that reports the given real collections and aliases.

    `points` is what count(...) returns, for dry-run count assertions.
    """
    client = MagicMock(name="qdrant")
    client.get_collections.return_value = SimpleNamespace(
        collections=[SimpleNamespace(name=n) for n in real]
    )
    client.get_aliases.return_value = SimpleNamespace(
        aliases=[SimpleNamespace(alias_name=a) for a in aliases]
    )
    client.count.return_value = SimpleNamespace(count=points)
    return client


# ---------------------------------------------------------------------------
# collection_exists / collection_is_alias
# ---------------------------------------------------------------------------

def test_collection_exists_true_for_real_collection():
    client = _fake_qdrant(real=(DOCUMENTS_COLLECTION,))
    assert collection_exists(client, DOCUMENTS_COLLECTION) is True


def test_collection_exists_true_for_alias_only():
    # The bug this guards against: an alias is absent from get_collections().
    client = _fake_qdrant(real=(), aliases=(DOCUMENTS_COLLECTION,))
    assert collection_exists(client, DOCUMENTS_COLLECTION) is True


def test_collection_exists_false_when_neither():
    client = _fake_qdrant(real=("other",), aliases=("another",))
    assert collection_exists(client, DOCUMENTS_COLLECTION) is False


def test_collection_is_alias_true_for_alias_only():
    client = _fake_qdrant(real=(), aliases=(DOCUMENTS_COLLECTION,))
    assert collection_is_alias(client, DOCUMENTS_COLLECTION) is True


def test_collection_is_alias_false_for_real_collection():
    client = _fake_qdrant(real=(DOCUMENTS_COLLECTION,))
    assert collection_is_alias(client, DOCUMENTS_COLLECTION) is False


def test_collection_is_alias_false_when_absent():
    client = _fake_qdrant()
    assert collection_is_alias(client, DOCUMENTS_COLLECTION) is False


# ---------------------------------------------------------------------------
# rag_reset.py — loaded from utils/ by file path (utils/ is not a package)
# ---------------------------------------------------------------------------

@pytest.fixture
def rag_reset():
    spec = importlib.util.spec_from_file_location("rag_reset", REPO_ROOT / "utils" / "rag_reset.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def queue_db(tmp_path):
    """A real SQLite queue file with three rows: two for florian, one for another owner.

    All rows start 'completed', so any reset is visible as a status change.
    """
    db_path = str(tmp_path / "sqlite" / "rag_state.db")
    state = StateDB(db_path)
    state._conn.executemany(
        "INSERT INTO ingestion_queue (file_path, owner, last_modified, status) VALUES (?, ?, ?, ?)",
        [
            ("/nas/a.pdf", "florian", 1.0, "completed"),
            ("/nas/b.pdf", "florian", 1.0, "completed"),
            ("/nas/c.pdf", "other", 1.0, "completed"),
        ],
    )
    state._conn.commit()
    state.close()
    return db_path


def _statuses(db_path: str) -> list[str]:
    """Read every queue row's status with a fresh connection."""
    conn = sqlite3.connect(db_path)
    try:
        return [r[0] for r in conn.execute("SELECT status FROM ingestion_queue ORDER BY file_path")]
    finally:
        conn.close()


def _read_only(db_path: str) -> sqlite3.Connection:
    """Same read-only connection that rag_reset.main() uses for --dry-run."""
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


# --- partial reset ---------------------------------------------------------

def test_partial_dry_run_counts_only(rag_reset, queue_db):
    client = _fake_qdrant(real=(DOCUMENTS_COLLECTION,), points=7)
    conn = _read_only(queue_db)
    try:
        rag_reset.partial_reset(client, conn, "florian", dry_run=True)
    finally:
        conn.close()

    # Zero deletes, and the queue is unchanged.
    client.delete.assert_not_called()
    assert _statuses(queue_db) == ["completed", "completed", "completed"]
    # The count is taken from Qdrant with the owner filter applied.
    assert client.count.call_count == 1


def test_partial_live_deletes_and_resets_only_owner(rag_reset, queue_db):
    client = _fake_qdrant(real=(DOCUMENTS_COLLECTION,), points=7)
    conn = sqlite3.connect(queue_db)
    try:
        rag_reset.partial_reset(client, conn, "florian", dry_run=False)
    finally:
        conn.close()

    client.delete.assert_called_once()
    # Florian's rows are reset to pending. The other owner's row stays completed.
    conn = sqlite3.connect(queue_db)
    statuses_by_owner = conn.execute(
        "SELECT owner, status FROM ingestion_queue ORDER BY file_path"
    ).fetchall()
    conn.close()
    assert statuses_by_owner == [("florian", "pending"), ("florian", "pending"), ("other", "completed")]


# --- full reset ------------------------------------------------------------

def test_full_refuses_when_documents_is_alias(rag_reset, queue_db):
    client = _fake_qdrant(real=(), aliases=(DOCUMENTS_COLLECTION,), points=7)
    conn = sqlite3.connect(queue_db)
    try:
        with pytest.raises(rag_reset.AliasResetRefused, match="docs/RAG-strategy.md §9.3"):
            rag_reset.full_reset(client, conn, dry_run=False)
    finally:
        conn.close()

    # No collection drop, no re-create, and no SQLite write.
    client.delete_collection.assert_not_called()
    client.create_collection.assert_not_called()
    assert _statuses(queue_db) == ["completed", "completed", "completed"]


def test_full_refuses_on_alias_even_in_dry_run(rag_reset, queue_db):
    client = _fake_qdrant(real=(), aliases=(DOCUMENTS_COLLECTION,))
    conn = _read_only(queue_db)
    try:
        with pytest.raises(rag_reset.AliasResetRefused):
            rag_reset.full_reset(client, conn, dry_run=True)
    finally:
        conn.close()
    client.delete_collection.assert_not_called()


def test_full_dry_run_counts_only(rag_reset, queue_db):
    client = _fake_qdrant(real=(DOCUMENTS_COLLECTION,), points=42)
    conn = _read_only(queue_db)
    try:
        rag_reset.full_reset(client, conn, dry_run=True)
    finally:
        conn.close()

    client.delete_collection.assert_not_called()
    client.create_collection.assert_not_called()
    client.delete.assert_not_called()
    assert _statuses(queue_db) == ["completed", "completed", "completed"]


def test_full_live_drops_real_collection_and_resets_all(rag_reset, queue_db, monkeypatch):
    client = _fake_qdrant(real=(DOCUMENTS_COLLECTION,), points=42)
    # ensure_collection() would talk to Qdrant for the re-create. Stub it out here.
    monkeypatch.setattr(rag_reset, "ensure_collection", lambda c: None)
    conn = sqlite3.connect(queue_db)
    try:
        rag_reset.full_reset(client, conn, dry_run=False)
    finally:
        conn.close()

    client.delete_collection.assert_called_once_with(DOCUMENTS_COLLECTION)
    assert _statuses(queue_db) == ["pending", "pending", "pending"]


# --- main() wiring ----------------------------------------------------------

@pytest.fixture
def main_env(rag_reset, queue_db, monkeypatch, tmp_path):
    """Point main() at the temporary queue database and at a mock Qdrant client.

    main() reads ragnart.yaml for db_base_path. We swap that for tmp_path so the
    live database is never opened. The base directory is tmp_path, so the
    'sqlite/rag_state.db' suffix that main() appends matches queue_db.
    """
    client = _fake_qdrant(real=(DOCUMENTS_COLLECTION,), points=5)
    monkeypatch.setattr(rag_reset, "get_client", lambda url, timeout=60: client)
    monkeypatch.setattr(rag_reset, "ensure_collection", lambda c: None)
    monkeypatch.setattr(
        rag_reset.yaml,
        "safe_load",
        # rag.ingest_lock is required by lock_path_from_config(), which main() calls before any write.
        lambda f: {
            "rag": {"qdrant_url": "http://unused:6333", "ingest_lock": str(tmp_path / "rag-ingest.lock")},
            "db_base_path": str(tmp_path),
        },
    )
    # Fail the test if a prompt is shown. Dry runs and --yes must never prompt.
    monkeypatch.setattr("builtins.input", lambda *a, **k: pytest.fail("unexpected prompt"))
    return client


def test_main_dry_run_full_makes_no_changes(rag_reset, queue_db, main_env, monkeypatch):
    monkeypatch.setattr("sys.argv", ["rag_reset.py", "--dry-run"])
    rag_reset.main()  # returns normally on success

    main_env.delete_collection.assert_not_called()
    main_env.delete.assert_not_called()
    assert _statuses(queue_db) == ["completed", "completed", "completed"]


def test_main_dry_run_partial_makes_no_changes(rag_reset, queue_db, main_env, monkeypatch):
    monkeypatch.setattr("sys.argv", ["rag_reset.py", "--user", "florian", "--dry-run"])
    rag_reset.main()

    main_env.delete.assert_not_called()
    assert _statuses(queue_db) == ["completed", "completed", "completed"]


def test_main_full_on_alias_exits_nonzero_without_delete(rag_reset, queue_db, main_env, monkeypatch, capsys):
    # Make 'documents' an alias, not a real collection.
    main_env.get_collections.return_value = SimpleNamespace(collections=[])
    main_env.get_aliases.return_value = SimpleNamespace(
        aliases=[SimpleNamespace(alias_name=DOCUMENTS_COLLECTION)]
    )
    monkeypatch.setattr("sys.argv", ["rag_reset.py", "--yes"])

    with pytest.raises(SystemExit) as exc:
        rag_reset.main()

    assert exc.value.code == 1
    main_env.delete_collection.assert_not_called()
    assert _statuses(queue_db) == ["completed", "completed", "completed"]
    assert "docs/RAG-strategy.md §9.3" in capsys.readouterr().err


# --- single-writer lock -----------------------------------------------------
# A non-dry-run reset is a writer, so it takes the lock after the confirmation gate.
# A dry-run never does.

def test_main_with_lock_held_exits_with_constant_and_resets_nothing(
    rag_reset, queue_db, main_env, monkeypatch
):
    from ragnart.lock import EXIT_LOCK_HELD

    # Simulate another writer holding the lock: main() must stop before any write.
    def _held(lock_path):
        sys.exit(EXIT_LOCK_HELD)

    monkeypatch.setattr(rag_reset, "acquire_or_exit", _held)
    monkeypatch.setattr("sys.argv", ["rag_reset.py", "--user", "florian", "--yes"])

    with pytest.raises(SystemExit) as exc:
        rag_reset.main()

    assert exc.value.code == EXIT_LOCK_HELD
    main_env.delete.assert_not_called()
    main_env.delete_collection.assert_not_called()
    # No SQLite write either: every row keeps its original status.
    assert _statuses(queue_db) == ["completed", "completed", "completed"]


def test_main_dry_run_never_touches_the_lock(rag_reset, queue_db, main_env, monkeypatch):
    lock_mock = MagicMock(name="acquire_or_exit")
    monkeypatch.setattr(rag_reset, "acquire_or_exit", lock_mock)
    monkeypatch.setattr("sys.argv", ["rag_reset.py", "--dry-run"])

    rag_reset.main()

    lock_mock.assert_not_called()
    main_env.delete_collection.assert_not_called()
    assert _statuses(queue_db) == ["completed", "completed", "completed"]
