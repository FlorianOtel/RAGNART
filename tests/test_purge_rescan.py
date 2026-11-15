"""
Tests for utils/rag_purge_rescan.py.

Everything runs on a temporary SQLite file, real files in a temporary NFS root, and a
fake Qdrant client that serves stored texts and records deletes. No Qdrant server is
contacted, and the production queue is never opened.
"""

from __future__ import annotations

import importlib.util
import sys
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml

from ragnart import ingest
from ragnart.schema import FIELD_PARENT_TEXT
from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent

# Text that the printable gate rejects: control characters only.
GARBAGE = "\x01\x02\x03\x04\x05" * 40
# Text that passes every gate.
CLEAN = "A plain sentence with ordinary words and punctuation.\n" * 5

OWNER = "tester"


@pytest.fixture
def rescan(monkeypatch):
    """Load utils/rag_purge_rescan.py as a module. It is a script, not a package."""
    name = "rag_purge_rescan"
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "utils" / "rag_purge_rescan.py")
    module = importlib.util.module_from_spec(spec)
    # Register before executing: the dataclasses in the module look themselves up in sys.modules.
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


class FakeQdrant:
    """
    Stand-in for the Qdrant client.

    Serves the parent texts given per source_path (one entry per point), and records
    every delete with its wait flag. Filters are read back to find the source_path.
    """

    def __init__(self, texts_by_path: dict[str, list[str]] | None = None) -> None:
        self.texts_by_path = texts_by_path or {}
        self.deleted: list[str] = []
        self.delete_waits: list[bool] = []

    @staticmethod
    def _path_of(flt) -> str:
        return flt.must[0].match.value

    def count(self, collection_name, count_filter=None, exact=True):
        return SimpleNamespace(count=len(self.texts_by_path.get(self._path_of(count_filter), [])))

    def scroll(self, collection_name, scroll_filter=None, limit=10, offset=None,
               with_payload=None, with_vectors=False):
        texts = self.texts_by_path.get(self._path_of(scroll_filter), [])
        points = [SimpleNamespace(payload={FIELD_PARENT_TEXT: text}) for text in texts]
        return points, None  # a single page; next offset is None

    def delete(self, collection_name, points_selector, wait=True):
        self.deleted.append(points_selector.filter.must[0].match.value)
        self.delete_waits.append(wait)


def _config(root: Path, max_text_bytes: int = 1000, max_chunks: int = 5000) -> dict:
    """A ragnart.yaml-shaped dict with one NFS root and explicit admission caps."""
    return {
        "db_base_path": str(root.parent / "db"),
        "rag": {
            "qdrant_url": "http://qdrant.invalid:6333",
            # .csv is deliberately absent, as in ragnart.yaml (commented out there).
            "scanner": {"include_extensions": [".pdf", ".txt", ".md"]},
            "admission": {
                "max_text_file_size_bytes": max_text_bytes,
                "max_chunks_per_item": max_chunks,
                "fffd_ratio_threshold": 0.005,
            },
        },
        "users": {"tester@example.com": {"owner": OWNER, "nfs_roots": [str(root)]}},
    }


@pytest.fixture
def nfs_root(tmp_path) -> Path:
    """A temporary NFS root with one .ragignore rule and one file of each interesting kind."""
    root = tmp_path / "nfs" / "Florian"
    root.mkdir(parents=True)
    (root / ".ragignore").write_text("secret.md\n")
    (root / "paper.pdf").write_bytes(b"%PDF-1.4 stub")
    (root / "notes.txt").write_text("x" * 2000)  # over the 1000-byte cap in _config()
    (root / "clean.md").write_text(CLEAN)
    (root / "secret.md").write_text(CLEAN)        # excluded by .ragignore
    (root / "data.csv").write_text("a,b\n1,2\n")  # extension no longer included
    return root


@pytest.fixture
def db(tmp_path) -> StateDB:
    state = StateDB(str(tmp_path / "db" / "sqlite" / "rag_state.db"))
    yield state
    state.close()


def _add_completed(db: StateDB, path: str) -> None:
    db.discover_or_update(path, OWNER, 1.0)
    db.mark_processing(path)
    db.mark_completed(path)


def _add_ignored(db: StateDB, path: str, reason: str) -> None:
    db.discover_or_update(path, OWNER, 1.0)
    db.mark_ignored(path, reason)


def _row(db: StateDB, path: str) -> dict:
    """Read one row straight from SQLite, independent of the StateDB methods under test."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM ingestion_queue WHERE file_path = ?", (path,)).fetchone()
        return dict(row)
    finally:
        conn.close()


def _admission(rescan, root: Path, **overrides):
    return rescan.admission_from_config(_config(root, **overrides))


def test_pdf_below_gate_is_reparsed_on_apply_and_untouched_on_dry_run(rescan, db, nfs_root):
    pdf = str(nfs_root / "paper.pdf")
    _add_completed(db, pdf)
    qdrant = FakeQdrant({pdf: [GARBAGE, GARBAGE]})
    admission = _admission(rescan, nfs_root)

    # Dry-run: classified, but nothing deleted and the row stays completed.
    counts, candidates = rescan.scan_completed(db._db_path, qdrant, admission)
    assert counts[rescan.REPARSE_GATE] == 1
    assert [c.action for c in candidates] == [rescan.ACTION_REPARSE]
    assert qdrant.deleted == []
    assert _row(db, pdf)["status"] == "completed"

    # Apply: points deleted with wait=True, then the row is requeued with retries cleared.
    rescan.apply_candidates(db, qdrant, candidates)
    assert qdrant.deleted == [pdf]
    assert qdrant.delete_waits == [True]
    row = _row(db, pdf)
    assert row["status"] == "pending"
    assert row["retry_count"] == 0
    assert row["skip_reason"] is None
    assert row["error_msg"] is None


def test_ragignore_excluded_row_is_left_alone(rescan, db, nfs_root):
    secret = str(nfs_root / "secret.md")
    _add_completed(db, secret)
    qdrant = FakeQdrant({secret: [CLEAN]})

    counts, candidates = rescan.scan_completed(db._db_path, qdrant, _admission(rescan, nfs_root))

    assert counts[rescan.SKIP_RAGIGNORE] == 1
    assert candidates == []
    assert qdrant.deleted == []
    assert _row(db, secret)["status"] == "completed"


def test_csv_row_is_skipped_as_not_included_even_though_file_exists(rescan, db, nfs_root):
    csv_path = str(nfs_root / "data.csv")
    _add_completed(db, csv_path)
    qdrant = FakeQdrant({csv_path: [CLEAN]})

    counts, candidates = rescan.scan_completed(db._db_path, qdrant, _admission(rescan, nfs_root))

    # The file is on disk, so the category must come from the extension check, not "missing".
    assert (nfs_root / "data.csv").exists()
    assert counts[rescan.SKIP_NOT_INCLUDED] == 1
    assert candidates == []
    assert qdrant.deleted == []
    assert _row(db, csv_path)["status"] == "completed"


def test_text_over_size_cap_is_deleted_and_marked_ignored_on_apply(rescan, db, nfs_root):
    notes = str(nfs_root / "notes.txt")
    _add_completed(db, notes)
    qdrant = FakeQdrant({notes: ["stored chunk"]})

    counts, candidates = rescan.scan_completed(db._db_path, qdrant, _admission(rescan, nfs_root))

    assert counts[rescan.IGNORE_TOO_LARGE_BYTES] == 1
    assert candidates[0].reason == "too large: 2000 bytes > 1000 byte cap"
    rescan.apply_candidates(db, qdrant, candidates)
    assert qdrant.deleted == [notes]
    assert qdrant.delete_waits == [True]
    row = _row(db, notes)
    assert row["status"] == "ignored"
    assert row["skip_reason"] == "too large: 2000 bytes > 1000 byte cap"


def test_row_over_chunk_cap_is_marked_ignored(rescan, db, nfs_root):
    clean = str(nfs_root / "clean.md")
    _add_completed(db, clean)
    # Three points against a cap of two.
    qdrant = FakeQdrant({clean: [CLEAN, CLEAN, CLEAN]})

    counts, candidates = rescan.scan_completed(
        db._db_path, qdrant, _admission(rescan, nfs_root, max_chunks=2)
    )

    assert counts[rescan.IGNORE_TOO_MANY_CHUNKS] == 1
    assert candidates[0].reason == "too large: 3 chunks > 2 chunk cap"


def test_clean_text_row_is_kept(rescan, db, nfs_root):
    clean = str(nfs_root / "clean.md")
    _add_completed(db, clean)
    qdrant = FakeQdrant({clean: [CLEAN]})

    counts, candidates = rescan.scan_completed(db._db_path, qdrant, _admission(rescan, nfs_root))

    assert counts[rescan.KEEP] == 1
    assert candidates == []


def test_transient_rows_requeued_only_on_apply(rescan, db, nfs_root):
    slow = str(nfs_root / "paper.pdf")
    server = str(nfs_root / "clean.md")
    binary = str(nfs_root / "notes.txt")
    _add_ignored(db, slow, "docling failed: timed out after 0ns")
    _add_ignored(db, server, "500 Internal Server Error from Qdrant")
    _add_ignored(db, binary, "binary content: 10% printable chars")
    qdrant = FakeQdrant()

    # Dry-run: the two transient rows are candidates, the binary row is not.
    counts, candidates = rescan.scan_transient(db._db_path, qdrant)
    assert counts[rescan.REQUEUE_TRANSIENT] == 2
    assert sorted(c.file_path for c in candidates) == sorted([slow, server])
    assert _row(db, slow)["status"] == "ignored"

    # Apply: both become pending with retries cleared, and no Qdrant delete is issued.
    rescan.apply_candidates(db, qdrant, candidates)
    for path in (slow, server):
        row = _row(db, path)
        assert row["status"] == "pending"
        assert row["retry_count"] == 0
        assert row["skip_reason"] is None
    assert _row(db, binary)["status"] == "ignored"
    assert qdrant.deleted == []


def test_main_dry_run_writes_nothing_and_never_opens_statedb(rescan, db, nfs_root, tmp_path, monkeypatch):
    pdf = str(nfs_root / "paper.pdf")
    _add_completed(db, pdf)
    qdrant = FakeQdrant({pdf: [GARBAGE]})

    # Point the config at the same database directory the db fixture created.
    config = _config(nfs_root)
    config["db_base_path"] = str(tmp_path / "db")
    config_path = tmp_path / "ragnart.yaml"
    config_path.write_text(yaml.safe_dump(config))
    tsv_path = tmp_path / "rescan.tsv"

    monkeypatch.setattr(rescan, "CONFIG_PATH", config_path)
    monkeypatch.setattr(rescan, "get_client", lambda url, timeout=60: qdrant)

    def _forbidden(*args, **kwargs):
        raise AssertionError("dry-run must not construct StateDB")

    monkeypatch.setattr(rescan, "StateDB", _forbidden)

    rescan.main(["--save", str(tsv_path)])

    assert qdrant.deleted == []
    assert _row(db, pdf)["status"] == "completed"
    lines = tsv_path.read_text().splitlines()
    assert lines[0] == "action\tcategory\tpoints\tfile_path\treason"
    assert lines[1].startswith(f"{rescan.ACTION_REPARSE}\t{rescan.REPARSE_GATE}\t1\t{pdf}\t")


def test_text_like_predicate_is_the_shared_ingest_definition():
    # The tool imports these helpers from ingest, so the definition is checked here once.
    assert ingest.is_text_like_path("/x/notes.txt")
    assert ingest.is_text_like_path("/x/config.yaml")
    assert not ingest.is_text_like_path("/x/paper.pdf")
    assert not ingest.is_text_like_path("/x/nb.ipynb")
    assert not ingest.is_text_like_path("/x/chat.jsonl")
    assert not ingest.is_text_like_path("opencode://ses_123")
    assert ingest.fffd_ratio("ab�") == pytest.approx(1 / 3)


# ---------------------------------------------------------------------------
# Single-writer lock: --apply takes it, dry-run never does.
# ---------------------------------------------------------------------------

def _write_config_with_lock(rescan, tmp_path, nfs_root, monkeypatch) -> None:
    """Point main() at a temporary ragnart.yaml that carries rag.ingest_lock."""
    config = _config(nfs_root)
    config["db_base_path"] = str(tmp_path / "db")
    config["rag"]["ingest_lock"] = str(tmp_path / "rag-ingest.lock")
    config_path = tmp_path / "ragnart.yaml"
    config_path.write_text(yaml.safe_dump(config))
    monkeypatch.setattr(rescan, "CONFIG_PATH", config_path)


def test_apply_with_lock_held_exits_with_constant_and_applies_nothing(
    rescan, db, nfs_root, tmp_path, monkeypatch
):
    from ragnart.lock import EXIT_LOCK_HELD

    pdf = str(nfs_root / "paper.pdf")
    _add_completed(db, pdf)
    qdrant = FakeQdrant({pdf: [GARBAGE]})
    _write_config_with_lock(rescan, tmp_path, nfs_root, monkeypatch)

    monkeypatch.setattr(rescan, "get_client", lambda url, timeout=60: qdrant)

    # Simulate another writer holding the lock: the apply branch must stop right here.
    def _held(lock_path):
        sys.exit(EXIT_LOCK_HELD)

    monkeypatch.setattr(rescan, "acquire_or_exit", _held)

    # If the lock check were skipped, these would fire and fail the test.
    def _forbidden(*args, **kwargs):
        raise AssertionError("lock held: must not reach StateDB or apply_candidates")

    monkeypatch.setattr(rescan, "StateDB", _forbidden)
    monkeypatch.setattr(rescan, "apply_candidates", _forbidden)

    with pytest.raises(SystemExit) as exc:
        rescan.main(["--apply"])

    assert exc.value.code == EXIT_LOCK_HELD
    assert qdrant.deleted == []
    assert _row(db, pdf)["status"] == "completed"


def test_dry_run_never_touches_the_lock(rescan, db, nfs_root, tmp_path, monkeypatch):
    pdf = str(nfs_root / "paper.pdf")
    _add_completed(db, pdf)
    qdrant = FakeQdrant({pdf: [GARBAGE]})
    _write_config_with_lock(rescan, tmp_path, nfs_root, monkeypatch)

    monkeypatch.setattr(rescan, "get_client", lambda url, timeout=60: qdrant)
    lock_mock = MagicMock(name="acquire_or_exit")
    monkeypatch.setattr(rescan, "acquire_or_exit", lock_mock)

    def _forbidden(*args, **kwargs):
        raise AssertionError("dry-run must not construct StateDB")

    monkeypatch.setattr(rescan, "StateDB", _forbidden)

    rescan.main([])

    lock_mock.assert_not_called()
    assert qdrant.deleted == []
    assert _row(db, pdf)["status"] == "completed"
