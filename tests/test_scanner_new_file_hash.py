"""
Tests for hashing genuinely new files at scan time (ragnart/scanner.py scan_nfs_roots).

A file with no queue row yet is hashed during the scan, so a move can be detected in the
same sync. A file that already has a row is not re-read. These tests run the real
scanner over tmp_path with a real StateDB. No live NFS, Qdrant or OpenCode is contacted.
"""

from __future__ import annotations

import hashlib
import logging
import os

import ragnart.scanner as scanner
from ragnart.scanner import scan_nfs_roots
from ragnart.state import StateDB

INCLUDE = [".md", ".txt"]
OWNER = "tester"


def _write(path, text: str = "") -> None:
    """Create a file, and its parent directories, with the given text."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


def _config(root) -> dict:
    """A config with one user root, the way ragnart.yaml lays it out."""
    return {
        "rag": {"scanner": {"include_extensions": list(INCLUDE)}},
        "users": {"tester@example.com": {"owner": OWNER, "nfs_roots": [str(root)]}},
    }


def _row(state_db: StateDB, file_path: str):
    """Return the queue row for file_path as a dict-like sqlite row, or None."""
    return state_db._conn.execute(
        "SELECT size_bytes, content_hash, hashed_at FROM ingestion_queue WHERE file_path = ?",
        (file_path,),
    ).fetchone()


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def test_new_file_gets_size_hash_and_hashed_at_after_one_scan(tmp_path) -> None:
    root = tmp_path / "nas"
    path = str(root / "doc.md")
    _write(path, "hello world")
    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        result = scan_nfs_roots(state_db, _config(root), user_filter=OWNER)
        row = _row(state_db, path)
    finally:
        state_db.close()

    assert row["size_bytes"] == len("hello world")
    assert row["content_hash"] == _md5("hello world")
    assert row["hashed_at"] is not None
    assert result["new_hashes"] == {path: _md5("hello world")}


def test_file_already_in_the_queue_is_not_hashed_again(tmp_path, monkeypatch) -> None:
    root = tmp_path / "nas"
    path = str(root / "doc.md")
    _write(path, "stable content")

    calls: list[str] = []
    real_hash_file = scanner.hash_file

    def counting_hash_file(file_path: str):
        calls.append(file_path)
        return real_hash_file(file_path)

    monkeypatch.setattr(scanner, "hash_file", counting_hash_file)

    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        first = scan_nfs_roots(state_db, _config(root), user_filter=OWNER)
        second = scan_nfs_roots(state_db, _config(root), user_filter=OWNER)
    finally:
        state_db.close()

    # Hashed once, on the scan that first saw the file. The second scan finds the row
    # and does not re-read it.
    assert calls == [path]
    assert first["new_hashes"] == {path: _md5("stable content")}
    assert second["new_hashes"] == {}


def test_only_genuinely_new_files_appear_in_new_hashes(tmp_path) -> None:
    root = tmp_path / "nas"
    old_path = str(root / "old.md")
    new_path = str(root / "new.md")
    _write(old_path, "already known")

    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        scan_nfs_roots(state_db, _config(root), user_filter=OWNER)
        _write(new_path, "just appeared")
        result = scan_nfs_roots(state_db, _config(root), user_filter=OWNER)
    finally:
        state_db.close()

    assert result["new_hashes"] == {new_path: _md5("just appeared")}


def test_unreadable_new_file_is_admitted_without_a_hash(tmp_path, monkeypatch, caplog) -> None:
    root = tmp_path / "nas"
    path = str(root / "locked.md")
    _write(path, "cannot be read")

    def failing_hash_file(file_path: str):
        raise OSError("permission denied")

    monkeypatch.setattr(scanner, "hash_file", failing_hash_file)

    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        with caplog.at_level(logging.WARNING, logger="ragnart.scanner"):
            result = scan_nfs_roots(state_db, _config(root), user_filter=OWNER)
        row = _row(state_db, path)
    finally:
        state_db.close()

    # The file is still queued, so ingestion can report its failure. It has no hash, so
    # it cannot take part in move detection.
    assert row is not None
    assert row["content_hash"] is None
    assert row["size_bytes"] is None
    assert path not in result["new_hashes"]
    assert any("could not hash new file" in record.getMessage() for record in caplog.records)
