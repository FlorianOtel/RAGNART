"""
Tests for the OpenCode scan in ragnart/scanner.py (scan_opencode_sessions): the queue
row written for each session carries the OpenCode source id.

The OpenCode HTTP API is replaced by a fake get_json (the same pattern as
tests/test_metadata_backfill.py), and the password read by a fake load_password.
A real StateDB over tmp_path holds the queue. No OpenCode server, Qdrant or NFS is contacted.
"""

from __future__ import annotations

import ragnart.scanner as scanner
from ragnart.scanner import scan_opencode_sessions
from ragnart.state import StateDB

OWNER = "tester"
API_URL = "http://oc.invalid:4096"


def _config() -> dict:
    """An opencode section as ragnart.yaml lays it out, with the owner that matches the scan."""
    return {"opencode": {"api_url": API_URL, "owner": OWNER, "password_env_file": "unused"}}


def _one_session_api(monkeypatch, session_id: str) -> None:
    """Make the fake API return one session on the first page and no further pages."""

    def fake_get_json(api_url, path, password, params=None, timeout=30):
        return {
            "data": [{"id": session_id, "time": {"updated": 1_700_000_000_000}}],
            "cursor": {"next": None},
        }

    monkeypatch.setattr(scanner, "get_json", fake_get_json)
    monkeypatch.setattr(scanner, "load_password", lambda env_file: "pw")


def test_opencode_session_row_carries_the_opencode_source_id(tmp_path, monkeypatch) -> None:
    _one_session_api(monkeypatch, "ses_abc123")
    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        result = scan_opencode_sessions(state_db, _config())
        row = state_db._conn.execute(
            "SELECT file_path, source_type, source_id FROM ingestion_queue WHERE file_path = ?",
            ("opencode://ses_abc123",),
        ).fetchone()
    finally:
        state_db.close()

    assert result["existing_paths"] == {"opencode://ses_abc123"}
    assert row["source_type"] == "opencode_session"
    assert row["source_id"] == "opencode-k11"


def test_opencode_scan_with_unreachable_password_writes_no_rows(tmp_path, monkeypatch) -> None:
    # A missing password is a configuration error: the scan stops before any row is written.
    def no_password(env_file):
        raise RuntimeError("no password configured")

    monkeypatch.setattr(scanner, "load_password", no_password)
    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        result = scan_opencode_sessions(state_db, _config())
        count = state_db._conn.execute("SELECT COUNT(*) FROM ingestion_queue").fetchone()[0]
    finally:
        state_db.close()

    assert result["existing_paths"] is None
    assert count == 0
