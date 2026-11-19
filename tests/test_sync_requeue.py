"""
Re-queue behaviour after a NAS-side submission (session B2, follow-up to step A.8).

These tests run the real scanners (ragnart/scanner.py), the submit (ragnart/sync_submit.py) and the
status poll (ragnart/sync_poll.py) over tmp_path, with a real StateDB and RegistryDB. Each hub source is
a mock. No live NFS, hub, OpenCode server or Qdrant is contacted.

What they pin down:
  - an item the hub has taken, and that has completed, is not queued again by the next scan while the file
    is unchanged, so nothing is submitted again;
  - the same item is queued again once it changes on disk;
  - a claude session whose completed row holds the hub's text MD5 is not queued again either. The scanner
    does not compare that column, so the hub's value cannot start a re-queue loop.

The scanner decides "changed" from the mtime alone (ragnart/state.py discover_or_update, the rule
`last_modified > stored`). The size and MD5 recorded at acceptance do not decide it.
"""

from __future__ import annotations

import hashlib
import os
from unittest.mock import MagicMock

from ragnart.endpoint_client import HubClient
from ragnart.registry import RegistryDB
from ragnart.scanner import scan_claude_chats, scan_nfs_roots
from ragnart.schema import (
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
)
from ragnart.state import StateDB
from ragnart.sync_plan_core import SyncPlan
from ragnart.sync_poll import poll_outstanding
from ragnart.sync_submit import submit_plan

INCLUDE = [".md", ".txt"]
OWNER = "tester"
NOW_ISO = "2026-11-19T10:00:00+00:00"
CLAUDE_UUID = "11111111-2222-3333-4444-555555555555"
CLAUDE_KEY = f"claude-session://{CLAUDE_UUID}"
CLAUDE_TEXT_HASH = "hub-text-md5-of-the-rendered-session"


def _write(path: str, text: str = "") -> None:
    """Create a file, and its parent directories, with the given text."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _nfs_config(root) -> dict:
    """A config with one NFS user root, as ragnart.yaml lays it out."""
    return {
        "rag": {"scanner": {"include_extensions": list(INCLUDE)}},
        "users": {"tester@example.com": {"owner": OWNER, "nfs_roots": [str(root)]}},
    }


def _claude_config(root) -> dict:
    """A config with one claude_chats root."""
    return {"claude_chats": {"roots": [{"path": str(root), "owner": OWNER}]}}


def _hub(content_hash: str) -> MagicMock:
    """
    A hub client for one source. Uploads and marks are accepted with a request id. A status poll
    reports the request completed, with the given content hash.
    """
    client = MagicMock(spec=HubClient)
    client.upsert_bytes.return_value = {"request_id": "req-file", "status": "received"}
    client.upsert_text.return_value = {"request_id": "req-session", "status": "received"}
    client.status.return_value = {
        "status": "completed",
        "chunk_count": 3,
        "pipeline_version": "p1",
        "content_hash": content_hash,
    }
    return client


def _clients(nas_hash: str, claude_hash: str) -> dict:
    """One hub client per NAS-side source id."""
    return {
        SOURCE_ID_NAS: _hub(nas_hash),
        SOURCE_ID_CLAUDE_LINUX_SHARED_NFS: _hub(claude_hash),
        SOURCE_ID_OPENCODE_K11: _hub("unused"),
    }


def _status(state: StateDB, key: str) -> str | None:
    """The status of one queue row, or None when it does not exist."""
    row = state._conn.execute("SELECT status FROM ingestion_queue WHERE file_path = ?", (key,)).fetchone()
    return row["status"] if row is not None else None


def _later(path: str, seconds: int = 10) -> None:
    """Move a file's mtime forward, as an edit on disk does."""
    future = os.path.getmtime(path) + seconds
    os.utime(path, (future, future))


def test_a_completed_file_is_not_queued_again_while_unchanged_and_is_after_a_change(tmp_path):
    """A file the hub completed is not submitted again by the next scan. Once it is edited, it is queued and submitted."""
    root = tmp_path / "nas"
    path = str(root / "doc.md")
    _write(path, "hello world")
    state = StateDB(str(tmp_path / "state" / "rag_state.db"))
    registry = RegistryDB(str(tmp_path / "hub_registry.db"))
    clients = _clients(nas_hash=_md5("hello world"), claude_hash="unused")
    try:
        scan_nfs_roots(state, _nfs_config(root), user_filter=OWNER)
        submit_plan(SyncPlan(), state, registry, clients, {}, NOW_ISO)
        assert _status(state, path) == "processing"
        poll_outstanding(state, clients)
        assert _status(state, path) == "completed"

        # Unchanged: the scan leaves the completed row alone, and nothing is submitted.
        scan_nfs_roots(state, _nfs_config(root), user_filter=OWNER)
        assert _status(state, path) == "completed"
        assert state.fetch_pending() == []
        submit_plan(SyncPlan(), state, registry, clients, {}, NOW_ISO)
        assert clients[SOURCE_ID_NAS].upsert_bytes.call_count == 1

        # Changed on disk: the scan queues it again, and the next submit sends it.
        _write(path, "hello changed world")
        _later(path)
        scan_nfs_roots(state, _nfs_config(root), user_filter=OWNER)
        assert _status(state, path) == "pending"
        submit_plan(SyncPlan(), state, registry, clients, {}, NOW_ISO)
        assert clients[SOURCE_ID_NAS].upsert_bytes.call_count == 2
    finally:
        state.close()
        registry.close()


def test_a_file_still_in_flight_at_the_hub_is_not_queued_again_by_the_scan(tmp_path):
    """A processing row waits for the hub. An unchanged scan leaves it processing, so it is not submitted twice."""
    root = tmp_path / "nas"
    path = str(root / "doc.md")
    _write(path, "waiting")
    state = StateDB(str(tmp_path / "state" / "rag_state.db"))
    registry = RegistryDB(str(tmp_path / "hub_registry.db"))
    clients = _clients(nas_hash=_md5("waiting"), claude_hash="unused")
    try:
        scan_nfs_roots(state, _nfs_config(root), user_filter=OWNER)
        submit_plan(SyncPlan(), state, registry, clients, {}, NOW_ISO)
        assert _status(state, path) == "processing"

        scan_nfs_roots(state, _nfs_config(root), user_filter=OWNER)

        assert _status(state, path) == "processing"
        assert state.fetch_pending() == []
    finally:
        state.close()
        registry.close()


def test_a_completed_claude_session_is_not_queued_again_after_the_hub_reports_its_text_hash(tmp_path):
    """
    A claude session completed with the hub's text MD5 is not queued again while its transcript is unchanged.
    The scanner compares the transcript's mtime, not the content_hash column, so the hub's value is harmless.
    The session is marked processing directly, since rendering is covered by test_sync_submit.py.
    """
    claude_root = tmp_path / "claude"
    transcript = str(claude_root / "project-a" / f"{CLAUDE_UUID}.jsonl")
    _write(transcript, '{"type": "user"}\n')
    state = StateDB(str(tmp_path / "state" / "rag_state.db"))
    clients = _clients(nas_hash="unused", claude_hash=CLAUDE_TEXT_HASH)
    try:
        scan_claude_chats(state, _claude_config(claude_root), user_filter=OWNER)
        assert _status(state, CLAUDE_KEY) == "pending"
        state.mark_processing(CLAUDE_KEY, request_id="req-session")

        poll_outstanding(state, clients)
        assert _status(state, CLAUDE_KEY) == "completed"
        assert state._conn.execute(
            "SELECT content_hash FROM ingestion_queue WHERE file_path = ?", (CLAUDE_KEY,)
        ).fetchone()["content_hash"] == CLAUDE_TEXT_HASH

        scan_claude_chats(state, _claude_config(claude_root), user_filter=OWNER)
        assert _status(state, CLAUDE_KEY) == "completed"
        assert state.fetch_pending() == []

        # The transcript grows: its mtime moves on, and the session is queued again.
        _write(transcript, '{"type": "user"}\n{"type": "assistant"}\n')
        _later(transcript)
        scan_claude_chats(state, _claude_config(claude_root), user_filter=OWNER)
        assert _status(state, CLAUDE_KEY) == "pending"
    finally:
        state.close()
