"""
Tests for ragnart/sync_poll.py (session B2, step A.9): the status poll that records each hub outcome.

Each test queues rows in a real StateDB (tmp file), marks them processing with a request id, and runs
poll_outstanding() against one mocked HubClient per source id. The status each test expects is the status
GET /v1/status would return, as ragnart/ingest_api.py builds it (STATUS_FIELDS).

No live database, hub or Qdrant is touched.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock

import httpx
import pytest

from ragnart.endpoint_client import HubClient, HubError
from ragnart.schema import SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_NAS, SOURCE_ID_OPENCODE_K11
from ragnart.state import StateDB
from ragnart.sync_poll import REASON_UNKNOWN_REQUEST, poll_outstanding

NAS_FILE = "/mnt/nfs/Florian/a.md"
CLAUDE_KEY = "claude-session://11111111-2222-3333-4444-555555555555"
OPENCODE_KEY = "opencode://ses_abc"
MTIME = 1_700_000_000.0


@pytest.fixture
def state(tmp_path) -> StateDB:
    db = StateDB(str(tmp_path / "rag_state.db"))
    yield db
    db.close()


@pytest.fixture
def clients() -> dict:
    """One mocked HubClient per source id. Each one's status() is set by the test."""
    return {
        SOURCE_ID_NAS: MagicMock(spec=HubClient),
        SOURCE_ID_CLAUDE_LINUX_SHARED_NFS: MagicMock(spec=HubClient),
        SOURCE_ID_OPENCODE_K11: MagicMock(spec=HubClient),
    }


def _submitted(state: StateDB, key: str, request_id: str, source_id: str = SOURCE_ID_NAS) -> None:
    """A row as the submit step leaves it: pending from the scan, then processing with its request id."""
    state.discover_or_update(key, "florian", MTIME, origin_path=key, source_type="file",
                             file_type="md", source_id=source_id)
    state.mark_processing(key, request_id=request_id)


def _row(state: StateDB, key: str) -> dict:
    conn = sqlite3.connect(state._db_path)
    conn.row_factory = sqlite3.Row
    try:
        return dict(conn.execute("SELECT * FROM ingestion_queue WHERE file_path = ?", (key,)).fetchone())
    finally:
        conn.close()


def _status(clients: dict, answer: dict) -> None:
    """Make the NAS client answer every status call with `answer`."""
    clients[SOURCE_ID_NAS].status.return_value = answer


# ---------------------------------------------------------------------------
# One test per outcome
# ---------------------------------------------------------------------------

def test_completed_records_the_hubs_chunk_count_pipeline_and_hash(state, clients):
    """A completed request records the chunk count, pipeline version and content hash the hub reports."""
    _submitted(state, NAS_FILE, "req-1")
    _status(clients, {"status": "completed", "chunk_count": 7, "pipeline_version": "p1",
                      "content_hash": "hub-md5", "kind": "upsert", "key": NAS_FILE})

    counts = poll_outstanding(state, clients)

    row = _row(state, NAS_FILE)
    assert row["status"] == "completed"
    assert row["chunk_count"] == 7
    assert row["pipeline_version"] == "p1"
    assert row["content_hash"] == "hub-md5"
    assert counts["completed"] == 1


def test_completed_keeps_the_fields_the_hub_does_not_report(state, clients):
    """launch_dir and project_id, stored at submission, survive completion: the hub does not report them."""
    key = CLAUDE_KEY
    state.discover_or_update(key, "florian", MTIME, origin_path="/x/s.jsonl", source_type="claude_session",
                             source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS)
    state.set_launch_dir(key, "/mnt/nfs/Florian/proj")
    state.set_project_id(key, "proj-1")
    state.mark_processing(key, request_id="req-c")
    clients[SOURCE_ID_CLAUDE_LINUX_SHARED_NFS].status.return_value = {
        "status": "completed", "chunk_count": 3, "pipeline_version": "p1", "content_hash": "h",
    }

    poll_outstanding(state, clients)

    row = _row(state, key)
    assert row["status"] == "completed"
    assert row["launch_dir"] == "/mnt/nfs/Florian/proj"
    assert row["project_id"] == "proj-1"
    assert row["source_type"] == "claude_session"


def test_completed_with_no_chunks_records_zero(state, clients):
    """An empty item completes with zero chunks, and zero is recorded (not left as NULL)."""
    _submitted(state, NAS_FILE, "req-0")
    _status(clients, {"status": "completed", "chunk_count": 0, "pipeline_version": "p1", "content_hash": "h"})

    poll_outstanding(state, clients)

    assert _row(state, NAS_FILE)["chunk_count"] == 0


def test_ignored_records_the_hubs_admission_reason(state, clients):
    """An ignored request is recorded as ignored, with the hub's reason as the skip reason."""
    _submitted(state, NAS_FILE, "req-i")
    _status(clients, {"status": "ignored", "admission_reason": "text-like file is larger than the cap"})

    counts = poll_outstanding(state, clients)

    row = _row(state, NAS_FILE)
    assert row["status"] == "ignored"
    assert row["skip_reason"] == "text-like file is larger than the cap"
    assert counts["ignored"] == 1


def test_failed_goes_back_to_pending_with_the_retry_counted(state, clients):
    """A failed request follows the retry rule: pending again, one retry used, the hub's error kept."""
    _submitted(state, NAS_FILE, "req-f")
    _status(clients, {"status": "failed", "error": "embedding timed out"})

    counts = poll_outstanding(state, clients)

    row = _row(state, NAS_FILE)
    assert row["status"] == "pending"
    assert row["retry_count"] == 1
    assert row["error_msg"] == "embedding timed out"
    assert counts["failed"] == 1


def test_failed_on_the_last_retry_is_ignored(state, clients):
    """When the retries are used up, a failed request ends ignored, as mark_failed decides."""
    _submitted(state, NAS_FILE, "req-f")
    conn = sqlite3.connect(state._db_path)
    conn.execute("UPDATE ingestion_queue SET retry_count = 4 WHERE file_path = ?", (NAS_FILE,))
    conn.commit()
    conn.close()
    _status(clients, {"status": "failed", "error": "still failing"})

    poll_outstanding(state, clients)

    row = _row(state, NAS_FILE)
    assert row["status"] == "ignored"
    assert row["skip_reason"] == "still failing"


def test_a_request_the_hub_does_not_know_fails_the_row_with_a_fixed_reason(state, clients):
    """404 from the hub means the request was never applied: the row is failed (retry counted) and submitted again."""
    _submitted(state, NAS_FILE, "req-gone")
    clients[SOURCE_ID_NAS].status.side_effect = HubError(404, "no such request")

    counts = poll_outstanding(state, clients)

    row = _row(state, NAS_FILE)
    assert row["status"] == "pending"
    assert row["retry_count"] == 1
    assert row["error_msg"] == REASON_UNKNOWN_REQUEST
    assert counts["failed"] == 1


@pytest.mark.parametrize("status", ["received", "processing"])
def test_a_request_still_in_progress_is_left_alone(state, clients, status):
    """received and processing are not terminal: the row stays processing, and nothing is counted as a result."""
    _submitted(state, NAS_FILE, "req-w")
    _status(clients, {"status": status})

    counts = poll_outstanding(state, clients)

    row = _row(state, NAS_FILE)
    assert row["status"] == "processing"
    assert row["request_id"] == "req-w"
    assert counts["waiting"] == 1


def test_a_transport_error_leaves_the_row_for_the_next_run(state, clients):
    """The hub could not be reached: nothing is recorded, and the row keeps its request id."""
    _submitted(state, NAS_FILE, "req-t")
    clients[SOURCE_ID_NAS].status.side_effect = httpx.ConnectError("hub down")

    counts = poll_outstanding(state, clients)

    row = _row(state, NAS_FILE)
    assert row["status"] == "processing"
    assert row["request_id"] == "req-t"
    assert counts["transport"] == 1


def test_a_hub_error_other_than_404_leaves_the_row_alone(state, clients):
    """A 500 (or any non-404 failure) is not an answer about the request: the row is left alone."""
    _submitted(state, NAS_FILE, "req-5")
    clients[SOURCE_ID_NAS].status.side_effect = HubError(500, "internal error")

    counts = poll_outstanding(state, clients)

    assert _row(state, NAS_FILE)["status"] == "processing"
    assert counts["hub_error"] == 1


def test_an_unknown_status_leaves_the_row_alone(state, clients):
    """A status this code does not know is not guessed at: the row is left as it is, and counted."""
    _submitted(state, NAS_FILE, "req-u")
    _status(clients, {"status": "archived"})

    counts = poll_outstanding(state, clients)

    assert _row(state, NAS_FILE)["status"] == "processing"
    assert counts["unknown_status"] == 1


# ---------------------------------------------------------------------------
# Which rows are asked, and of which client
# ---------------------------------------------------------------------------

def test_each_row_is_asked_of_the_client_of_its_own_source(state, clients):
    """A file goes to the nas client, a claude session to its client, an OpenCode session to its client."""
    _submitted(state, NAS_FILE, "req-n")
    state.discover_or_update(CLAUDE_KEY, "florian", MTIME, source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS)
    state.mark_processing(CLAUDE_KEY, request_id="req-c")
    state.discover_or_update(OPENCODE_KEY, "florian", MTIME, source_id=SOURCE_ID_OPENCODE_K11)
    state.mark_processing(OPENCODE_KEY, request_id="req-o")
    for source_id in (SOURCE_ID_NAS, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_OPENCODE_K11):
        clients[source_id].status.return_value = {"status": "received"}

    poll_outstanding(state, clients)

    clients[SOURCE_ID_NAS].status.assert_called_once_with("req-n")
    clients[SOURCE_ID_CLAUDE_LINUX_SHARED_NFS].status.assert_called_once_with("req-c")
    clients[SOURCE_ID_OPENCODE_K11].status.assert_called_once_with("req-o")


def test_rows_without_a_request_or_already_terminal_are_not_asked(state, clients):
    """A processing row with no request id, and a completed row, have nothing for the poll to ask."""
    state.discover_or_update(NAS_FILE, "florian", MTIME, source_id=SOURCE_ID_NAS)
    state.mark_processing(NAS_FILE)  # processing, no request id
    done = "/mnt/nfs/Florian/done.md"
    _submitted(state, done, "req-done")
    state.mark_completed(done, chunk_count=1)

    counts = poll_outstanding(state, clients)

    clients[SOURCE_ID_NAS].status.assert_not_called()
    assert counts == {}


def test_a_poll_with_nothing_outstanding_returns_no_counts(state, clients):
    """An empty queue makes no request and returns no counts."""
    assert poll_outstanding(state, clients) == {}
    clients[SOURCE_ID_NAS].status.assert_not_called()
