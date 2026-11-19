"""
Status poll of the NAS ingestion side (v0.3 session B2): records the hub's outcome for each submitted row.

A row is 'processing' from the moment the hub accepts its upsert (ragnart/sync_submit.py). Its request is
then queued on the hub, which applies it later. poll_outstanding() asks the hub about each such row, and
records a terminal outcome. It runs at the start of every NAS ingestion run, before the scan and the submit,
so the plan that follows sees the outcomes of the previous run's requests.

The mapping, from the hub's status (GET /v1/status, ragnart/ingest_api.py STATUS_FIELDS):

  completed   mark_completed with the chunk count, the pipeline version and the content hash the hub
              reports. The row's own fields stay: launch_dir and project_id were stored at submission,
              and source_type, file_type, size_bytes and hashed_at were set by the scan or at acceptance
              (ragnart/sync_submit.py). The content hash is stored as the hub reports it, even where it
              differs from the scan's. For a claude session that is the hash of the rendered text, not of
              the .jsonl file; that choice is pending an operator decision and is not changed here. The
              scanner does not read the content hash to detect change (ragnart/state.py discover_or_update
              compares mtime), so storing the hub's value cannot re-queue a session.
  ignored     mark_ignored with the hub's admission reason. The hub has already removed the item's old points.
  failed      mark_failed with the hub's error. The retry rule of ragnart/state.py applies: back to pending
              until the retries are used up, then ignored.
  404         mark_failed with a fixed reason. The hub does not know the request, so it was never applied.
              The row is submitted again on the next run.
  received, processing, or any answer that is not terminal: nothing is recorded. The row waits.

A transport error, or a hub answer other than 404, leaves the row as it is. The next run asks again.

Dependencies: httpx (through ragnart.endpoint_client), and the state module. No qdrant_client, docling or
embedder is loaded. The import contract is enforced by tests/test_ingestion_side_imports.py.
"""

from __future__ import annotations

import logging
from collections import Counter

import httpx

from .endpoint_client import HubClient, HubError
from .schema import source_id_for_key
from .state import StateDB

logger = logging.getLogger(__name__)

# The reason written when the hub has no record of a request the row is waiting on.
REASON_UNKNOWN_REQUEST = "the hub does not know this request"

# The statuses the hub reports while a request has not finished. Nothing is recorded for them.
WAITING_STATUSES = ("received", "processing")


def poll_outstanding(state_db: StateDB, clients: dict[str, HubClient]) -> Counter:
    """
    Ask the hub about every processing row that names a request, and record each terminal outcome.

    Args:
        state_db: The NAS side's rag_state.db. Written once per row with a terminal outcome.
        clients:  One HubClient per source id (SOURCE_ID_* keys). The row's client is chosen by its key.

    Returns:
        How many rows were recorded as completed, ignored or failed, and how many were left waiting
        (waiting), not reached (transport, hub_error) or had an unknown status (unknown_status).
    """
    counts: Counter = Counter()
    for row in state_db.processing_rows_with_request_id():
        key = row["file_path"]
        request_id = row["request_id"]
        client = clients[source_id_for_key(key)]

        try:
            answer = client.status(request_id)
        except HubError as exc:
            if exc.status_code != 404:
                counts["hub_error"] += 1
                logger.warning("%s: status of request %s not read (retried next run): %s", key, request_id, exc)
                continue
            # The hub does not know this request. It was never applied, so the row is failed and submitted again.
            state_db.mark_failed(key, REASON_UNKNOWN_REQUEST)
            counts["failed"] += 1
            logger.error("%s: request %s is unknown to the hub; the row is failed", key, request_id)
            continue
        except httpx.HTTPError as exc:
            counts["transport"] += 1
            logger.warning("%s: status of request %s not read, the hub was unreachable: %s", key, request_id, exc)
            continue

        status = answer["status"]
        if status in WAITING_STATUSES:
            counts["waiting"] += 1
        elif status == "completed":
            # The hub's own values are stored. The fields it does not report keep the values the row already has.
            state_db.mark_completed(
                key,
                chunk_count=answer.get("chunk_count"),
                pipeline_version=answer.get("pipeline_version"),
                content_hash=answer.get("content_hash"),
            )
            counts["completed"] += 1
            logger.info("%s: completed (chunks=%s)", key, answer.get("chunk_count"))
        elif status == "ignored":
            reason = answer.get("admission_reason") or "ignored by the hub"
            state_db.mark_ignored(key, reason)
            counts["ignored"] += 1
            logger.info("%s: ignored by the hub: %s", key, reason)
        elif status == "failed":
            reason = answer.get("error") or "failed at the hub"
            state_db.mark_failed(key, reason)
            counts["failed"] += 1
            logger.warning("%s: failed at the hub: %s", key, reason)
        else:
            # A status the poll does not know is left alone, so an unexpected hub version does not corrupt rows.
            counts["unknown_status"] += 1
            logger.warning("%s: request %s has an unknown status %r; the row is left as it is", key, request_id, status)
    return counts
