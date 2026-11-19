"""
Hub intake worker (v0.3 session B1): applies the requests that endpoint sources sent to the ingestion API.

The ingestion API (ragnart/ingest_api.py) only records a request: one row in the intake table of the
hub registry, with status 'received', and for bytes a spool file. This worker does the work later.

Run:
    python -m ragnart.intake_worker

Exit status:
    0    the intake queue is empty (every request processed, or none waiting).
    75   EXIT_LOCK_HELD: another writer holds the ingestion lock. Nothing is read or changed.

Per request, oldest first, one at a time:
    upsert           bytes:  ingest_content(spool_path)  ->  completed | ignored | failed
                     text:   ingest_content(text)        ->  completed | ignored | failed
    delete           remove the points of (source_id, key)                        ->  completed
    change_key       rewrite source_path and file_name on the points of
                     (source_id, old key). Nothing is re-embedded. A new key that
                     already has points is refused, nothing changes.              ->  completed | failed
    change_metadata  set the owner on the points of (source_id, key)              ->  completed

A request that raises is recorded as failed with its error, and the worker moves on to the next one.
A request is never run twice by this worker: each one is claimed (status 'processing') before it runs.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from qdrant_client import QdrantClient
from qdrant_client.models import FilterSelector

from ragnart.cli import load_config, open_registry
from ragnart.collection import DOCUMENTS_COLLECTION, ensure_collection, get_client
from ragnart.ingest import ingest_content, item_filter
from ragnart.lock import acquire_or_exit, lock_path_from_config
from ragnart.registry import RegistryDB
from ragnart.schema import FIELD_FILE_NAME, FIELD_OWNER, FIELD_SOURCE_PATH

logger = logging.getLogger(__name__)

# The longest error text kept on a failed request. The column is for diagnosis, not a log.
MAX_ERROR_CHARS = 500

# The fields a change_metadata request may set in B1. This must match the check in ragnart/ingest_api.py,
# which is the one that refuses other names at the door. The worker checks again, because it runs
# the change, and a row written by other code must not change more than the API allows.
CHANGEABLE_FIELDS = frozenset({FIELD_OWNER})


# ---------------------------------------------------------------------------
# One kind of request each
# ---------------------------------------------------------------------------

async def _apply_upsert(row: dict, source: dict, rag_cfg: dict, qdrant_client: QdrantClient) -> dict:
    """
    Ingest one upsert request. Returns the intake fields to record.

    A bytes request names a spool file, and a text request carries its text in the row. The
    chat metadata, if any, travels in the row's metadata. The item's modification time, if the
    endpoint sent one, travels in the row's modified_at. The source's registered owner is written
    to every point.
    """
    if row["spool_path"] is not None:
        result = await ingest_content(
            source_id=source["id"],
            key=row["key"],
            owner=source["owner"],
            rag_cfg=rag_cfg,
            qdrant_client=qdrant_client,
            spool_path=row["spool_path"],
            file_type=row["file_type"],
            chat_meta=row["metadata"],
            modified_at=row["modified_at"],
        )
    else:
        result = await ingest_content(
            source_id=source["id"],
            key=row["key"],
            owner=source["owner"],
            rag_cfg=rag_cfg,
            qdrant_client=qdrant_client,
            text=row["text"],
            chat_meta=row["metadata"],
            modified_at=row["modified_at"],
        )

    fields = {
        "status": result.status,
        "chunk_count": result.chunk_count,
        "content_hash": result.content_hash,
        "pipeline_version": result.pipeline_version,
    }
    if result.status == "ignored":
        fields["admission_reason"] = result.reason
    return fields


def _apply_delete(row: dict, source: dict, qdrant_client: QdrantClient) -> dict:
    """Delete the points of one item of this source. Points of other sources with the same key are not touched."""
    qdrant_client.delete(
        collection_name=DOCUMENTS_COLLECTION,
        points_selector=FilterSelector(filter=item_filter(source["id"], row["key"])),
        wait=True,
    )
    return {"status": "completed"}


def _apply_change_key(row: dict, source: dict, qdrant_client: QdrantClient) -> dict:
    """
    Move one item to its new key by rewriting its points' key fields. The vectors are kept, so nothing
    is re-embedded and the point ids do not change.

    file_name follows the key, because the key's last part is the name shown on hits.

    A move onto a key that already has points of this source would merge two items into one, so it is
    refused before anything is changed. The count is exact, so a single point is enough to refuse.
    """
    new_key = row["new_key"]
    if not new_key:
        raise ValueError("change_key request has no new_key")
    existing = qdrant_client.count(
        collection_name=DOCUMENTS_COLLECTION,
        count_filter=item_filter(source["id"], new_key),
        exact=True,
    ).count
    if existing > 0:
        raise ValueError("target key already has points")
    qdrant_client.set_payload(
        collection_name=DOCUMENTS_COLLECTION,
        payload={
            FIELD_SOURCE_PATH: new_key,
            FIELD_FILE_NAME: new_key.rsplit("/", 1)[-1],
        },
        points=item_filter(source["id"], row["key"]),
        wait=True,
    )
    return {"status": "completed"}


def _apply_change_metadata(row: dict, source: dict, qdrant_client: QdrantClient) -> dict:
    """Set the given metadata fields on the points of one item of this source. In B1 only owner is allowed."""
    metadata = row["metadata"] or {}
    if not metadata:
        raise ValueError("change_metadata request has no metadata")
    unknown = set(metadata) - CHANGEABLE_FIELDS
    if unknown:
        raise ValueError(f"metadata field(s) not changeable in this version: {sorted(unknown)}")
    if FIELD_OWNER in metadata and not isinstance(metadata[FIELD_OWNER], str):
        raise ValueError("owner must be a string")
    qdrant_client.set_payload(
        collection_name=DOCUMENTS_COLLECTION,
        payload=metadata,
        points=item_filter(source["id"], row["key"]),
        wait=True,
    )
    return {"status": "completed"}


# ---------------------------------------------------------------------------
# The loop
# ---------------------------------------------------------------------------

def _outcome_text(fields: dict) -> str:
    """One short phrase for the log line, from the fields recorded for a request."""
    status = fields["status"]
    if status == "completed" and "chunk_count" in fields:
        return f"completed chunks={fields['chunk_count']}"
    if status == "ignored":
        return f"ignored: {fields['admission_reason']}"
    return status


async def _process_row(row: dict, registry: RegistryDB, rag_cfg: dict, qdrant_client: QdrantClient) -> str:
    """
    Apply one claimed request and record its outcome. Returns the outcome text for the log.

    A request whose source is not registered fails at once. A failure keeps the spool file, so the
    bytes can be inspected. A completed or ignored upload has its spool file removed, because the
    intake row is then the only record the hub needs.
    """
    request_id = row["id"]
    source = registry.source_by_id(row["source_id"])
    if source is None:
        registry.update_intake(request_id, status="failed", error="unknown source")
        logger.error("intake request=%s source=%s unknown: failed", request_id, row["source_id"])
        return "failed: unknown source"

    kind = row["kind"]
    try:
        if kind == "upsert":
            fields = await _apply_upsert(row, source, rag_cfg, qdrant_client)
        elif kind == "delete":
            fields = _apply_delete(row, source, qdrant_client)
        elif kind == "change_key":
            fields = _apply_change_key(row, source, qdrant_client)
        elif kind == "change_metadata":
            fields = _apply_change_metadata(row, source, qdrant_client)
        else:
            raise ValueError(f"unknown request kind {kind!r}")
    except Exception as exc:
        # The request failed. Its error is recorded and the loop carries on with the next request.
        error_text = str(exc)[:MAX_ERROR_CHARS]
        registry.update_intake(request_id, status="failed", error=error_text)
        logger.error(
            "intake request=%s source=%s kind=%s key=%s failed: %s",
            request_id, source["id"], kind, row["key"], error_text,
        )
        return f"failed: {error_text}"

    registry.update_intake(request_id, **fields)

    # Only a terminal success or refusal removes the spool file. A failed upload keeps it for diagnosis.
    if row["spool_path"] is not None and fields["status"] in ("completed", "ignored"):
        Path(row["spool_path"]).unlink(missing_ok=True)

    outcome = _outcome_text(fields)
    logger.info(
        "intake request=%s source=%s kind=%s key=%s outcome=%s",
        request_id, source["id"], kind, row["key"], outcome,
    )
    return outcome


async def drain_intake(registry: RegistryDB, rag_cfg: dict, qdrant_client: QdrantClient) -> int:
    """
    Process every waiting request, oldest first, until none is left. Returns how many were processed.

    The lock is taken by main(), not here, so this function can be run on a registry and a client
    that a test supplies. Requests are claimed one at a time by next_intake(), so a request is
    recorded before it runs.
    """
    processed = 0
    while (row := registry.next_intake()) is not None:
        await _process_row(row, registry, rag_cfg, qdrant_client)
        processed += 1
    return processed


def main() -> int:
    """
    Take the ingestion lock, then apply the waiting intake requests. Returns the process exit status.

    The lock is taken before the registry or Qdrant is opened, so a refused run touches nothing. On
    contention acquire_or_exit exits the process with EXIT_LOCK_HELD (75) and names the holder.
    """
    config = load_config()

    # The returned fd is kept in a named variable so the lock stays for the whole process lifetime.
    _lock_fd = acquire_or_exit(lock_path_from_config(config))  # noqa: F841

    rag_cfg = config["rag"]
    registry = open_registry(config)
    try:
        # Crash recovery. A run that died mid-request left its row 'processing', and no later run would
        # claim it again. The lock is held, so no other worker is running and every such row is an orphan.
        reset_count = registry.reset_processing_intake()
        if reset_count > 0:
            logger.warning("intake: %d request(s) left processing by an interrupted run set back to received",
                           reset_count)

        qdrant_client = get_client(rag_cfg["qdrant_url"])
        ensure_collection(qdrant_client)
        processed = asyncio.run(drain_intake(registry, rag_cfg, qdrant_client))
    finally:
        registry.close()

    logger.info("intake queue empty: %d request(s) processed", processed)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    sys.exit(main())
