"""
Apply half of the sync (RESEARCH C3, C4, C6, C10; design §4.5, §10.3). The only sync code that writes to Qdrant.

apply_sync_plan() carries a plan out, in a fixed order. Each Qdrant write happens before
the SQLite change that records it.

The plan itself (SyncPlan, build_sync_plan(), format_plan()) lives in ragnart/sync_plan_core.py.
That module imports no qdrant_client, so the ingestion side can compute a plan without the hub
stack. It is re-exported below, so every existing `from ragnart.sync_plan import ...` keeps working.
"""

from __future__ import annotations

import logging
from typing import Any

from qdrant_client.models import FieldCondition, Filter, FilterSelector, MatchValue

from .collection import DOCUMENTS_COLLECTION
from .registry import RegistryDB
from .schema import (
    FIELD_PROJECT_ID,
    FIELD_SOURCE_ID,
    FIELD_SOURCE_PATH,
    SOURCE_ID_NAS,
    source_id_for_key,
)
from .state import StateDB

# Re-exported for callers that import the plan from here. Nothing in this module defines them.
# REASON_GRACE_EXPIRED is used by apply_sync_plan() below; the other three are for importers.
from .sync_plan_core import (  # noqa: F401
    REASON_GRACE_EXPIRED,
    SyncPlan,
    build_sync_plan,
    format_plan,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Qdrant helpers. All waits are wait=True: SQLite is written only after Qdrant confirms.
# ---------------------------------------------------------------------------


def _source_filter(source_path: str) -> Filter:
    """
    Filter matching the points of one item: its source_id AND its source_path.

    `source_path` is the item's queue key. Its source_id is derived from the key by
    source_id_for_key(), the one place the key-prefix rule is written. Both conditions
    are named, so only this source's points for this item can match.
    """
    return Filter(
        must=[
            FieldCondition(
                key=FIELD_SOURCE_ID, match=MatchValue(value=source_id_for_key(source_path))
            ),
            FieldCondition(key=FIELD_SOURCE_PATH, match=MatchValue(value=source_path)),
        ]
    )


def _set_payload_for_path(qdrant_client: Any, source_path: str, payload: dict) -> None:
    """Merge `payload` into every point stored for the item at source_path (its source_id and source_path)."""
    qdrant_client.set_payload(
        collection_name=DOCUMENTS_COLLECTION,
        payload=payload,
        points=_source_filter(source_path),
        wait=True,
    )


def _delete_points_for_path(qdrant_client: Any, source_path: str) -> None:
    """Delete every point stored for the item at source_path. Any exception propagates; SQLite keeps its row."""
    qdrant_client.delete(
        collection_name=DOCUMENTS_COLLECTION,
        points_selector=FilterSelector(filter=_source_filter(source_path)),
        wait=True,
    )


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


def apply_sync_plan(
    state_db: StateDB,
    registry_db: RegistryDB,
    qdrant_client: Any,
    plan: SyncPlan,
    now_iso: str,
) -> None:
    """
    Carry out a plan. Order matters; each step is safe to repeat after a crash.

    1. Projects (hub registry): upsert markers and their tags; remove projects whose marker is gone.
    2. Moves: delete the scan's pending row at the new path, set source_path on the
       old key's points (Qdrant, wait=True), then rename the SQLite row.
       A crash after the pending row is deleted leaves the move to be detected again
       on the next sync (the new file has no row, so it is hashed again). A crash after
       the Qdrant write is safe: the next sync finds the new file with no row, the old
       key has no points, and the rename completes. No re-embed in either case.
    3. Reappeared: clear missing_since.
    4. Newly missing: set missing_since (first time only). Points are kept.
    5. Purge: delete points (Qdrant, wait=True), then the row. Qdrant first: a crash
       in between leaves the row, which the next sync purges again.
    6. Stale rows: remove non-completed rows whose source is gone (no points).
    7. Attachment: set project_id on the points (Qdrant, wait=True), then in SQLite.
    """
    # 1. Projects.
    # Markers come from the NAS crawl, so every project they describe has the NAS source as home.
    for marker in plan.project_upserts:
        registry_db.upsert_project(
            marker["id"], marker["path"], marker["name"], marker["owner"], SOURCE_ID_NAS,
        )
        registry_db.replace_project_tags(marker["id"], marker["tags"])
    for project_id in plan.project_removals:
        registry_db.remove_project(project_id)
        logger.info("Project removed (marker no longer found): %s", project_id)

    # 2. Moves.
    for old_path, new_path in plan.moves.items():
        last_modified = state_db.delete_pending_row(new_path)
        if last_modified is None:
            # The scan queued new_path as pending just before this step. Without that row
            # the move cannot be completed, so stop rather than guess.
            raise RuntimeError(f"move target has no pending row, refusing to move: {new_path}")
        _set_payload_for_path(qdrant_client, old_path, {FIELD_SOURCE_PATH: new_path})
        state_db.rename_key(old_path, new_path, last_modified, origin_path=new_path)
        logger.info("Moved %s -> %s (content unchanged, no re-embed)", old_path, new_path)

    # 3. Reappeared.
    for path in plan.reappeared:
        state_db.clear_missing(path)
        logger.info("Reappeared, tombstone cleared: %s", path)

    # 4. Newly missing.
    for path in plan.newly_missing:
        state_db.mark_missing(path, now_iso)
        logger.info(
            "Tombstoned (%s), points kept until the grace period ends: %s",
            plan.reasons.get(path, "gone"), path,
        )

    # 5. Purge. The tombstone details are read once, for the log line.
    if plan.purge_ready:
        tombstones = state_db.tombstoned_rows()
        for path in plan.purge_ready:
            details = tombstones[path]
            _delete_points_for_path(qdrant_client, path)
            state_db.purge_deleted([path])
            logger.info(
                "purged %s chunks=%s reason=%s missing_since=%s",
                path, details["chunk_count"], REASON_GRACE_EXPIRED, details["missing_since"],
            )

    # 6. Stale rows.
    for path in plan.stale_rows:
        logger.info("Queue row removed (%s): %s", plan.reasons.get(path, "gone"), path)
    if plan.stale_rows:
        state_db.purge_deleted(plan.stale_rows)

    # 7. Attachment.
    for path, project_id in plan.attachment_changes.items():
        _set_payload_for_path(qdrant_client, path, {FIELD_PROJECT_ID: project_id})
        state_db.set_project_id(path, project_id)
    if plan.attachment_changes:
        logger.info("Re-attached %d item(s)", len(plan.attachment_changes))
