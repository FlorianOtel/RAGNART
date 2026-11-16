"""
Sync plan for the NFS, Claude Code and OpenCode scans (RESEARCH C3, C4, C6, C10; design §4.5, §10.3).

A sync runs in two halves:

  build_sync_plan()   reads the scan result and the queue, and works out every change
                      this run would make. It writes nothing.
  apply_sync_plan()   carries the plan out, in a fixed order. Each Qdrant write
                      happens before the SQLite change that records it.

format_plan() renders the plan for the sync log and for --dry-run.

The plan covers:
  - ragnart projects: each marker is upserted; a project whose marker is gone is removed.
  - moves: a vanished file and a new file with the same non-empty content_hash are one
    item at a new path. The key is rewritten; nothing is re-embedded.
  - tombstones: a completed row that vanished gets missing_since. Its points stay searchable.
  - reappearance: a tombstoned row found again has its tombstone cleared.
  - purge: a tombstone older than the grace period loses its points, then its row.
  - attachment: every completed item's project_id is its closest enclosing ragnart
    project (sessions by launch_dir), else 'global'.

The scan itself writes to the queue (new rows, re-queued rows). build_sync_plan() is
therefore only called after the scan has run; --dry-run runs the whole sync on a copy
of the database (see utils/rag_sync_nfs.py run_dry_run()).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from qdrant_client.models import FieldCondition, Filter, FilterSelector, MatchValue

from .collection import DOCUMENTS_COLLECTION
from .moves import detect_moves
from .projects import project_id_for_path, resolve_parent
from .scanner import include_extensions_from_config, is_includable
from .schema import FIELD_PROJECT_ID, FIELD_SOURCE_PATH, GLOBAL_PROJECT_ID
from .state import StateDB

logger = logging.getLogger(__name__)

# Tombstones are purged this many hours after they were first seen missing.
DEFAULT_GRACE_HOURS = 24

# Keys that are not file paths. These items attach by launch_dir, not by their key.
SESSION_KEY_PREFIXES = ("claude-session://", "opencode://")

# Reason written to the purge log line.
REASON_GRACE_EXPIRED = "tombstone grace period expired"


@dataclass
class SyncPlan:
    """
    Every change one sync run will make. Built by build_sync_plan(), carried out by apply_sync_plan().

    moves:              old key -> new key. Same content, one vanished and one new item.
    newly_missing:      completed rows that vanished this run. They are tombstoned, not deleted.
    purge_ready:        tombstones older than the grace period that are still gone.
    reappeared:         tombstoned rows the scan found again.
    attachment_changes: key -> new project_id. The key is the row's key after this plan's
                        moves, because attachment is applied after them.
    project_upserts:    marker dicts (path, owner, name, id, inherit_access, mode, tags),
                        plus 'parent' (the enclosing project's id, or None).
    project_removals:   project ids whose marker is no longer found.
    stale_rows:         non-completed rows whose source is gone. They have no points, so
                        their queue row is removed at once.
    reasons:            path -> why it is gone, for newly_missing and stale_rows.
    skipped:            a root could not be scanned. No moves, tombstones or removals this run.
    guard_tripped:      the mass-delete guard refused the tombstones this run.
    """

    moves: dict[str, str] = field(default_factory=dict)
    newly_missing: list[str] = field(default_factory=list)
    purge_ready: list[str] = field(default_factory=list)
    reappeared: list[str] = field(default_factory=list)
    attachment_changes: dict[str, str] = field(default_factory=dict)
    project_upserts: list[dict] = field(default_factory=list)
    project_removals: list[str] = field(default_factory=list)
    stale_rows: list[str] = field(default_factory=list)
    reasons: dict[str, str] = field(default_factory=dict)
    skipped: bool = False
    guard_tripped: bool = False


def _gone_reason(path: str, include_exts: frozenset[str]) -> str:
    """
    Best-effort reason a queued path is no longer part of the scan, for the log.

    The checks run from the most certain cause to the least certain. The last reason is
    a catch-all: a path that exists and has an included extension can only have been
    dropped by an exclusion rule, or by leaving the scanned tree.
    """
    if path.startswith("opencode://"):
        return "OpenCode session no longer returned by the API"
    # A claude-session:// key is not a path, so os.path.exists() would always report it missing.
    if path.startswith("claude-session://"):
        return "Claude Code session no longer found on disk"
    if not os.path.exists(path):
        return "file missing on disk"
    if not is_includable(path, include_exts):
        return "extension no longer in rag.scanner.include_extensions"
    return "excluded by config/ragignore or a .ragignore, or outside the scanned tree"


def _plan_projects(
    state_db: StateDB,
    markers: list[dict],
    owners: list[str] | None,
    skipped: bool,
) -> tuple[list[dict], list[str], list[tuple[str, str]]]:
    """
    Work out project upserts, removals and the project set used for attachment.

    Returns (upserts, removals, attach_projects). attach_projects is a list of (id, path)
    for every project that still exists after this run, markers included.

    Removal rule: an existing project whose marker was not found is removed, but only
    when no root was skipped (a skipped root may simply not have been visited) and only
    for owners that were scanned (`owners` is None for every owner). Its items are
    re-attached to the enclosing project or global by the attachment step of the same run.
    """
    # A marker is ignored when its id is reserved, or when an earlier marker already
    # claimed the id (a copied project directory). Both are logged loudly.
    kept_markers: list[dict] = []
    claimed_ids: set[str] = set()
    for marker in markers:
        if marker["id"] == GLOBAL_PROJECT_ID:
            logger.error(
                "project marker %s uses the reserved id '%s'; ignored",
                marker["path"], GLOBAL_PROJECT_ID,
            )
            continue
        if marker["id"] in claimed_ids:
            logger.error(
                "project marker %s reuses id %s already claimed by an earlier marker; ignored",
                marker["path"], marker["id"],
            )
            continue
        claimed_ids.add(marker["id"])
        kept_markers.append(marker)

    # The reserved 'global' row is never a marker project, so it is never removed.
    existing = [p for p in state_db.list_projects() if p["id"] != GLOBAL_PROJECT_ID]
    removals: list[str] = []
    if not skipped:
        for project in existing:
            if project["id"] in claimed_ids:
                continue
            if owners is not None and project["owner"] not in owners:
                continue
            removals.append(project["id"])
    removed = set(removals)

    # Projects that stay: existing ones with no marker this run, minus the removals.
    kept_existing = [
        p for p in existing if p["id"] not in claimed_ids and p["id"] not in removed
    ]

    # Parent resolution. A marker's parent is its longest enclosing project path
    # among every project that exists after this run. A marker row wins over a stored
    # row that shares its path.
    known_paths = [p["path"] for p in kept_existing if p["path"]] + [m["path"] for m in kept_markers]
    id_by_path = {p["path"]: p["id"] for p in kept_existing if p["path"]}
    id_by_path.update({m["path"]: m["id"] for m in kept_markers})

    upserts: list[dict] = []
    for marker in kept_markers:
        parent_path = resolve_parent(marker["path"], known_paths)
        parent_id = id_by_path[parent_path] if parent_path is not None else None
        upserts.append({**marker, "parent": parent_id})

    attach_projects = [(p["id"], p["path"]) for p in kept_existing if p["path"]] + [
        (m["id"], m["path"]) for m in kept_markers
    ]
    return upserts, removals, attach_projects


def _plan_vanished(
    state_db: StateDB,
    plan: SyncPlan,
    all_existing: set[str],
    new_hashes: dict[str, str],
    tombstones: dict[str, dict],
    owners: list[str] | None,
    include_exts: frozenset[str],
    max_delete_fraction: float,
    allow_mass_delete: bool,
) -> None:
    """
    Decide moves, tombstones and stale-row removals for everything that vanished this run.

    Only called when no root was skipped. A skipped root means the scan cannot tell
    a vanished file from one it simply did not see.
    """
    completed_gone, all_gone = state_db.find_deleted(all_existing, owners=owners)
    completed_gone_set = set(completed_gone)

    # A row tombstoned on an earlier sync is still gone, but it is not vanished now.
    # Counting it again would trip the guard on every run.
    vanished = [p for p in completed_gone if p not in tombstones]
    if not vanished:
        plan.stale_rows = [p for p in all_gone if p not in completed_gone_set]
        for path in plan.stale_rows:
            plan.reasons[path] = _gone_reason(path, include_exts)
        return

    # Moves. Only file keys can move. A session key is not a path, and a session must
    # never be re-keyed onto a NAS file that happens to share its content.
    all_hashes = state_db.completed_rows_with_content_hash()
    vanished_hashes = {
        path: all_hashes[path]
        for path in vanished
        if path in all_hashes and not path.startswith(SESSION_KEY_PREFIXES)
    }
    plan.moves = detect_moves(vanished_hashes, new_hashes)
    unmatched = [path for path in vanished if path not in plan.moves]

    # Mass-delete guard. It gates the tombstones (the new dangerous step). Moves never
    # delete anything, so they are not counted and are not blocked by it.
    completed_total = state_db.get_counts()["completed"]
    limit = max_delete_fraction * completed_total
    if len(unmatched) > limit:
        if allow_mass_delete:
            logger.warning(
                "Mass-delete guard overridden by --allow-mass-delete: %d completed file(s) "
                "to tombstone, limit %.0f (%.0f%% of %d completed)",
                len(unmatched), limit, max_delete_fraction * 100, completed_total,
            )
        else:
            logger.error(
                "Deletion phase SKIPPED: %d completed file(s) would be tombstoned, more than "
                "%.0f%% of the %d completed rows (limit %.0f). Re-run with --allow-mass-delete "
                "if this is intended. No rows or Qdrant points were changed by the deletion phase.",
                len(unmatched), max_delete_fraction * 100, completed_total, limit,
            )
            # Refused: no tombstones, and no queue rows removed either.
            plan.guard_tripped = True
            return

    plan.newly_missing = unmatched
    plan.stale_rows = [p for p in all_gone if p not in completed_gone_set]
    for path in plan.newly_missing + plan.stale_rows:
        plan.reasons[path] = _gone_reason(path, include_exts)


def _plan_attachment(
    state_db: StateDB,
    plan: SyncPlan,
    attach_projects: list[tuple[str, str]],
) -> None:
    """
    Record every completed item whose project_id differs from its closest enclosing project.

    Files match by their key; sessions match by launch_dir (None means global). Matching
    is verbatim (projects.project_id_for_path). Items queued for purge are skipped, since
    they are about to lose their points. Each change is keyed by the row's key after this
    plan's moves, because apply_sync_plan() applies the moves first.
    """
    purge_set = set(plan.purge_ready)
    for row in state_db.rows_for_attachment():
        path = row["file_path"]
        if path in purge_set:
            continue
        # A moved row carries its new key from here on.
        key = plan.moves.get(path, path)
        if path.startswith(SESSION_KEY_PREFIXES):
            match_path = row["launch_dir"]
        else:
            match_path = key
        if match_path:
            desired = project_id_for_path(match_path, attach_projects, GLOBAL_PROJECT_ID)
        else:
            desired = GLOBAL_PROJECT_ID
        if desired != row["project_id"]:
            plan.attachment_changes[key] = desired


def build_sync_plan(
    state_db: StateDB,
    scan_result: dict,
    config: dict,
    now_iso: str,
    owners: list[str] | None = None,
    allow_mass_delete: bool = False,
    grace_hours: int = DEFAULT_GRACE_HOURS,
) -> SyncPlan:
    """
    Work out every change one sync will make. Reads SQLite only.

    Args:
        state_db:         Open StateDB. Read only here.
        scan_result:      {"nfs": ..., "chats": ..., "opencode": ...}, the three scanners' returns.
        config:           Full ragnart.yaml dict. Reads rag.sync.max_delete_fraction and the include extensions.
        now_iso:          The run's time, ISO-8601 UTC. Written to missing_since, and the purge cutoff is taken from it.
        owners:           Owners in scope for this run, or None for every owner (see sync() --user).
        allow_mass_delete: True to let the mass-delete guard pass.
        grace_hours:      How long a tombstone stays searchable before its purge.

    Returns:
        The SyncPlan. Its skipped and guard_tripped flags tell the caller the deletion phase was refused.
    """
    nfs = scan_result["nfs"]
    chats = scan_result["chats"]
    opencode = scan_result["opencode"]
    include_exts = include_extensions_from_config(config)
    max_delete_fraction = config["rag"]["sync"]["max_delete_fraction"]
    plan = SyncPlan()

    # What the scan actually saw this run, and what still counts as existing. An OpenCode
    # API that cannot be read keeps its queued rows alive, but they are not "seen".
    seen = nfs["existing_paths"] | chats["existing_paths"]
    if opencode["existing_paths"] is not None:
        seen = seen | opencode["existing_paths"]
        preserved: set[str] = set()
    else:
        preserved = state_db.paths_with_prefix("opencode://")
        logger.warning(
            "Opencode API unreachable — preserving %d existing opencode session(s) "
            "in the deletion check",
            len(preserved),
        )
    all_existing = seen | preserved

    # Skipped roots: a root not scannable this run. Its rows would look gone.
    skipped_roots = nfs["skipped_roots"] + chats.get("skipped_roots", [])
    plan.skipped = bool(skipped_roots)
    if plan.skipped:
        logger.error(
            "Deletion phase SKIPPED: root(s) not scannable this run: %s. "
            "No moves or tombstones were planned and no rows or Qdrant points were deleted.",
            ", ".join(skipped_roots),
        )

    # Projects. Markers are upserted even when a root was skipped: they only describe
    # what is present, and a skipped root only ever hides markers (so no removal).
    plan.project_upserts, plan.project_removals, attach_projects = _plan_projects(
        state_db, nfs["project_markers"], owners, plan.skipped,
    )

    # Reappearance. Only the rows the scan actually saw can be found again.
    tombstones = state_db.tombstoned_rows()
    plan.reappeared = sorted(path for path in tombstones if path in seen)

    # Purge. The grace period is measured from missing_since, and the purge is not
    # guarded by skipped roots or by the mass-delete guard: those rows were vetted when
    # they were tombstoned. A row found again is excluded (it is in all_existing).
    cutoff = (datetime.fromisoformat(now_iso) - timedelta(hours=grace_hours)).isoformat()
    plan.purge_ready = [
        path
        for path in state_db.rows_missing_since_before(cutoff)
        if path not in all_existing
        and (owners is None or tombstones[path]["owner"] in owners)
    ]

    # Moves and tombstones need a complete view of the sources.
    if not plan.skipped:
        _plan_vanished(
            state_db, plan, all_existing, nfs["new_hashes"], tombstones,
            owners, include_exts, max_delete_fraction, allow_mass_delete,
        )

    _plan_attachment(state_db, plan, attach_projects)
    return plan


# ---------------------------------------------------------------------------
# Qdrant helpers. All waits are wait=True: SQLite is written only after Qdrant confirms.
# ---------------------------------------------------------------------------


def _source_filter(source_path: str) -> Filter:
    """Filter matching every point stored for one source_path value."""
    return Filter(
        must=[FieldCondition(key=FIELD_SOURCE_PATH, match=MatchValue(value=source_path))]
    )


def _set_payload_for_path(qdrant_client: Any, source_path: str, payload: dict) -> None:
    """Merge `payload` into every point currently stored under source_path."""
    qdrant_client.set_payload(
        collection_name=DOCUMENTS_COLLECTION,
        payload=payload,
        points=_source_filter(source_path),
        wait=True,
    )


def _delete_points_for_path(qdrant_client: Any, source_path: str) -> None:
    """Delete every point stored under source_path. Any exception propagates; SQLite keeps its row."""
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
    qdrant_client: Any,
    plan: SyncPlan,
    now_iso: str,
) -> None:
    """
    Carry out a plan. Order matters; each step is safe to repeat after a crash.

    1. Projects: upsert markers and their tags; remove projects whose marker is gone.
    2. Moves: delete the scan's pending row at the new path, set source_path on the
       old key's points (Qdrant, wait=True), then rename the SQLite row and its links.
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
    for marker in plan.project_upserts:
        state_db.upsert_project(
            marker["id"],
            marker["path"],
            marker["name"],
            marker["parent"],
            marker["owner"],
            marker["inherit_access"],
            marker["mode"],
        )
        state_db.replace_project_tags(marker["id"], marker["tags"])
    for project_id in plan.project_removals:
        state_db.remove_project(project_id)
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
        state_db.rename_source_links(old_path, new_path)
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


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _section(title: str, entries: list[str]) -> list[str]:
    """One titled block of the plan: the count, then each entry indented under it."""
    return [f"  {title}: {len(entries)}"] + [f"    {entry}" for entry in entries]


def format_plan(plan: SyncPlan) -> str:
    """Render a plan as human-readable text. Used for the sync log and for --dry-run."""
    lines = [
        "Sync plan",
        f"  roots skipped (no moves, tombstones or removals): {'yes' if plan.skipped else 'no'}",
        f"  mass-delete guard tripped (tombstones refused): {'yes' if plan.guard_tripped else 'no'}",
    ]
    lines += _section(
        "project upserts",
        [
            f"{m['id']}  {m['path']}  name={m['name']}  parent={m['parent'] or 'none'}"
            for m in plan.project_upserts
        ],
    )
    lines += _section("project removals", plan.project_removals)
    lines += _section("moves", [f"{old} -> {new}" for old, new in plan.moves.items()])
    lines += _section(
        "newly missing (tombstoned, points kept)",
        [f"{p}  ({plan.reasons.get(p, 'gone')})" for p in plan.newly_missing],
    )
    lines += _section("reappeared (tombstone cleared)", plan.reappeared)
    lines += _section("purge (grace period over: points and row removed)", plan.purge_ready)
    lines += _section(
        "stale queue rows removed (no points)",
        [f"{p}  ({plan.reasons.get(p, 'gone')})" for p in plan.stale_rows],
    )
    lines += _section(
        "attachment changes",
        [f"{path} -> {project_id}" for path, project_id in plan.attachment_changes.items()],
    )
    return "\n".join(lines)
