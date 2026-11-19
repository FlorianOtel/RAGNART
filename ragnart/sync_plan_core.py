"""
Sync plan computation for the NFS, Claude Code and OpenCode scans (RESEARCH C3, C4, C6, C10; design §4.5, §10.3).

This module computes the plan and renders it; it writes nothing. It imports no qdrant_client, so the
ingestion side (endpoint hosts included) can plan a sync without the hub Qdrant stack. Carrying the plan
out is ragnart/sync_submit.py submit_plan(), which sends each change through the ingestion API.

build_sync_plan()   reads the scan result and the queue, and works out every change
                    this run would make. It writes nothing.
format_plan()       renders the plan for the sync log and for --dry-run.

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

from .moves import detect_moves
from .projects import project_id_for_path
from .registry import RegistryDB
from .scanner import include_extensions_from_config, is_includable
from .schema import GLOBAL_PROJECT_ID, SOURCE_ID_NAS
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
    Every change one sync run will make. Built by build_sync_plan(), carried out by ragnart/sync_submit.py submit_plan().

    moves:              old key -> new key. Same content, one vanished and one new item.
    newly_missing:      completed rows that vanished this run. They are tombstoned, not deleted.
    purge_ready:        tombstones older than the grace period that are still gone.
    reappeared:         tombstoned rows the scan found again.
    attachment_changes: key -> new project_id. The key is the row's key after this plan's
                        moves, because attachment is applied after them.
    project_upserts:    marker dicts (path, owner, name, id, inherit_access, mode, tags).
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
    registry_db: RegistryDB,
    markers: list[dict],
    owners: list[str] | None,
    skipped: bool,
) -> tuple[list[dict], list[str], list[tuple[str, str]]]:
    """
    Work out project upserts, removals and the project set used for attachment.

    Projects live in the hub registry, so the existing ones are read from registry_db.

    Returns (upserts, removals, attach_projects). attach_projects is a list of (id, path)
    for every project that still exists after this run, markers included.

    Removal rule: an existing project whose marker was not found is removed, but only
    when no root was skipped (a skipped root may simply not have been visited), only
    for owners that were scanned (`owners` is None for every owner), and only when the
    project is homed on the NAS source. A project homed on another source (a MBP or
    claude-linux-shared-nfs project) was never found by this crawl, so its absence here
    says nothing about it, and it is never planned for removal. Its items are
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
    existing = [p for p in registry_db.list_projects() if p["id"] != GLOBAL_PROJECT_ID]
    removals: list[str] = []
    if not skipped:
        for project in existing:
            if project["id"] in claimed_ids:
                continue
            # Only a project homed on the NAS source is this crawl's to remove. home_source_id is
            # compared case-insensitively, as the registry compares source ids (ragnart/scope.py).
            # A NULL home (no source recorded) does not match, so it is never removed.
            if (project["home_source_id"] or "").casefold() != SOURCE_ID_NAS.casefold():
                continue
            if owners is not None and project["owner"] not in owners:
                continue
            removals.append(project["id"])
    removed = set(removals)

    # Projects that stay: existing ones with no marker this run, minus the removals.
    kept_existing = [
        p for p in existing if p["id"] not in claimed_ids and p["id"] not in removed
    ]

    # Attachment needs every project that exists after this run: the kept stored ones and the markers.
    attach_projects = [(p["id"], p["path"]) for p in kept_existing if p["path"]] + [
        (m["id"], m["path"]) for m in kept_markers
    ]
    return kept_markers, removals, attach_projects


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
    plan moves, because submit_plan() sends the moves first.
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
    registry_db: RegistryDB,
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
        registry_db:      Open RegistryDB. Read only here: it supplies the existing projects.
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
        registry_db, nfs["project_markers"], owners, plan.skipped,
    )

    # Reappearance. Only the rows the scan actually saw can be found again.
    tombstones = state_db.tombstoned_rows()
    plan.reappeared = sorted(path for path in tombstones if path in seen)

    # Purge. The grace period is measured from missing_since, and the purge is not
    # guarded by skipped roots or by the mass-delete guard: those rows were vetted when
    # they were tombstoned. A row found again is excluded (it is in all_existing).
    # Invariant: every timestamp is written by datetime.now(timezone.utc).isoformat(), so the
    # string order used by rows_missing_since_before() is time order.
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
            f"{m['id']}  {m['path']}  name={m['name']}"
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
