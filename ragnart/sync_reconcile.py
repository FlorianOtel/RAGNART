"""
Reconcile and rebuild for the NAS ingestion side (v0.3 session B2, steps A.10 and A.10b).

Two entry points, both per source (nas, claude-linux-shared-nfs, opencode-k11):

reconcile_source()     compares the hub's listing (GET /v1/items) with the local rows of rag_state.db and
                       brings the hub in line with them. It writes the local rows only when a row must
                       be sent again (it sets the row pending, and the normal submit path sends it). It
                       never touches Qdrant. It runs after the scan and the submit of each NAS run.
rebuild_from_hub()     repopulates an empty rag_state.db from the hub's listing, after the state database
                       was lost. It sends nothing to the hub.

Reconcile rules, applied to each source in this order:

  (a) ORPHAN. A hub item with no local row (any status) is deleted on the hub. Guards:
        - no orphan is deleted when the local state holds no completed row for the source (an empty or
          lost state must never delete the hub's items);
        - no orphan is deleted when their number is more than rag.sync.max_delete_fraction of the source's own
          completed rows (the same fraction the sync's mass-delete guard uses, but per source). A WARNING names the count.
      A pending, processing or ignored row is not an orphan: it has a local row.
      Note: after a move, the hub may still list the old key until its change_key is applied. The
      delete then sits behind the change_key in the intake queue (the per-key order of
      ragnart/registry.py next_intake), so it finds nothing left to delete.

  (b) MISSING. A completed local row whose item is still present (the scan saw it) and that the hub does
      not list is set pending again, so the submit path sends it. Exception: a completed row with
      chunk_count 0 is skipped. A zero-chunk completion has no points, so the hub never lists it; resubmitting
      it would loop forever. A NULL chunk_count is not zero (never recorded) and stays eligible.

  (c) OLDER VERSION. A completed, present row whose hub pipeline_version is older than GET /v1/info's
      (ragnart/schema.py pipeline_version_is_older) is set pending again. At most `resubmit_batch_size`
      rows are re-sent in one run (default 200), so a version change never spools the whole corpus at once.

  (d) DRIFT. A completed row whose hub owner or project_id differs from the local value gets a
      change_metadata request carrying the local value of each differing field.

Keys are compared exactly. In dry-run mode every rule is counted, and nothing is sent or written.

Rebuild rules (rebuild_from_hub). Each hub item that is restored becomes a completed row, with its key, owner,
chunk count, content hash, pipeline version, project_id, source_type and file_type. The rebuild is refused
(nothing read, nothing written) when rag_state.db holds rows, or when a NAS crawl root (the scanner's
nfs_roots_from_config list) or a claude_chats root is not a readable directory. All rows are then written in
one transaction (StateDB.insert_completed_rows): all of them, or none.
  - NAS file on disk, md5 equal to the hub's content hash: restored with the file's current mtime, size, md5
    and hash time, so the scanner does not queue it again. The md5 is computed with the scanner's hash_file.
  - NAS file on disk, md5 different, or unreadable: NO row. The next scan queues it as new and submits it.
  - NAS file not on disk: restored as completed with last_modified 0, so the next sync tombstones it as any
    vanished file.
  - Claude session, found on disk: the row is restored only when the hub hash matches either the md5 of
    the rendered text (the rule for points written from B2 on) or the md5 of the .jsonl bytes (the rule
    for points written before B2, which the operator chose to keep until they are re-submitted). A session
    whose hash does not match is NOT restored: the scanner then queues it as new and it is submitted again.
  - Claude session, not on disk: restored as completed with last_modified 0, so it is tombstoned by the
    next sync like any vanished session.
  - OpenCode session: NOT restored. It carries no hash that could be checked, so the next scan queues it as
    new and it is submitted again (the operator-accepted cost of the rebuild).
  - An item with no owner cannot be restored (the queue's owner column is NOT NULL): it is counted and
    skipped with a WARNING.

Dependencies: httpx (through ragnart.endpoint_client and ragnart.sync_submit), the chat renderer
(ragnart.chat_render, docling-free), the scanner's key rule, its Claude winner walk and its NAS root list, and
the state module. No qdrant_client, docling or embedder is loaded. The import contract is enforced by
tests/test_ingestion_side_imports.py.
"""

from __future__ import annotations

import logging
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from .chat_render import _parse_claude_chat
from .endpoint_client import HubClient
from .hashing import hash_file, hash_text
from .scanner import collect_claude_session_winners, nfs_roots_from_config
from .schema import (
    DEFAULT_RESUBMIT_BATCH_SIZE,
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
    pipeline_version_is_older,
    source_id_for_key,
)
from .state import StateDB
from .sync_submit import HUB_ERRORS, REQUIRED_SOURCES

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers shared by both entry points
# ---------------------------------------------------------------------------

def present_keys(scan_result: dict) -> set[str]:
    """
    The keys the scan saw this run: NAS files, Claude sessions, and OpenCode sessions when the API answered.

    scan_result has the shape build_sync_plan() takes: {"nfs", "chats", "opencode"}. An unreachable OpenCode
    API reports existing_paths None, so none of its sessions count as present (rule (b) then leaves them alone).
    """
    seen = set(scan_result["nfs"]["existing_paths"]) | set(scan_result["chats"]["existing_paths"])
    if scan_result["opencode"]["existing_paths"] is not None:
        seen |= set(scan_result["opencode"]["existing_paths"])
    return seen


def _local_rows_of_source(state_db: StateDB, source_id: str) -> dict[str, dict]:
    """The rows of one source, keyed by file_path. A row belongs to the source its key names (source_id_for_key)."""
    return {
        row["file_path"]: row
        for row in state_db.rows_for_reconcile()
        if source_id_for_key(row["file_path"]) == source_id
    }


# ---------------------------------------------------------------------------
# Reconcile (rules a to d)
# ---------------------------------------------------------------------------

def _delete_orphans(
    state_db: StateDB,
    client: HubClient,
    source_id: str,
    hub: dict[str, dict],
    local: dict[str, dict],
    max_delete_fraction: float,
    dry_run: bool,
    counts: Counter,
) -> None:
    """Rule (a). Deletes the hub items with no local row, unless one of the two guards refuses the deletion."""
    orphans = sorted(key for key in hub if key not in local)
    if not orphans:
        return

    # Guard 1: an empty state (no completed row for this source) must never delete the hub's items.
    source_completed = sum(1 for row in local.values() if row["status"] == "completed")
    if source_completed == 0:
        counts["orphan_skipped_no_completed_rows"] += len(orphans)
        logger.warning(
            "%s: %d orphan(s) on the hub, but the local state holds no completed row for this source; "
            "no delete sent", source_id, len(orphans),
        )
        return

    # Guard 2: the mass-delete limit, as a fraction of THIS source's completed rows (rag.sync.max_delete_fraction).
    # The denominator is per source: a large completed count in another source must not let this source's
    # orphans through, and a small one in another source must not block this source's own deletions.
    limit = max_delete_fraction * source_completed
    if len(orphans) > limit:
        counts["orphan_guard_tripped"] += len(orphans)
        logger.warning(
            "%s: %d orphan(s) on the hub, more than the mass-delete limit %.0f (%.0f%% of %d completed rows of this "
            "source); no delete sent. Check the state before any delete is allowed.",
            source_id, len(orphans), limit, max_delete_fraction * 100, source_completed,
        )
        return

    for key in orphans:
        if dry_run:
            counts["orphan_would_delete"] += 1
            continue
        try:
            client.delete(key)
        except HUB_ERRORS as exc:
            counts["orphan_failed"] += 1
            logger.warning("%s: orphan %s not deleted (retried next run): %s", source_id, key, exc)
            continue
        counts["orphan_deleted"] += 1
        logger.info("%s: deleted orphan %s (no local row)", source_id, key)


def _resubmit_missing(
    state_db: StateDB,
    source_id: str,
    hub: dict[str, dict],
    local: dict[str, dict],
    present: set[str],
    dry_run: bool,
    counts: Counter,
) -> None:
    """Rule (b). A completed row whose item is present on disk but not listed by the hub goes back to pending."""
    for key in sorted(local):
        row = local[key]
        if row["status"] != "completed" or key in hub or key not in present:
            continue
        # A completion with zero chunks is a real, final outcome (for example an empty transcript). It has no
        # points, so GET /v1/items can never list it. Resubmitting it would repeat on every run and end in the
        # same zero-chunk completion, so it is left alone. A NULL chunk_count (never recorded) is not zero, so it stays eligible.
        if row["chunk_count"] == 0:
            counts["missing_zero_chunks_skipped"] += 1
            continue
        if dry_run:
            counts["missing_would_resubmit"] += 1
            continue
        state_db.reset_to_pending(key)
        counts["missing_resubmitted"] += 1
        logger.info("%s: %s is not listed by the hub; submitting it again", source_id, key)


def _resubmit_older_versions(
    state_db: StateDB,
    source_id: str,
    hub: dict[str, dict],
    local: dict[str, dict],
    present: set[str],
    current_version: str,
    batch_size: int,
    dry_run: bool,
    counts: Counter,
) -> None:
    """Rule (c). At most batch_size completed, present rows with an older hub pipeline version go back to pending."""
    stale = [
        key for key in sorted(hub)
        if key in local
        and local[key]["status"] == "completed"
        and key in present
        and pipeline_version_is_older(hub[key]["pipeline_version"], current_version)
    ]
    # The rest waits for a later run, so each run stays within the batch size.
    batch, deferred = stale[:batch_size], stale[batch_size:]
    counts["version_deferred"] += len(deferred)
    for key in batch:
        if dry_run:
            counts["version_would_resubmit"] += 1
            continue
        state_db.reset_to_pending(key)
        counts["version_resubmitted"] += 1
    if deferred:
        logger.info(
            "%s: %d item(s) on an older pipeline version; %d resubmitted this run, %d deferred",
            source_id, len(stale), len(batch), len(deferred),
        )


def _fix_drift(
    client: HubClient,
    source_id: str,
    hub: dict[str, dict],
    local: dict[str, dict],
    dry_run: bool,
    counts: Counter,
) -> None:
    """Rule (d). A completed row whose owner or project_id the hub holds differently gets change_metadata with the local values."""
    for key in sorted(hub):
        row = local.get(key)
        if row is None or row["status"] != "completed":
            continue
        item = hub[key]
        # Only the fields that differ are sent. The client sends whatever it is given.
        fields = {}
        if item["owner"] != row["owner"]:
            fields["owner"] = row["owner"]
        if item["project_id"] != row["project_id"]:
            fields["project_id"] = row["project_id"]
        if not fields:
            continue
        if dry_run:
            counts["drift_would_fix"] += 1
            continue
        try:
            client.change_metadata(key, **fields)
        except HUB_ERRORS as exc:
            counts["drift_failed"] += 1
            logger.warning("%s: metadata of %s not corrected (retried next run): %s", source_id, key, exc)
            continue
        counts["drift_fixed"] += 1
        logger.info("%s: corrected %s on the hub: %s", source_id, key, sorted(fields))


def reconcile_source(
    state_db: StateDB,
    client: HubClient,
    source_id: str,
    present: set[str],
    max_delete_fraction: float,
    *,
    resubmit_batch_size: int = DEFAULT_RESUBMIT_BATCH_SIZE,
    dry_run: bool = False,
    hub_items: list[dict] | None = None,
) -> Counter:
    """
    Reconcile one source's hub listing with its local rows (rules a to d above). Returns how many of each outcome.

    Args:
        state_db:            The NAS side's rag_state.db. Written only where a row is set pending.
        client:              The HubClient of this source (its secret selects the source on the hub).
        source_id:           The SOURCE_ID_* of the source. Local rows are filtered to it.
        present:             Keys the scan saw this run (see present_keys()).
        max_delete_fraction: rag.sync.max_delete_fraction, the orphan guard's limit. Applied to this source's
                             own completed rows.
        resubmit_batch_size: The most rule (c) resubmissions in one run. Must be at least 1.
        dry_run:             True to count what would happen, without sending or writing anything.
        hub_items:           The source's listing, when the caller has already fetched it (so it can time the
                             listing). None (the default) makes this function fetch it with client.items().

    Returns:
        A Counter. Keys: orphan_deleted, orphan_failed, orphan_skipped_no_completed_rows, orphan_guard_tripped,
        missing_resubmitted, missing_zero_chunks_skipped, version_resubmitted, version_deferred, drift_fixed,
        drift_failed, and inconsistent_items (items whose points disagree). In dry-run the keys end in _would_*.

    Raises:
        ValueError: resubmit_batch_size is below 1. Nothing is read or sent.
        HubError or httpx.HTTPError: the listing or the info could not be read. Nothing is sent.
    """
    if resubmit_batch_size < 1:
        raise ValueError(f"resubmit_batch_size must be at least 1; got {resubmit_batch_size}")

    current_version = client.info()["pipeline_version"]
    listing = client.items() if hub_items is None else hub_items
    hub = {item["key"]: item for item in listing}
    local = _local_rows_of_source(state_db, source_id)

    counts: Counter = Counter()
    # Items whose points disagree are reported, not hidden. The first point's values are used (see ingest_api.py).
    for item in hub.values():
        if item["inconsistent"]:
            counts["inconsistent_items"] += 1
            logger.warning("%s: %s has points that disagree on %s", source_id, item["key"], item["inconsistent"])

    _delete_orphans(state_db, client, source_id, hub, local, max_delete_fraction, dry_run, counts)
    _resubmit_missing(state_db, source_id, hub, local, present, dry_run, counts)
    _resubmit_older_versions(
        state_db, source_id, hub, local, present, current_version, resubmit_batch_size, dry_run, counts,
    )
    _fix_drift(client, source_id, hub, local, dry_run, counts)

    logger.info("%s: reconcile %s %s", source_id, "(dry run)" if dry_run else "", dict(sorted(counts.items())))
    return counts


# ---------------------------------------------------------------------------
# Rebuild (step A.10b)
# ---------------------------------------------------------------------------

def _unreadable_nas_roots(config: dict, user_filter: str | None = None) -> list[str]:
    """
    The NAS crawl roots that are not a readable directory, from the scanner's own root list.

    nfs_roots_from_config() is the list scan_nfs_roots() walks: the per-user roots, then the shared
    roots, with user_filter applied the same way. A root that is missing, not a directory, or not
    readable is returned. The rebuild refuses when this list is not empty.
    """
    unreadable: list[str] = []
    for root, _owner in nfs_roots_from_config(config, user_filter):
        # A directory is listed only when it can be read (R_OK) and entered (X_OK).
        if not (os.path.isdir(root) and os.access(root, os.R_OK | os.X_OK)):
            unreadable.append(root)
    return unreadable


def claude_session_paths(config: dict, user_filter: str | None = None) -> dict[str, tuple[float, str]]:
    """
    Map each Claude session key to the (mtime, path) of its winning transcript, read without writing.

    The winner rule is collect_claude_session_winners() in ragnart/scanner.py. The scanner uses the same
    walk to write the queue, so the rebuild and the scan always pick the same copy of a session.

    Raises:
        RuntimeError: a configured root is not a directory. The rebuild must not treat its sessions as gone.
    """
    roots_cfg = config.get("claude_chats", {}).get("roots", [])
    winners = collect_claude_session_winners(roots_cfg, user_filter)
    if winners.unreachable_roots:
        raise RuntimeError(f"claude_chats root not accessible, rebuild refused: {winners.unreachable_roots!r}")
    return {key: (mtime, path) for key, (mtime, path, _owner) in winners.best.items()}


def _completed_row(
    item: dict,
    source_id: str,
    last_modified: float,
    origin_path: str | None,
    launch_dir: str | None,
    size_bytes: int | None = None,
    hashed_at: str | None = None,
) -> dict:
    """
    The queue row for one hub item, as completed. The hub supplies the item's values; the local fields
    (last_modified, origin_path, launch_dir, and the size and hash time of a file) are given by the caller.
    The dict is written by StateDB.insert_completed_rows(), which leaves out any None value.
    """
    return {
        "file_path": item["key"],
        "owner": item["owner"],
        "last_modified": last_modified,
        "origin_path": origin_path,
        "source_type": item["source_type"],
        "file_type": item["file_type"],
        "source_id": source_id,
        "size_bytes": size_bytes,
        "content_hash": item["content_hash"],
        "hashed_at": hashed_at,
        "chunk_count": item["chunk_count"],
        "pipeline_version": item["pipeline_version"],
        "project_id": item["project_id"],
        "launch_dir": launch_dir,
    }


def _restore_file(item: dict, counts: Counter) -> dict | None:
    """
    A NAS file. Returns its completed row, or None when no row should be written.

      - not on disk: completed with last_modified 0, so the next sync tombstones it as for any vanished file.
      - on disk, md5 equal to the hub's content_hash: completed with the file's current mtime, size, md5 and
        hash time. The scanner then sees no change and does not queue it again.
      - on disk, md5 different, or unreadable: no row. The scanner queues it as new and it is submitted again.

    The file is stat-ed before it is hashed. If it changes during the hash, the stored mtime is the older one,
    so the scanner sees a change and queues the file again. That is the safe direction.
    """
    key = item["key"]
    try:
        last_modified = os.stat(key).st_mtime
    except FileNotFoundError:
        counts["file_gone"] += 1
        return _completed_row(item, SOURCE_ID_NAS, 0.0, origin_path=key, launch_dir=None)
    except OSError as exc:
        counts["file_unreadable"] += 1
        logger.warning("%s not restored (it cannot be stat-ed): %s", key, exc)
        return None

    try:
        size_bytes, md5 = hash_file(key)
    except OSError as exc:
        counts["file_unreadable"] += 1
        logger.warning("%s not restored (it cannot be read): %s", key, exc)
        return None

    if md5 != item["content_hash"]:
        # The file changed since the hub's ingest: no row, so the scan queues and resubmits it.
        counts["file_resubmit"] += 1
        return None

    counts["file_on_disk"] += 1
    hashed_at = datetime.now(timezone.utc).isoformat()
    return _completed_row(
        item, SOURCE_ID_NAS, last_modified, origin_path=key, launch_dir=None,
        size_bytes=size_bytes, hashed_at=hashed_at,
    )


def _restore_session(item: dict, found: tuple[float, str] | None, counts: Counter) -> dict | None:
    """
    A Claude session. Returns its completed row, or None when no row should be written (see the module docstring).
    """
    if found is None:
        # Gone from disk: restored with last_modified 0, so the next sync tombstones it, as for any vanished session.
        counts["session_gone"] += 1
        return _completed_row(item, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, 0.0, origin_path=None, launch_dir=None)

    mtime, path = found
    try:
        text, _metadata, launch_dir = _parse_claude_chat(Path(path))
        # The two accepted hashes: the rendered text (B2 rule), or the .jsonl bytes (points written before B2).
        matches = item["content_hash"] == hash_text(text) or item["content_hash"] == hash_file(path)[1]
    except (OSError, ValueError) as exc:
        counts["session_unreadable"] += 1
        logger.warning("claude session %s not restored (its transcript cannot be read): %s", item["key"], exc)
        return None

    if not matches:
        # No row: the scan queues the session as new, and the submit path sends it again.
        counts["session_resubmit"] += 1
        return None
    counts["session_restored"] += 1
    return _completed_row(
        item, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, mtime, origin_path=path, launch_dir=launch_dir,
    )


def rebuild_from_hub(
    state_db: StateDB,
    clients: dict[str, HubClient],
    config: dict,
    *,
    user_filter: str | None = None,
) -> Counter:
    """
    Repopulate an empty rag_state.db from the hub's listing of each source. Sends nothing to the hub.

    Args:
        state_db: The NAS side's rag_state.db. It must hold no rows.
        clients:  One HubClient per source id (SOURCE_ID_* keys, see REQUIRED_SOURCES).
        config:   The full ragnart.yaml dict. Reads the NAS roots (users and shared) and claude_chats.roots.
        user_filter: Limit the walks to one owner's roots, as the scan does (--user).

    Returns:
        How many items of each kind were restored, skipped or left for a resubmission, and rows_written.

    Raises:
        RuntimeError: the state database already holds rows (nothing is read from the hub), or a NAS crawl
                      root or a claude root is not a readable directory (nothing is read or written).
        ValueError:   a source has no client (nothing is read from the hub).
    """
    total = state_db.get_counts()["total"]
    if total:
        raise RuntimeError(f"refusing to rebuild: rag_state.db already holds {total} row(s)")
    missing = [source for source in REQUIRED_SOURCES if source not in clients]
    if missing:
        raise ValueError(f"no HubClient for source(s): {', '.join(missing)}")

    # A NAS root that cannot be read would make its files look gone, and the next sync would tombstone them.
    # So the rebuild stops before reading anything when a root is unreadable.
    unreadable = _unreadable_nas_roots(config, user_filter)
    if unreadable:
        raise RuntimeError(f"NAS crawl root not readable, rebuild refused: {unreadable!r}")
    sessions = claude_session_paths(config, user_filter)
    listings = {source_id: clients[source_id].items() for source_id in REQUIRED_SOURCES}

    # Everything is read and decided before anything is written. The rows are then written in one
    # transaction: a failure leaves the state empty, so the rebuild can simply be run again.
    counts: Counter = Counter()
    rows: list[dict] = []
    for source_id in REQUIRED_SOURCES:
        for item in listings[source_id]:
            key = item["key"]
            if item["owner"] is None:
                counts["skipped_no_owner"] += 1
                logger.warning("%s: %s has no owner on the hub; not restored", source_id, key)
                continue
            if source_id == SOURCE_ID_OPENCODE_K11:
                # No checkable hash: no row, so the next scan queues the session as new.
                counts["opencode_resubmit"] += 1
                continue
            if source_id == SOURCE_ID_CLAUDE_LINUX_SHARED_NFS:
                row = _restore_session(item, sessions.get(key), counts)
            else:
                row = _restore_file(item, counts)
            if row is not None:
                rows.append(row)

    state_db.insert_completed_rows(rows)
    counts["rows_written"] = len(rows)
    logger.info("rebuild from hub: %s", dict(sorted(counts.items())))
    return counts
