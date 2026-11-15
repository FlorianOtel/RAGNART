#!/usr/bin/env python3
"""
Scan configured NFS roots and populate the RAG ingestion queue.

Handles all cases uniformly:

  - New file:    not yet in SQLite → inserted as 'pending'.
  - Modified:    disk mtime > stored mtime → reset to 'pending' (any status,
                 including 'ignored' — the file was replaced on disk).
  - Failed:      status = 'failed', mtime unchanged → reset to 'pending'
                 with retry_count = 0 so the daemon gives it a fresh run.
  - Ignored:     status = 'ignored', mtime unchanged → no-op (permanent skip).
  - Completed / pending / processing, mtime unchanged: no-op.

Files removed from NFS, or newly matched by an exclusion rule (config/ragignore or
a .ragignore file), or no longer on the include_extensions whitelist, are deleted
from SQLite and their Qdrant points purged.

Deletion is guarded in two ways. Both skip the whole deletion phase, log ERROR and
exit 1. The queueing done by the scan is kept.
  - A source root could not be scanned this run: an NFS root (missing, or empty
    while the queue still holds rows under it) or a claude_chats root (missing).
    Its rows would look gone, so nothing is deleted.
  - The completed rows that would be deleted exceed rag.sync.max_delete_fraction of
    all completed rows. Pass --allow-mass-delete to proceed anyway.

Usage (run from project root):
    python utils/rag_sync_nfs.py
    python utils/rag_sync_nfs.py --user florian
    python utils/rag_sync_nfs.py --user florian --allow-mass-delete
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Project root is one directory up from utils/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml  # noqa: E402  (after sys.path fix)

from qdrant_client.models import FieldCondition, Filter, FilterSelector, MatchValue  # noqa: E402

from ragnart.collection import DOCUMENTS_COLLECTION, get_client  # noqa: E402
from ragnart.lock import acquire_or_exit, lock_path_from_config  # noqa: E402
from ragnart.scanner import is_includable, scan_claude_chats, scan_nfs_roots, scan_opencode_sessions  # noqa: E402
from ragnart.schema import FIELD_SOURCE_PATH  # noqa: E402
from ragnart.state import StateDB  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Exit code when the deletion phase was refused (skipped root or mass-delete guard).
EXIT_DELETION_REFUSED = 1


def _deletion_reason(path: str, include_exts: frozenset[str]) -> str:
    """
    Best-effort reason a queued path is no longer part of the scan, for the log.

    The checks run from the most certain cause to the least certain. The last
    reason is a catch-all: a path that exists and has an included extension can
    only have been dropped by an exclusion rule, or by leaving the scanned tree.
    """
    if path.startswith("opencode://"):
        return "OpenCode session no longer returned by the API"
    # A claude-session:// key is not a path, so os.path.exists() below would always
    # report it missing. Its transcript was simply not found in any claude_chats root.
    if path.startswith("claude-session://"):
        return "Claude Code session no longer found on disk"
    if not os.path.exists(path):
        return "file missing on disk"
    if not is_includable(path, include_exts):
        return "extension no longer in rag.scanner.include_extensions"
    return "excluded by config/ragignore or a .ragignore, or outside the scanned tree"


def _delete_path_from_qdrant(client, path: str, file_num: int, total: int, reason: str) -> None:
    """
    Delete all Qdrant points for one source path, and wait until Qdrant confirms.

    wait=True: the call returns only after Qdrant has applied the delete. A
    fire-and-forget delete (wait=False) could leave points behind with no queue row
    left to retry them, so the caller must not continue until this call returns.
    Any exception means the delete did not complete, so it propagates and the script
    aborts. SQLite rows survive, and the next sync re-submits.
    """
    client.delete(
        collection_name=DOCUMENTS_COLLECTION,
        points_selector=FilterSelector(
            filter=Filter(
                must=[
                    FieldCondition(
                        key=FIELD_SOURCE_PATH,
                        match=MatchValue(value=path),
                    )
                ]
            )
        ),
        wait=True,
    )
    logger.info("Qdrant cleanup %d/%d: deleted points for %s (%s)", file_num, total, path, reason)


def sync(config: dict, state_db: StateDB, args: argparse.Namespace) -> int:
    """
    Run one sync: scan every source, queue new or modified files, then remove what has gone.

    Args:
        config:   Full ragnart.yaml dict.
        state_db: Open StateDB. The caller owns it and closes it.
        args:     Parsed arguments. Reads args.user (str or None) and
                  args.allow_mass_delete (bool).

    Returns:
        0 on success. EXIT_DELETION_REFUSED when the deletion phase was skipped.
        In that case the queueing done by the scan still stands.
    """
    include_exts = frozenset(config["rag"]["scanner"]["include_extensions"])
    max_delete_fraction = config["rag"]["sync"]["max_delete_fraction"]

    result_nfs = scan_nfs_roots(state_db, config, user_filter=args.user)
    result_chats = scan_claude_chats(state_db, config, user_filter=args.user)
    result_opencode = scan_opencode_sessions(state_db, config, user_filter=args.user)

    all_existing = result_nfs["existing_paths"] | result_chats["existing_paths"]
    if result_opencode["existing_paths"] is not None:
        all_existing |= result_opencode["existing_paths"]
    else:
        # The OpenCode API could not be scanned. Count every already-queued
        # opencode:// row as still existing, so find_deleted() does not treat
        # them as gone (before 2026-11-04 it did, and a 401 deleted them all).
        preserved = state_db.paths_with_prefix("opencode://")
        all_existing |= preserved
        logger.warning(
            "Opencode API unreachable — preserving %d existing opencode session(s) "
            "in the deletion check",
            len(preserved),
        )

    exit_code = 0
    deleted_paths: list[str] = []
    stale_paths: list[str] = []

    # -- Guard 1: a source root could not be seen this run ------------------------
    # Rows under such a root would look deleted. This covers NFS roots and claude_chats
    # roots. A claude-session:// key is not a path, so an NFS prefix check cannot
    # protect it; the claude root's own skipped_roots entry is what protects its rows.
    # None of the deletion phase runs, and the scan's queueing above still stands.
    skipped_roots = result_nfs["skipped_roots"] + result_chats.get("skipped_roots", [])
    if skipped_roots:
        logger.error(
            "Deletion phase SKIPPED: root(s) not scannable this run: %s. "
            "No rows or Qdrant points were deleted.",
            ", ".join(skipped_roots),
        )
        exit_code = EXIT_DELETION_REFUSED
    else:
        # Scope the check to --user's rows, so one owner's sync never deletes another's.
        owners = [args.user] if args.user else None
        deleted_paths, stale_paths = state_db.find_deleted(all_existing, owners=owners)

        # -- Guard 2: too many completed rows would be deleted at once ----------------
        # A mass deletion usually means a source vanished, not that files were removed.
        completed_total = state_db.get_counts()["completed"]
        limit = max_delete_fraction * completed_total
        if len(deleted_paths) > limit:
            if args.allow_mass_delete:
                logger.warning(
                    "Mass-delete guard overridden by --allow-mass-delete: %d completed file(s) "
                    "to delete, limit %.0f (%.0f%% of %d completed)",
                    len(deleted_paths), limit, max_delete_fraction * 100, completed_total,
                )
            else:
                logger.error(
                    "Deletion phase SKIPPED: %d completed file(s) would be deleted, more than "
                    "%.0f%% of the %d completed rows (limit %.0f). Re-run with --allow-mass-delete "
                    "if this is intended. No rows or Qdrant points were deleted.",
                    len(deleted_paths), max_delete_fraction * 100, completed_total, limit,
                )
                deleted_paths, stale_paths = [], []
                exit_code = EXIT_DELETION_REFUSED

    if deleted_paths or stale_paths:
        logger.info(
            "Deletion phase: %d completed file(s) pending Qdrant cleanup, %d row(s) pending queue removal",
            len(deleted_paths), len(stale_paths),
        )

    # Qdrant cleanup BEFORE SQLite deletion — if killed mid-loop, the SQLite rows
    # survive intact and the next sync retries automatically (no orphaned Qdrant points).
    if deleted_paths:
        qdrant_url = config.get("rag", {}).get("qdrant_url", "http://192.168.1.93:6333")
        qdrant_client = get_client(qdrant_url, timeout=120)
        for i, path in enumerate(deleted_paths, 1):
            reason = _deletion_reason(path, include_exts)
            _delete_path_from_qdrant(qdrant_client, path, i, len(deleted_paths), reason)
        logger.info("Deleted Qdrant points for %d removed file(s)", len(deleted_paths))
    if stale_paths:
        for path in stale_paths:
            logger.info("Queue row removed (%s): %s", _deletion_reason(path, include_exts), path)
        state_db.purge_deleted(stale_paths)

    counts = state_db.get_counts()
    print()
    print("Scan complete:")
    print(f"  NFS files discovered  : {result_nfs['scanned']}")
    print(f"  Chat sessions found   : {result_chats['scanned']}")
    if result_opencode["existing_paths"] is not None:
        print(f"  Opencode sessions found : {result_opencode['scanned']}")
    else:
        print("  Opencode API unreachable — preserved existing opencode points")
    if skipped_roots:
        print(f"  Roots skipped         : {len(skipped_roots)} (deletion phase not run)")
    print(f"  Stale rows removed    : {len(stale_paths)}")
    print()
    print("Queue status:")
    print(f"  pending    : {counts['pending']}")
    print(f"  completed  : {counts['completed']}")
    print(f"  failed     : {counts['failed']}")
    print(f"  ignored    : {counts['ignored']}")
    print(f"  total      : {counts['total']}")

    return exit_code


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan NFS roots and populate the ingestion queue")
    parser.add_argument("--user", metavar="OWNER", help="Scan only this owner's roots (e.g. florian)")
    parser.add_argument(
        "--allow-mass-delete",
        action="store_true",
        help="Proceed with deletion even when it would remove more than rag.sync.max_delete_fraction "
             "of the completed rows",
    )
    args = parser.parse_args()

    with open(Path(__file__).resolve().parent.parent / "ragnart.yaml") as f:
        config = yaml.safe_load(f)

    # Take the single-writer ingestion lock before anything is written. This sync
    # writes SQLite rows and Qdrant points, and it must not run alongside the daemon,
    # a purge, a reset or the snapshot. Contention exits at once with EXIT_LOCK_HELD
    # (75) and names the holder; the sync does not wait. The lock is taken before
    # StateDB is opened, so a refused run never touches the queue database.
    # When the run script already holds the lock (see ragnart/lock.py `run`),
    # acquire_or_exit returns None and this process continues under that lock.
    acquire_or_exit(lock_path_from_config(config))

    db_base = config.get("db_base_path", "/mnt/nfs/__Backups/RAGNART--databases")
    db_path = f"{db_base}/sqlite/rag_state.db"
    state_db = StateDB(db_path)
    try:
        exit_code = sync(config, state_db, args)
    finally:
        state_db.close()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
