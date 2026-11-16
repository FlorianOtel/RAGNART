#!/usr/bin/env python3
"""
Scan configured sources and bring the queue and Qdrant in line with what was found.

Every run covers every configured NFS root (La-Familia included), the Claude Code
transcripts and the OpenCode sessions. It works in two halves:

  1. Scan. New files are queued as pending and modified files are re-queued. New NFS
     files are hashed here, so a move can be recognised in the same run.
  2. Plan, then apply (ragnart/sync_plan.py). The plan lists every change: project
     markers, moves, tombstones, purges, reappearances and attachment. The plan is
     logged, then applied.

A file that vanished is not deleted at once. It is tombstoned (missing_since is set)
and its points stay searchable for 24 hours. After that, the first sync purges its
points and its queue row. A file that moved (same content_hash, one vanished and one
new in the same run) keeps its points: its key is rewritten, with no re-embedding.

Refusals. The deletion phase is refused, and the run exits EXIT_DELETION_REFUSED, when:
  - A source root could not be scanned this run (missing, or empty while the queue
    still holds rows under it). Nothing is moved or tombstoned.
  - The vanished completed rows would exceed rag.sync.max_delete_fraction of all
    completed rows. Pass --allow-mass-delete to proceed anyway. The purge of rows
    already tombstoned is not subject to this guard.

--dry-run prints the plan and exits 0. It takes no lock and writes nothing to the live
database. The scan writes to the queue, so the dry run runs on a private copy of it.

Usage (run from project root):
    python utils/rag_sync_nfs.py
    python utils/rag_sync_nfs.py --dry-run
    python utils/rag_sync_nfs.py --user florian
    python utils/rag_sync_nfs.py --allow-mass-delete
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Project root is one directory up from utils/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml  # noqa: E402  (after sys.path fix)

from ragnart.collection import get_client  # noqa: E402
from ragnart.lock import acquire_or_exit, lock_path_from_config  # noqa: E402
from ragnart.scanner import scan_claude_chats, scan_nfs_roots, scan_opencode_sessions  # noqa: E402
from ragnart.state import StateDB  # noqa: E402
from ragnart.sync_plan import apply_sync_plan, build_sync_plan, format_plan  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Exit code when the deletion phase was refused (skipped root or mass-delete guard).
EXIT_DELETION_REFUSED = 1


def sync(config: dict, state_db: StateDB, args: argparse.Namespace) -> int:
    """
    Run one sync: scan every source, plan the changes, log the plan, then apply it.

    Args:
        config:   Full ragnart.yaml dict.
        state_db: Open StateDB. The caller owns it and closes it. The scan writes to it.
        args:     Parsed arguments. Reads args.user (str or None), args.allow_mass_delete
                  (bool) and args.dry_run (bool). With dry_run the plan is printed and
                  nothing is applied, so the caller must pass a private copy of the database.

    Returns:
        0 on success, or after a dry run. EXIT_DELETION_REFUSED when the deletion phase
        was refused. The queueing done by the scan still stands in that case.
    """
    result_nfs = scan_nfs_roots(state_db, config, user_filter=args.user)
    result_chats = scan_claude_chats(state_db, config, user_filter=args.user)
    result_opencode = scan_opencode_sessions(state_db, config, user_filter=args.user)

    now_iso = datetime.now(timezone.utc).isoformat()
    # --user limits the sync to one owner. Their projects and rows are the only ones
    # that can be removed or purged, so another owner's data is never touched.
    owners = [args.user] if args.user else None
    scan_result = {"nfs": result_nfs, "chats": result_chats, "opencode": result_opencode}
    plan = build_sync_plan(
        state_db,
        scan_result,
        config,
        now_iso,
        owners=owners,
        allow_mass_delete=args.allow_mass_delete,
    )

    if args.dry_run:
        print(format_plan(plan))
        return 0

    # The plan is logged before it is applied, so the sync log shows what was intended.
    logger.info("%s", format_plan(plan))
    qdrant_url = config.get("rag", {}).get("qdrant_url", "http://192.168.1.93:6333")
    qdrant_client = get_client(qdrant_url, timeout=120)
    apply_sync_plan(state_db, qdrant_client, plan, now_iso)

    skipped_roots = result_nfs["skipped_roots"] + result_chats.get("skipped_roots", [])
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
        print(f"  Roots skipped         : {len(skipped_roots)} (no moves or tombstones this run)")
    print()
    print("Sync plan applied:")
    print(f"  Moves                 : {len(plan.moves)}")
    print(f"  Newly missing         : {len(plan.newly_missing)} (tombstoned, points kept)")
    print(f"  Reappeared            : {len(plan.reappeared)}")
    print(f"  Purged                : {len(plan.purge_ready)}")
    print(f"  Re-attached           : {len(plan.attachment_changes)}")
    print(f"  Projects upserted     : {len(plan.project_upserts)}")
    print(f"  Projects removed      : {len(plan.project_removals)}")
    print(f"  Stale rows removed    : {len(plan.stale_rows)}")
    if plan.guard_tripped:
        print("  Mass-delete guard     : tripped, tombstones refused (re-run with --allow-mass-delete)")
    print()
    print("Queue status:")
    print(f"  pending    : {counts['pending']}")
    print(f"  completed  : {counts['completed']}")
    print(f"  failed     : {counts['failed']}")
    print(f"  ignored    : {counts['ignored']}")
    print(f"  total      : {counts['total']}")

    refused = plan.skipped or plan.guard_tripped
    return EXIT_DELETION_REFUSED if refused else 0


def _copy_database(src_path: str, dst_path: str) -> None:
    """
    Copy the queue database with SQLite's backup API, reading the source read-only.

    The source is opened with mode=ro, so the copy cannot write to the live database.
    The backup API copies a consistent snapshot, including WAL content.
    """
    src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
    try:
        dst = sqlite3.connect(dst_path)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def run_dry_run(config: dict, db_path: str, args: argparse.Namespace) -> int:
    """
    Print the sync plan without changing the live database and without taking the lock.

    The scan writes to the queue: new files are queued and modified ones re-queued. To
    plan without touching the live database, the sync runs on a private copy of it in a
    scratch directory. The copy is removed afterwards. No Qdrant client is created,
    because apply_sync_plan() is never called.

    Args:
        config:   Full ragnart.yaml dict.
        db_path:  Path to the live rag_state.db. It is only read.
        args:     Parsed arguments, as for sync().

    Returns:
        The exit code from sync().
    """
    scratch_dir = tempfile.mkdtemp(prefix="ragnart-dry-run-")
    try:
        copy_path = os.path.join(scratch_dir, "rag_state.db")
        _copy_database(db_path, copy_path)
        state_db = StateDB(copy_path)
        try:
            return sync(config, state_db, args)
        finally:
            state_db.close()
    finally:
        shutil.rmtree(scratch_dir, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan sources, plan the queue and Qdrant changes, and apply them")
    parser.add_argument("--user", metavar="OWNER", help="Scan only this owner's roots (e.g. florian)")
    parser.add_argument(
        "--allow-mass-delete",
        action="store_true",
        help="Proceed with tombstoning even when more than rag.sync.max_delete_fraction "
             "of the completed rows would be missing at once",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the sync plan and write nothing. Takes no lock and does not touch the live database.",
    )
    args = parser.parse_args()

    with open(Path(__file__).resolve().parent.parent / "ragnart.yaml") as f:
        config = yaml.safe_load(f)

    db_base = config.get("db_base_path", "/mnt/nfs/__Backups/RAGNART--databases")
    db_path = f"{db_base}/sqlite/rag_state.db"

    # A dry run neither takes the ingestion lock nor opens the live database for writing.
    if args.dry_run:
        sys.exit(run_dry_run(config, db_path, args))

    # Take the single-writer ingestion lock before anything is written. This sync
    # writes SQLite rows and Qdrant points, and it must not run alongside the daemon,
    # a purge, a reset or the snapshot. Contention exits at once with EXIT_LOCK_HELD
    # (75) and names the holder; the sync does not wait. The lock is taken before
    # StateDB is opened, so a refused run never touches the queue database.
    # When the run script already holds the lock (see ragnart/lock.py `run`),
    # acquire_or_exit returns None and this process continues under that lock.
    acquire_or_exit(lock_path_from_config(config))

    state_db = StateDB(db_path)
    try:
        exit_code = sync(config, state_db, args)
    finally:
        state_db.close()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
