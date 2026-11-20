#!/usr/bin/env python3
"""
NAS ingestion run: scan the NAS-side sources and submit their changes to the ingestion API.

The NAS-side sources (nas, claude-linux-shared-nfs, opencode-k11) run on the hub's host. Since v0.3 session B2 they
write nothing to Qdrant. Every change goes to the hub as an ingestion API request (ragnart/sync_submit.py), through
one HubClient per source with that source's own secret. This script keeps the NAS side's own state DB (rag_state.db)
as the record of what it has submitted.

Phases, in this order, for every normal run:

  0. Startup. Take the NAS-ingestion lock (rag.nas_ingest.lock). Read the secret files, then check the hub: one
     authenticated GET per source. A failure here stops the run before anything is written (exit 2).
  1. Poll (ragnart/sync_poll.py). Record the hub's outcome for each row that an earlier run submitted.
  2. Scan. Every NFS root, the Claude Code transcripts and the OpenCode sessions. New and modified files are queued.
  3. Plan (ragnart/sync_plan_core.py build_sync_plan). Moves, tombstones, purges, projects and attachment.
  4. Reconcile (ragnart/sync_reconcile.py), once per source, after the scan. It compares the hub's listing with the
     local rows. Its resends are set pending here, so the submit below sends them in the same run.
  5. Submit (ragnart/sync_submit.py submit_plan). Projects, moves, purges, attachments, then every pending upsert.

The lock is rag.nas_ingest.lock, not rag.ingest_lock (the index lock of the intake worker and hub maintenance).
This script never resets 'processing' rows: each one means the hub has the request and has not reported yet.

--dry-run reads the live hub and writes nothing. It takes no lock. The scan, the poll and the reconcile run on private
copies of rag_state.db and hub_registry.db, and the only hub requests are GETs (status, listing, info). It prints the
plan, and for each source the poll, reconcile and submit counts and the listing time. It sends no POST, DELETE or PATCH.

--rebuild-from-hub repopulates an EMPTY rag_state.db from the hub's listings (ragnart/sync_reconcile.py
rebuild_from_hub), then stops. It sends nothing to the hub. The next normal run scans against the rebuilt rows.
--state-db <path> points the rebuild at another rag_state.db (a scratch copy to diff against the live one). It is
accepted only together with --rebuild-from-hub; any other use is a usage error (exit 2). A rebuild opens no registry.

Refusals. The deletion phase is refused, and the run exits 1, when:
  - A source root could not be scanned this run (missing, or empty while the queue still holds rows under it).
    Nothing is moved or tombstoned.
  - The vanished completed rows would exceed rag.sync.max_delete_fraction of all completed rows. Pass
    --allow-mass-delete to proceed anyway.
  The reconcile orphan guard is separate. It refuses on the same fraction, but of one source's own completed rows,
  and it does not change the exit code. It is reported in the counts and the log.

Exit codes:
  0    success. A dry run exits 0 too, unless the hub could not list a source (then 2).
  1    deletion refused (skipped root or mass-delete guard). The scan's queue changes still stand.
  2    the hub could not be reached, or did not accept a source's secret, at startup. Nothing was written. Also a
       dry run whose listing or info failed, and a rebuild whose listing failed (a rebuild reads before it writes).
  3    refused before any unsafe write: an unusable rag.nas_ingest config or secret file, a NAS-side source with a
       fixed owner, a rebuild refused (rag_state.db not empty, or a crawl root not readable), or a plan step that
       found a state it cannot record.
  75   another writer holds rag.nas_ingest.lock (sysexits EX_TEMPFAIL, ragnart/lock.py EXIT_LOCK_HELD).

Usage (run from project root):
    python utils/rag_sync_nfs.py
    python utils/rag_sync_nfs.py --dry-run
    python utils/rag_sync_nfs.py --user florian
    python utils/rag_sync_nfs.py --allow-mass-delete
    python utils/rag_sync_nfs.py --rebuild-from-hub
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

# Project root is one directory up from utils/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml  # noqa: E402  (after sys.path fix)

from ragnart.endpoint_client import HubClient  # noqa: E402
from ragnart.lock import acquire_or_exit, nas_ingest_lock_path_from_config  # noqa: E402
from ragnart.registry import RegistryDB  # noqa: E402
from ragnart.scanner import scan_claude_chats, scan_nfs_roots, scan_opencode_sessions  # noqa: E402
from ragnart.schema import DEFAULT_RESUBMIT_BATCH_SIZE, SOURCE_ID_NAS, source_id_for_key  # noqa: E402
from ragnart.state import StateDB  # noqa: E402
from ragnart.sync_plan_core import SyncPlan, build_sync_plan, format_plan  # noqa: E402
from ragnart.sync_poll import poll_outstanding  # noqa: E402
from ragnart.sync_reconcile import present_keys, rebuild_from_hub, reconcile_source  # noqa: E402
from ragnart.sync_submit import (  # noqa: E402
    HUB_ERRORS,
    REQUIRED_SOURCES,
    check_nas_sources_have_no_fixed_owner,
    submit_plan,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Exit codes. See the module docstring for their meaning. EXIT_LOCK_HELD (75) is imported from ragnart/lock.py.
EXIT_OK = 0
EXIT_DELETION_REFUSED = 1
EXIT_HUB_UNAVAILABLE = 2
EXIT_REFUSED = 3

# The reconcile counts a dry run reports, always shown even when zero. They are the four rules (orphan, missing,
# version, drift) and the inconsistent-items count. The names are the dry-run ("would") keys of sync_reconcile.py,
# except missing_zero_chunks_skipped: it has no "would" form, because a dry run and a real run count it the same way.
# It counts completed rows with zero chunks that the hub cannot list, which are left alone and not resubmitted.
DRY_RUN_RECONCILE_KEYS = (
    "orphan_would_delete",
    "orphan_guard_tripped",
    "orphan_skipped_no_completed_rows",
    "missing_would_resubmit",
    "missing_zero_chunks_skipped",
    "version_would_resubmit",
    "version_deferred",
    "drift_would_fix",
    "inconsistent_items",
)


# ---------------------------------------------------------------------------
# Configuration and hub clients
# ---------------------------------------------------------------------------

def _nas_ingest_block(config: dict) -> dict:
    """The rag.nas_ingest block of ragnart.yaml. Raises ValueError when it is missing or not a mapping."""
    block = (config.get("rag") or {}).get("nas_ingest")
    if not isinstance(block, dict):
        raise ValueError("ragnart.yaml is missing the 'rag.nas_ingest' block.")
    return block


def build_clients(config: dict) -> dict[str, HubClient]:
    """
    Make one HubClient per NAS-side source, from rag.nas_ingest.

    Each client reads its source's secret file once, here. The secret is never logged or printed. A missing or empty
    secret file raises, and its message names the file, never its content.

    Raises:
        ValueError: hub_url is missing or empty, or a source has no secret path.
        OSError:    a secret file cannot be read.
    """
    block = _nas_ingest_block(config)
    hub_url = block.get("hub_url")
    if not isinstance(hub_url, str) or not hub_url:
        raise ValueError("ragnart.yaml rag.nas_ingest.hub_url must be a non-empty URL string.")
    secrets = block.get("secrets") or {}
    clients: dict[str, HubClient] = {}
    for source_id in REQUIRED_SOURCES:
        secret_path = secrets.get(source_id)
        if not secret_path:
            raise ValueError(f"ragnart.yaml rag.nas_ingest.secrets has no path for source '{source_id}'.")
        clients[source_id] = HubClient(hub_url, secret_path)
    return clients


def resubmit_batch_size(config: dict) -> int:
    """rag.nas_ingest.resubmit_batch_size, default DEFAULT_RESUBMIT_BATCH_SIZE. Raises ValueError below 1."""
    value = _nas_ingest_block(config).get("resubmit_batch_size", DEFAULT_RESUBMIT_BATCH_SIZE)
    # bool is a subclass of int, so it is refused explicitly.
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"ragnart.yaml rag.nas_ingest.resubmit_batch_size must be an integer of at least 1; got {value!r}")
    return value


def hub_startup_failures(clients: dict[str, HubClient]) -> dict[str, str]:
    """
    Ask each source's hub once, before anything is written. Returns {source_id: reason} for each source that failed.

    GET /v1/info is authenticated (every route is), so a passing answer shows two things: the hub is reachable,
    and the source's secret is accepted. An empty dict means every source passed. The reason is the error text,
    which never contains a secret (ragnart/endpoint_client.py).
    """
    failures: dict[str, str] = {}
    for source_id in REQUIRED_SOURCES:
        try:
            clients[source_id].info()
        except HUB_ERRORS as exc:
            failures[source_id] = str(exc)
    return failures


# ---------------------------------------------------------------------------
# Small helpers for the printed report
# ---------------------------------------------------------------------------

def _counter_text(counts: Counter, keys: tuple[str, ...] = ()) -> str:
    """
    One line of counts. With `keys`, every key is shown (zero when absent). Without, only the non-zero counts.
    """
    if keys:
        return " ".join(f"{key}={counts.get(key, 0)}" for key in keys)
    shown = [f"{key}={value}" for key, value in sorted(counts.items()) if value]
    return " ".join(shown) if shown else "none"


def _would_submit(plan: SyncPlan, state_db: StateDB) -> dict[str, Counter]:
    """
    How many requests the submit would send for each source, counted from the plan and the pending rows.

    This runs before reconcile's resends are added, because a dry run does not set rows pending. The reconcile
    would_resubmit counts are reported beside it, and a real run sends both.
    """
    per_source: dict[str, Counter] = {source: Counter() for source in REQUIRED_SOURCES}
    # Projects are published and removed through the NAS source only (SOURCES_WITH_PROJECT_SUPPORT: project support is built for it only).
    per_source[SOURCE_ID_NAS]["projects_publish"] += len(plan.project_upserts)
    per_source[SOURCE_ID_NAS]["projects_remove"] += len(plan.project_removals)
    for old_path in plan.moves:
        per_source[source_id_for_key(old_path)]["moves"] += 1
    for path in plan.purge_ready:
        per_source[source_id_for_key(path)]["purges"] += 1
    for path in plan.attachment_changes:
        per_source[source_id_for_key(path)]["attachments"] += 1
    for row in state_db.pending_rows():
        per_source[source_id_for_key(row["file_path"])]["upserts"] += 1
    return per_source


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def _reconcile_all(
    state_db: StateDB,
    clients: dict[str, HubClient],
    config: dict,
    scan_result: dict,
    dry_run: bool,
) -> tuple[dict[str, Counter], dict[str, tuple[float, int]]]:
    """
    Phase 4. Reconcile each source against its hub listing. Returns (counts per source, listing per source).

    The listing is fetched here, not inside reconcile_source, so that its time can be reported. A source whose
    listing or info cannot be read is decided by nothing this run: no deletes and no resends for it. Its counts are
    marked listing_failed or info_failed, and the next run tries again. The other sources still reconcile.

    The listing per source is (seconds, item count). It is absent for a source whose listing failed.
    """
    present = present_keys(scan_result)
    max_fraction = config["rag"]["sync"]["max_delete_fraction"]
    batch = resubmit_batch_size(config)

    counts_by_source: dict[str, Counter] = {}
    listing_by_source: dict[str, tuple[float, int]] = {}
    for source_id in REQUIRED_SOURCES:
        client = clients[source_id]
        started = time.monotonic()
        try:
            listing = client.items()
        except HUB_ERRORS as exc:
            counts_by_source[source_id] = Counter({"listing_failed": 1})
            logger.error("%s: the hub listing could not be read; reconcile skipped this run: %s", source_id, exc)
            continue
        listing_by_source[source_id] = (time.monotonic() - started, len(listing))

        try:
            counts_by_source[source_id] = reconcile_source(
                state_db, client, source_id, present, max_fraction,
                resubmit_batch_size=batch, dry_run=dry_run, hub_items=listing,
            )
        except HUB_ERRORS as exc:
            counts_by_source[source_id] = Counter({"info_failed": 1})
            logger.error("%s: the hub did not answer the reconcile info call; reconcile skipped: %s", source_id, exc)
    return counts_by_source, listing_by_source


def _dry_run_report(
    plan: SyncPlan,
    state_db: StateDB,
    poll_counts: Counter,
    reconcile_counts: dict[str, Counter],
    listing: dict[str, tuple[float, int]],
) -> str:
    """The text a dry run prints after the plan: poll, reconcile and submit counts for each source, and the listing times."""
    lines = [
        "",
        "Dry run: nothing was written and no request was sent. The hub was only read (GET).",
        "Poll (status reads, on a copy of rag_state.db): " + _counter_text(poll_counts),
        "Per source (reconcile counts are what would happen; nothing was sent):",
    ]
    submit = _would_submit(plan, state_db)
    for source_id in REQUIRED_SOURCES:
        seconds_and_items = listing.get(source_id)
        listing_text = (
            f"listing {seconds_and_items[0]:.2f} s for {seconds_and_items[1]} item(s)"
            if seconds_and_items is not None else "listing NOT read"
        )
        lines.append(f"  {source_id}: {listing_text}")
        lines.append("    reconcile: " + _counter_text(reconcile_counts[source_id], DRY_RUN_RECONCILE_KEYS))
        lines.append("    submit (pending rows before reconcile, plus plan actions): " + _counter_text(submit[source_id]))
    return "\n".join(lines)


def sync(
    config: dict,
    state_db: StateDB,
    registry_db: RegistryDB,
    clients: dict[str, HubClient],
    args: argparse.Namespace,
) -> int:
    """
    One normal run: poll, scan, plan, reconcile, submit. With args.dry_run it stops after the reconcile and prints.

    Args:
        config:      Full ragnart.yaml dict.
        state_db:    Open StateDB. The caller owns it and closes it. Poll, scan, reconcile and submit write to it.
        registry_db: Open RegistryDB (the hub registry copy the plan reads). The caller owns it and closes it.
                     Submit reads it; nothing writes to it here.
        clients:     One HubClient per NAS-side source (SOURCE_ID_* keys).
        args:        Parsed arguments. Reads args.user (str or None), args.allow_mass_delete (bool) and
                     args.dry_run (bool). With dry_run, state_db and registry_db must be private copies.

    Returns:
        EXIT_OK, EXIT_DELETION_REFUSED, or (dry run only) EXIT_HUB_UNAVAILABLE when a source's listing or info failed.

    Raises:
        RuntimeError: a NAS-side source has a fixed owner (before any write), or a move target has no pending row.
        ValueError:   rag.nas_ingest.resubmit_batch_size is not usable (before any write).
    """
    # Refuse first: a fixed owner on a NAS-side source would make every upload fail, so nothing is written.
    check_nas_sources_have_no_fixed_owner(registry_db)
    resubmit_batch_size(config)

    now_iso = datetime.now(timezone.utc).isoformat()

    # Phase 1. Poll the rows that earlier runs submitted, before the scan changes the queue.
    poll_counts = poll_outstanding(state_db, clients)

    # Phase 2. Scan. Unchanged from the pre-B2 sync.
    result_nfs = scan_nfs_roots(state_db, config, user_filter=args.user)
    result_chats = scan_claude_chats(state_db, config, user_filter=args.user)
    result_opencode = scan_opencode_sessions(state_db, config, user_filter=args.user)
    scan_result = {"nfs": result_nfs, "chats": result_chats, "opencode": result_opencode}

    # Phase 3. Plan. Unchanged from the pre-B2 sync. --user limits the plan to one owner's rows and projects.
    owners = [args.user] if args.user else None
    plan = build_sync_plan(
        state_db,
        registry_db,
        scan_result,
        config,
        now_iso,
        owners=owners,
        allow_mass_delete=args.allow_mass_delete,
    )

    # Phase 4. Reconcile, per source, before the submit. Its resends are set pending here, so they go in this run.
    reconcile_counts, listing = _reconcile_all(state_db, clients, config, scan_result, args.dry_run)

    if args.dry_run:
        print(format_plan(plan))
        print(_dry_run_report(plan, state_db, poll_counts, reconcile_counts, listing))
        hub_unread = any("listing_failed" in c or "info_failed" in c for c in reconcile_counts.values())
        return EXIT_HUB_UNAVAILABLE if hub_unread else EXIT_OK

    # The plan is logged before it is submitted, so the sync log shows what was intended.
    logger.info("%s", format_plan(plan))

    # Phase 5. Submit. Every change goes to the hub as a request. Local state changes only after the hub accepts.
    submit_counts = submit_plan(plan, state_db, registry_db, clients, config, now_iso)

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
    print("Poll (requests submitted by earlier runs): " + _counter_text(poll_counts))
    print("Plan: " + _counter_text(Counter({
        "moves": len(plan.moves),
        "newly_missing": len(plan.newly_missing),
        "reappeared": len(plan.reappeared),
        "purge_ready": len(plan.purge_ready),
        "attachment_changes": len(plan.attachment_changes),
        "project_upserts": len(plan.project_upserts),
        "project_removals": len(plan.project_removals),
        "stale_rows": len(plan.stale_rows),
    })))
    if plan.guard_tripped:
        print("  Mass-delete guard     : tripped, tombstones refused (re-run with --allow-mass-delete)")
    print("Reconcile:")
    for source_id in REQUIRED_SOURCES:
        print(f"  {source_id}: " + _counter_text(reconcile_counts[source_id]))
    print("Submit:")
    print("  " + _counter_text(submit_counts))
    print()
    print("Queue status:")
    print(f"  pending    : {counts['pending']}")
    print(f"  processing : {counts['processing']}")
    print(f"  completed  : {counts['completed']}")
    print(f"  failed     : {counts['failed']}")
    print(f"  ignored    : {counts['ignored']}")
    print(f"  total      : {counts['total']}")

    refused = plan.skipped or plan.guard_tripped
    return EXIT_DELETION_REFUSED if refused else EXIT_OK


def run_rebuild(config: dict, state_db: StateDB, clients: dict[str, HubClient], args: argparse.Namespace) -> int:
    """
    --rebuild-from-hub: repopulate an empty rag_state.db from the hub's listings, then stop. Sends nothing to the hub.

    rebuild_from_hub() refuses (RuntimeError, exit 3) when the state holds rows or a crawl root is unreadable. It
    reads every listing before it writes, so a hub failure here (exit 2) leaves the state empty.
    """
    try:
        counts = rebuild_from_hub(state_db, clients, config, user_filter=args.user)
    except HUB_ERRORS as exc:
        logger.error("rebuild stopped, the hub listing could not be read; nothing was written: %s", exc)
        return EXIT_HUB_UNAVAILABLE
    print("Rebuild from hub: " + _counter_text(counts))
    return EXIT_OK


def _copy_database(src_path: str, dst_path: str) -> None:
    """
    Copy a SQLite database with SQLite's backup API, reading the source read-only.

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


def run_dry_run(
    config: dict,
    db_path: str,
    registry_path: str,
    clients: dict[str, HubClient],
    args: argparse.Namespace,
) -> int:
    """
    Run the sync as a dry run: on private copies of both databases, with the hub read but never written.

    The live rag_state.db and hub_registry.db are only read (copied with the backup API). The copies live in a
    scratch directory, which is removed afterwards. The registry copy is taken from the live registry when that
    exists. Otherwise RegistryDB creates an empty one in the scratch directory, so the dry run never creates the
    live file. No lock is taken, because nothing is written to the live state.

    Returns:
        The exit code from sync().
    """
    scratch_dir = tempfile.mkdtemp(prefix="ragnart-dry-run-")
    try:
        copy_path = os.path.join(scratch_dir, "rag_state.db")
        _copy_database(db_path, copy_path)
        registry_copy_path = os.path.join(scratch_dir, "hub_registry.db")
        if os.path.exists(registry_path):
            _copy_database(registry_path, registry_copy_path)
        state_db = StateDB(copy_path)
        registry_db = RegistryDB(registry_copy_path)
        try:
            return sync(config, state_db, registry_db, clients, args)
        finally:
            registry_db.close()
            state_db.close()
    finally:
        shutil.rmtree(scratch_dir, ignore_errors=True)


def _run_against_hub(config: dict, db_path: str, registry_path: str, clients: dict[str, HubClient], args: argparse.Namespace) -> int:
    """
    Startup check, then the requested mode. Nothing is written before the startup check passes.

    The check runs before the databases are opened (a live run) or copied (a dry run), so a hub that is down or
    refuses a secret leaves the state exactly as it was.
    """
    failures = hub_startup_failures(clients)
    if failures:
        for source_id, reason in failures.items():
            logger.error("%s: the ingestion API did not pass the startup check: %s", source_id, reason)
        logger.error("nothing was written.")
        return EXIT_HUB_UNAVAILABLE

    if args.dry_run:
        return run_dry_run(config, db_path, registry_path, clients, args)

    state_db = StateDB(db_path)
    try:
        # A rebuild needs no registry: it reads the hub listings and writes rag_state.db only. It does not open
        # the live hub_registry.db, so a rebuild into a scratch --state-db never touches the live registry.
        if args.rebuild_from_hub:
            return run_rebuild(config, state_db, clients, args)
        registry_db = RegistryDB(registry_path)
        try:
            return sync(config, state_db, registry_db, clients, args)
        finally:
            registry_db.close()
    finally:
        state_db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan the NAS-side sources and submit their changes to the ingestion API")
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
        help="Print the plan and the poll, reconcile and submit counts. Reads the hub, writes nothing, sends no write request.",
    )
    parser.add_argument(
        "--rebuild-from-hub",
        action="store_true",
        help="Repopulate an empty rag_state.db from the hub's listings, then stop. Refused when the state is not empty.",
    )
    parser.add_argument(
        "--state-db",
        metavar="PATH",
        help="Use this rag_state.db instead of the live one. Only with --rebuild-from-hub, so a rebuild can be "
             "checked against a scratch file. Usage error without --rebuild-from-hub.",
    )
    args = parser.parse_args()
    if args.dry_run and args.rebuild_from_hub:
        parser.error("--rebuild-from-hub writes rag_state.db, so it has no dry run. Run it without --dry-run.")
    if args.state_db is not None and not args.rebuild_from_hub:
        # Only a rebuild may point at another file. A normal run or dry run on a stray path would sync against a
        # state that is not the live one, so the flag is refused instead of being quietly honoured.
        parser.error("--state-db is accepted only together with --rebuild-from-hub.")

    with open(Path(__file__).resolve().parent.parent / "ragnart.yaml") as f:
        config = yaml.safe_load(f)

    db_base = config.get("db_base_path", "/mnt/nfs/__Backups/RAGNART--databases")
    db_path = args.state_db or f"{db_base}/sqlite/rag_state.db"
    # The hub registry sits beside the queue (same as ragnart/cli.py registry_db_path()).
    registry_path = f"{db_base}/sqlite/hub_registry.db"

    # Take the NAS-ingestion lock before anything is written or any secret is read. Contention exits at once with
    # EXIT_LOCK_HELD (75) and names the holder; the run does not wait. A dry run takes no lock.
    # When a run script already holds this lock (ragnart/lock.py `run`), acquire_or_exit returns None and this
    # process continues under that lock.
    if not args.dry_run:
        acquire_or_exit(nas_ingest_lock_path_from_config(config))

    try:
        clients = build_clients(config)
    except (OSError, ValueError) as exc:
        logger.error("refused, the rag.nas_ingest configuration is unusable: %s", exc)
        sys.exit(EXIT_REFUSED)

    try:
        exit_code = _run_against_hub(config, db_path, registry_path, clients, args)
    except (RuntimeError, ValueError) as exc:
        logger.error("refused, nothing further done: %s", exc)
        exit_code = EXIT_REFUSED
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
