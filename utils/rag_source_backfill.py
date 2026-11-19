#!/usr/bin/env python3
"""
One-time v0.3 source backfill: gives every existing point and queue row its source_id.

Points ingested before v0.3 have no `source_id` and no `pipeline_version` in the Qdrant
`documents` collection. Queue rows ingested before v0.3 have a NULL `source_id` column
(migration 3 adds the column, and new rows get it from the scanner). This script fills both.
It never re-embeds: it changes payloads (Qdrant set_payload) and one SQLite column, and
leaves the vectors alone.

The source of every point comes from its `source_type`, which the earlier v0.2 backfill
already set:

    source_type         source_id                        (the three built sources)
    file                nas                              SOURCE_ID_NAS
    claude_session      claude-linux-shared-nfs          SOURCE_ID_CLAUDE_LINUX_SHARED_NFS
    opencode_session    opencode-k11                     SOURCE_ID_OPENCODE_K11

The queue source_id comes from the key with source_id_for_key(), the one rule in
ragnart/schema.py, so queue and points agree by construction.

Refusal. A point whose source_type is missing, or is none of the three values above, has no
source it can be assigned to. If any such point exists, --apply refuses and writes nothing,
and the report names the count. Both modes exit 1 in that case, so a dry-run shows the refusal
before an operator runs --apply.

Usage (run from project root):
    python utils/rag_source_backfill.py            # dry-run (default): report only, write nothing
    python utils/rag_source_backfill.py --dry-run
    python utils/rag_source_backfill.py --apply    # takes both ingestion locks (index, then NAS), then writes

Order on --apply:
    1. Refuse if any point has no known source_type (nothing is written).
    2. Qdrant set_payload, once per source_type, filtered on source_type, wait=True.
    3. SQLite: one transaction, setting source_id on every queue row where it is NULL.
    4. Verification printout: Qdrant points per source_id (exact) and their sum against the
       total point count; queue rows per source_id; points missing source_id or
       pipeline_version (must be 0).

Idempotent. A re-run sets the same payload values again and finds no NULL queue rows left.
A dry-run opens rag_state.db read-only and never runs a migration. Before migration 3 the
source_id column does not exist, so the dry-run treats every row as NULL in that case.

Exit codes: 0 when the apply (or the dry-run) found nothing to refuse and the verification is
clean. 1 when points without a known source_type exist (refused), or when the verification
after --apply is not clean.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import yaml
from qdrant_client.models import (
    FieldCondition,
    Filter,
    IsEmptyCondition,
    MatchAny,
    MatchValue,
    PayloadField,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ragnart.collection import DOCUMENTS_COLLECTION, get_client  # noqa: E402
from ragnart.lock import (  # noqa: E402
    acquire_or_exit,
    lock_path_from_config,
    nas_ingest_lock_path_from_config,
)
from ragnart.schema import (  # noqa: E402
    FIELD_PIPELINE_VERSION,
    FIELD_SOURCE_ID,
    FIELD_SOURCE_TYPE,
    PIPELINE_VERSION_P1,
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
    source_id_for_key,
)
from ragnart.state import StateDB  # noqa: E402

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "ragnart.yaml"

# The source_type value each point carries, and the source_id it is assigned. These are data
# values of the source_type payload field, not field names. Insertion order is the apply order.
SOURCE_TYPE_TO_SOURCE_ID: dict[str, str] = {
    "file": SOURCE_ID_NAS,
    "claude_session": SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    "opencode_session": SOURCE_ID_OPENCODE_K11,
}


# ---------------------------------------------------------------------------
# Qdrant filters and counts
# ---------------------------------------------------------------------------

def _source_type_filter(source_type: str) -> Filter:
    """Filter matching every point whose source_type is exactly `source_type`."""
    return Filter(must=[FieldCondition(key=FIELD_SOURCE_TYPE, match=MatchValue(value=source_type))])


def _source_id_filter(source_id: str) -> Filter:
    """Filter matching every point whose source_id is exactly `source_id`."""
    return Filter(must=[FieldCondition(key=FIELD_SOURCE_ID, match=MatchValue(value=source_id))])


def _no_known_source_type_filter() -> Filter:
    """
    Filter matching every point whose source_type is NOT one of the known values.

    That covers a point with no source_type field at all (the must_not condition does not match
    a missing field, so the point is kept) and a point with an unexpected value. Both have no
    source to be assigned to, so both block --apply.
    """
    return Filter(
        must_not=[FieldCondition(key=FIELD_SOURCE_TYPE, match=MatchAny(any=list(SOURCE_TYPE_TO_SOURCE_ID)))]
    )


def _missing_source_id_or_version_filter() -> Filter:
    """
    Filter matching every point that lacks a source_id or a pipeline_version.

    After --apply this must count zero. `should` is an OR: either field empty is enough.
    """
    return Filter(
        should=[
            IsEmptyCondition(is_empty=PayloadField(key=FIELD_SOURCE_ID)),
            IsEmptyCondition(is_empty=PayloadField(key=FIELD_PIPELINE_VERSION)),
        ]
    )


def _count(qdrant_client: Any, count_filter: Filter | None = None) -> int:
    """Exact number of points matching `count_filter` (all points when it is None)."""
    result = qdrant_client.count(
        collection_name=DOCUMENTS_COLLECTION,
        count_filter=count_filter,
        exact=True,
    )
    return int(result.count)


def survey_qdrant(qdrant_client: Any) -> dict:
    """
    Exact point counts before any write: per source_type, and the points with no known source_type.

    `no_source_type` is the refusal figure: --apply refuses when it is above zero.
    """
    return {
        "per_source_type": {
            source_type: _count(qdrant_client, _source_type_filter(source_type))
            for source_type in SOURCE_TYPE_TO_SOURCE_ID
        },
        "no_source_type": _count(qdrant_client, _no_known_source_type_filter()),
    }


# ---------------------------------------------------------------------------
# SQLite queue
# ---------------------------------------------------------------------------

def null_source_keys(state_db: StateDB) -> list[str]:
    """
    Return the file_path of every queue row whose source_id is NULL.

    Works before migration 3 too. A read-only StateDB does not run migrations, so the source_id
    column may not exist yet; every row is then NULL and all of them are returned.
    """
    columns = {row[1] for row in state_db._conn.execute("PRAGMA table_info(ingestion_queue)")}
    if "source_id" not in columns:
        sql = "SELECT file_path FROM ingestion_queue"
    else:
        sql = "SELECT file_path FROM ingestion_queue WHERE source_id IS NULL"
    return [row[0] for row in state_db._conn.execute(sql).fetchall()]


def write_queue_source_ids(state_db: StateDB) -> int:
    """
    Set source_id on every queue row where it is NULL, in one transaction. Returns the row count.

    The value for each row is source_id_for_key(file_path). The WHERE clause repeats
    `source_id IS NULL`, so a row set by anyone else in the meantime is never overwritten.
    The executemany runs inside one implicit transaction, committed once at the end.
    """
    keys = null_source_keys(state_db)
    state_db._conn.executemany(
        "UPDATE ingestion_queue SET source_id = ? WHERE file_path = ? AND source_id IS NULL",
        [(source_id_for_key(key), key) for key in keys],
    )
    state_db._conn.commit()
    return len(keys)


def survey_queue(state_db: StateDB) -> dict:
    """The NULL-source rows that --apply would set, counted per source_id from their keys."""
    keys = null_source_keys(state_db)
    return {
        "null_rows": len(keys),
        "per_source_id": dict(Counter(source_id_for_key(key) for key in keys)),
    }


def queue_counts_by_source_id(state_db: StateDB) -> dict:
    """Queue rows per source_id after the write. A NULL source_id is reported under the key None."""
    rows = state_db._conn.execute(
        "SELECT source_id, COUNT(*) AS n FROM ingestion_queue GROUP BY source_id"
    ).fetchall()
    return {row[0]: row[1] for row in rows}


# ---------------------------------------------------------------------------
# Whole-run driver
# ---------------------------------------------------------------------------

def verify(state_db: StateDB, qdrant_client: Any) -> dict:
    """
    Read back the state after --apply. Nothing here writes.

    `points_sum` against `points_total` shows whether every point landed in one of the three
    source ids. `points_missing_fields` must be zero.
    """
    per_source_id = {
        source_id: _count(qdrant_client, _source_id_filter(source_id))
        for source_id in sorted(set(SOURCE_TYPE_TO_SOURCE_ID.values()))
    }
    return {
        "points_per_source_id": per_source_id,
        "points_sum": sum(per_source_id.values()),
        "points_total": _count(qdrant_client),
        "queue_per_source_id": queue_counts_by_source_id(state_db),
        "points_missing_fields": _count(qdrant_client, _missing_source_id_or_version_filter()),
    }


def run_backfill(state_db: StateDB, qdrant_client: Any, dry_run: bool) -> dict:
    """
    Survey (always), then either stop (dry-run or refusal) or write and verify (apply).

    The survey is read-only in both modes. In a dry-run nothing else happens. On --apply with
    no refusal: three Qdrant set_payload calls (one per source_type), then the queue write,
    then the verification. The summary's `refused` flag says whether the apply was blocked.
    """
    summary: dict[str, Any] = {
        "dry_run": dry_run,
        "before": {
            "qdrant": survey_qdrant(qdrant_client),
            "queue": survey_queue(state_db),
        },
    }
    summary["refused"] = summary["before"]["qdrant"]["no_source_type"] > 0
    if dry_run or summary["refused"]:
        return summary

    # Qdrant first. Each call is filtered on source_type and waits for Qdrant to apply it.
    for source_type, source_id in SOURCE_TYPE_TO_SOURCE_ID.items():
        qdrant_client.set_payload(
            collection_name=DOCUMENTS_COLLECTION,
            payload={FIELD_SOURCE_ID: source_id, FIELD_PIPELINE_VERSION: PIPELINE_VERSION_P1},
            points=_source_type_filter(source_type),
            wait=True,
        )
    summary["queue_rows_set"] = write_queue_source_ids(state_db)
    summary["after"] = verify(state_db, qdrant_client)
    return summary


def exit_code(summary: dict) -> int:
    """1 when the apply was refused or the verification is not clean, else 0."""
    if summary["refused"]:
        return 1
    if summary["dry_run"]:
        return 0
    after = summary["after"]
    clean = (
        after["points_sum"] == after["points_total"]
        and after["points_missing_fields"] == 0
        and None not in after["queue_per_source_id"]
    )
    return 0 if clean else 1


# ---------------------------------------------------------------------------
# Reporting and command line
# ---------------------------------------------------------------------------

def print_report(summary: dict) -> None:
    """Print the survey, the refusal if any, and (after --apply) the verification."""
    before = summary["before"]
    print(f"=== rag_source_backfill ({'DRY-RUN' if summary['dry_run'] else 'APPLY'}) ===")
    print("Qdrant points by source_type (exact):")
    for source_type, count in before["qdrant"]["per_source_type"].items():
        print(f"  {source_type:<18} {count:>10}  -> source_id {SOURCE_TYPE_TO_SOURCE_ID[source_type]}")
    print(f"  no known source_type {before['qdrant']['no_source_type']:>8}")
    print("Queue rows with NULL source_id:")
    print(f"  total {before['queue']['null_rows']}")
    for source_id, count in sorted(before["queue"]["per_source_id"].items()):
        print(f"  -> {source_id:<28} {count}")

    if summary["refused"]:
        print()
        print(f"REFUSED: {before['qdrant']['no_source_type']} point(s) have no known source_type. "
              "Nothing is written. Fix or remove those points first.")
        return

    if summary["dry_run"]:
        print()
        print("(dry-run: nothing written. Re-run with --apply to carry out the backfill.)")
        return

    print(f"Queue rows set: {summary['queue_rows_set']}")
    after = summary["after"]
    print("Verification after --apply:")
    for source_id, count in after["points_per_source_id"].items():
        print(f"  points source_id={source_id:<28} {count}")
    sum_ok = "match" if after["points_sum"] == after["points_total"] else "MISMATCH"
    print(f"  points sum {after['points_sum']} vs total {after['points_total']}: {sum_ok}")
    for source_id, count in sorted(after["queue_per_source_id"].items(), key=lambda item: str(item[0])):
        label = "NULL" if source_id is None else source_id
        print(f"  queue source_id={label:<28} {count}")
    print(f"  points missing source_id or pipeline_version: {after['points_missing_fields']} (must be 0)")


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point. Returns the process exit code (see exit_code())."""
    parser = argparse.ArgumentParser(
        description="One-time v0.3 backfill of source_id on Qdrant points and queue rows."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Report only (the default).")
    mode.add_argument("--apply", action="store_true", help="Write source_id and pipeline_version.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry_run = not args.apply

    with open(CONFIG_PATH, encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    db_base = config.get("db_base_path", "/mnt/nfs/__Backups/RAGNART--databases")
    db_path = f"{db_base}/sqlite/rag_state.db"

    if dry_run:
        # The dry-run never takes the lock and never writes. SQLite is opened read-only (no
        # migration), and Qdrant is only counted.
        state_db = StateDB(db_path, read_only=True)
        qdrant_client = get_client(config["rag"]["qdrant_url"], timeout=120)
        try:
            summary = run_backfill(state_db, qdrant_client, True)
        finally:
            state_db.close()
        print_report(summary)
        return exit_code(summary)

    # Apply: take both single-writer ingestion locks before the live StateDB is opened.
    # The index lock comes first, then the NAS lock (this tool writes rag_state.db, which the
    # NAS run owns). Contention exits at once with EXIT_LOCK_HELD (75) and names the holder.
    acquire_or_exit(lock_path_from_config(config))
    acquire_or_exit(nas_ingest_lock_path_from_config(config))

    state_db = StateDB(db_path)
    qdrant_client = get_client(config["rag"]["qdrant_url"], timeout=120)
    try:
        summary = run_backfill(state_db, qdrant_client, False)
    finally:
        state_db.close()
    print_report(summary)
    return exit_code(summary)


if __name__ == "__main__":
    sys.exit(main())
