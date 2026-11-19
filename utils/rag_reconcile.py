#!/usr/bin/env python3
"""
Scheduled reconciliation: make rag_state.db chunk counts and Qdrant points agree.

Runs at the end of every scheduled ingestion run (scripts/ragnart-ingest-run.sh), under the
run-wide lock. It compares, for every queue row, the chunk_count the row records with the
number of Qdrant points stored under its key. It also looks for points whose key has no row
at all. See docs/RAGNART-design.md §10.3 and docs/RAG-strategy.md §5.8.

Counts come from ONE facet call over the source_path payload field (exact=True), not from one
count() call per row. The facet answers "how many points does each key have" for the whole
collection in one request.

Row kinds (see classify_row()):
    ok                     chunk_count > 0 and the points match it exactly. Nothing to do.
    ok_zero                chunk_count 0 and no points. An empty document: consistent.
    fill_null_chunk_count  chunk_count is NULL. Repaired: set to the number of points.
    tombstoned             missing_since is set. The file vanished; its points stay searchable
                           during the 24 h grace period. Never requeued, never an anomaly.
    requeue                chunk_count > 0 and fewer points than that (including none).
                           Repaired: reset_to_pending(), so the next ingestion re-creates the
                           points (ingestion deletes the stale ones first).
    anomaly                Anything else, e.g. more points than chunk_count, or chunk_count 0
                           with points. Logged loudly and NOT repaired: a human looks at it.
    not_completed          Pending, processing, failed or ignored rows. Not reconciled here.

Per-source check (v0.3): for each source_id, the sum of chunk_count over its completed rows is
compared with the number of points stored under that source_id (one facet on source_id). A mismatch
is logged as a WARN and listed in the report. It does not repair anything and does not change the
exit code: it is a signal for a human, not a row kind.

Zombie points: facet keys that have no queue row at all. Their points are deleted, unless the
mass-delete guard trips (see run_reconciliation()).

Hub sources only (v0.3): this tool reads and repairs the hub's own sources, the ones whose kind is
'hub' in the registry. Every facet and every zombie check is restricted to points whose source_id is
one of those sources. Points of an endpoint source (a host that submits through the ingestion API)
are never counted, compared, reported or deleted here: the hub has no queue rows for them.

Usage (run from project root):
    python utils/rag_reconcile.py            # dry-run (default): report only, write nothing
    python utils/rag_reconcile.py --dry-run
    python utils/rag_reconcile.py --apply    # takes the ingestion lock, then repairs

Exit codes: 0 when nothing anomalous remains and the guard did not trip. 1 when an anomaly
was found (anomalies are never repaired) or the mass-delete guard refused the zombie deletes.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import yaml
from qdrant_client.models import FieldCondition, Filter, FilterSelector, MatchAny, MatchValue

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ragnart.cli import open_registry, open_registry_read_only  # noqa: E402
from ragnart.collection import DOCUMENTS_COLLECTION, get_client  # noqa: E402
from ragnart.lock import acquire_or_exit, lock_path_from_config  # noqa: E402
from ragnart.schema import FIELD_SOURCE_ID, FIELD_SOURCE_PATH, source_id_for_key  # noqa: E402
from ragnart.state import StateDB  # noqa: E402

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "ragnart.yaml"

# Upper bound on distinct source_path values returned by one facet call. The live collection
# holds about 13.4k distinct keys. If the facet ever returns this many, the answer may be
# truncated, and a truncated answer would make real rows look empty (and requeue them). So the
# run stops instead of guessing.
FACET_LIMIT = 1_000_000

# Row kinds returned by classify_row(), plus the one non-row kind used in the tally.
KIND_OK = "ok"
KIND_OK_ZERO = "ok_zero"
KIND_FILL_NULL = "fill_null_chunk_count"
KIND_TOMBSTONED = "tombstoned"
KIND_REQUEUE = "requeue"
KIND_ANOMALY = "anomaly"
KIND_NOT_COMPLETED = "not_completed"


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify_row(row: dict, qdrant_point_count: int) -> str:
    """
    Return the kind of one COMPLETED row, given the number of points stored under its key.

    `row` needs the keys chunk_count and missing_since. The order of the checks matters:
      1. A NULL chunk_count is filled first, whatever else is true.
      2. A tombstoned row is never requeued: its file is gone, so re-ingesting it would fail.
         Its points stay, as the grace period requires, and it is not an anomaly.
      3. Equal counts are consistent: ok when there are points, ok_zero when there are none.
      4. Fewer points than a positive chunk_count means the points were lost: requeue.
      5. Everything else (more points than recorded, or points under a zero count) is an
         anomaly. It is reported and left alone.
    """
    chunk_count = row["chunk_count"]
    if chunk_count is None:
        return KIND_FILL_NULL
    if row["missing_since"] is not None:
        return KIND_TOMBSTONED
    if qdrant_point_count == chunk_count:
        return KIND_OK if chunk_count > 0 else KIND_OK_ZERO
    if chunk_count > 0 and qdrant_point_count < chunk_count:
        return KIND_REQUEUE
    return KIND_ANOMALY


# ---------------------------------------------------------------------------
# Qdrant and SQLite helpers
# ---------------------------------------------------------------------------

def count_points_by_source_path(qdrant_client: Any, hub_source_ids: set[str]) -> dict[str, int]:
    """
    Return {source_path: number of points} for every HUB key that has at least one point.

    One facet call answers for the whole collection, filtered to the hub sources: the filter
    keeps any endpoint point out of the answer, so it can never be a zombie. Values with a count
    of 0 are dropped: they carry no points, so they can neither be a zombie nor a point count for
    any row. Raises RuntimeError if the facet hit FACET_LIMIT, because the answer may then be partial.
    """
    response = qdrant_client.facet(
        collection_name=DOCUMENTS_COLLECTION,
        key=FIELD_SOURCE_PATH,
        # Hub points only. An endpoint point has no queue row here, so it would look like a zombie.
        facet_filter=Filter(
            must=[FieldCondition(key=FIELD_SOURCE_ID, match=MatchAny(any=sorted(hub_source_ids)))]
        ),
        exact=True,
        limit=FACET_LIMIT,
    )
    if len(response.hits) >= FACET_LIMIT:
        raise RuntimeError(
            f"facet on {FIELD_SOURCE_PATH} returned {len(response.hits)} values, the limit "
            f"FACET_LIMIT={FACET_LIMIT}; the answer may be truncated, so reconciliation stops"
        )
    return {hit.value: hit.count for hit in response.hits if hit.count > 0}


def count_points_by_source_id(qdrant_client: Any) -> dict[str, int]:
    """
    Return {source_id: number of points} for every source_id that has at least one point.

    One facet call over the source_id payload field. A point with no source_id (not yet
    backfilled) is in no bucket, so it is not counted here.
    """
    response = qdrant_client.facet(
        collection_name=DOCUMENTS_COLLECTION,
        key=FIELD_SOURCE_ID,
        exact=True,
        limit=FACET_LIMIT,
    )
    return {hit.value: hit.count for hit in response.hits if hit.count > 0}


def chunk_sums_by_source(rows: list[dict]) -> dict[str, int]:
    """
    Return {source_id: sum of chunk_count over that source's completed rows}.

    The source of a row is derived from its key with source_id_for_key(), not read from a
    column, so the check does not depend on the backfill having run. A NULL chunk_count adds
    nothing: the repair fills it later in the same run, and this check runs before the repairs.
    """
    sums: Counter = Counter()
    for row in rows:
        if row["status"] == "completed" and row["chunk_count"] is not None:
            sums[source_id_for_key(row["file_path"])] += row["chunk_count"]
    return dict(sums)


def _source_filter(file_path: str) -> Filter:
    """
    Filter matching every point stored for one queue key.

    Two conditions, both required: the source_id (derived from the key by source_id_for_key)
    AND the source_path. Matching on source_path alone could reach a point of another source
    that happens to carry the same path.
    """
    return Filter(
        must=[
            FieldCondition(key=FIELD_SOURCE_ID, match=MatchValue(value=source_id_for_key(file_path))),
            FieldCondition(key=FIELD_SOURCE_PATH, match=MatchValue(value=file_path)),
        ]
    )


def _commit(state_db: StateDB, sql: str, params: tuple) -> None:
    """
    Run one UPDATE on the queue and commit it.

    Same pattern as utils/rag_metadata_backfill.py: the utils scripts write through the
    connection directly, and each repair is committed on its own, so a crash leaves the
    rows that were already repaired repaired and the rest for the next run.
    """
    state_db._conn.execute(sql, params)
    state_db._conn.commit()


# ---------------------------------------------------------------------------
# Whole-run driver
# ---------------------------------------------------------------------------

def run_reconciliation(
    state_db: StateDB,
    qdrant_client: Any,
    dry_run: bool,
    max_delete_fraction: float,
    hub_source_ids: set[str],
) -> dict:
    """
    Classify every row, repair what can be repaired, and delete zombie points.

    In a dry run nothing is written: the summary reports what would be done. The repair
    counts and the zombie deletes are then 'would' figures.

    Hub scope: hub_source_ids (the registry's kind='hub' sources) limits everything this run
    looks at. The facet on source_path is filtered to those sources, so the zombie check, the
    point counts and the mass-delete guard all see hub points only. The per-source check
    iterates the same hub ids and nothing else. An endpoint source is neither compared nor
    logged here, and its points are never deleted here.

    The mass-delete guard for zombie points is the same arithmetic as the sync's guard in
    ragnart/sync_plan.py (its max_delete_fraction check, limit = fraction x total). The measure is POINTS,
    not keys: the damage a zombie delete does is points removed from the index, and the
    total point count comes free from the same facet, so no extra call is needed. Counting
    keys against completed rows would let one large zombie file look small, and would make
    a few tiny zombie keys look like a mass deletion. Under the hub scope, total is the number of
    HUB points only; the arithmetic itself is unchanged.

    The guard tripping does NOT stop the row repairs: those are independent of it. It only
    refuses the zombie deletes, and the run reports the refusal through the summary.

    Returns a summary dict, see the keys assembled at the end of this function.
    """
    if not hub_source_ids:
        # An empty set would match no points, so every queue row would look requeue-able and the
        # guard would see a zero total. Refuse rather than run on that.
        raise ValueError("hub_source_ids is empty: refusing to reconcile without a hub scope")

    points_by_key = count_points_by_source_path(qdrant_client, hub_source_ids)
    points_by_source = count_points_by_source_id(qdrant_client)

    # Every queue row, whatever its status. A key with a row of any status is not a zombie.
    rows = [
        dict(row)
        for row in state_db._conn.execute(
            "SELECT file_path, status, chunk_count, missing_since FROM ingestion_queue "
            "ORDER BY file_path"
        ).fetchall()
    ]
    known_keys = {row["file_path"] for row in rows}

    kinds: Counter = Counter()
    fills: list[tuple[str, int]] = []      # (file_path, points) to write into chunk_count
    requeues: list[tuple[str, int, int]] = []  # (file_path, points, chunk_count)
    anomalies: list[tuple[str, int, int]] = []  # (file_path, points, chunk_count)

    for row in rows:
        if row["status"] != "completed":
            kinds[KIND_NOT_COMPLETED] += 1
            continue
        points = points_by_key.get(row["file_path"], 0)
        kind = classify_row(row, points)
        kinds[kind] += 1

        if kind == KIND_FILL_NULL:
            fills.append((row["file_path"], points))
        elif kind == KIND_REQUEUE:
            requeues.append((row["file_path"], points, row["chunk_count"]))
        elif kind == KIND_ANOMALY:
            # Loud on purpose: an anomaly is never repaired automatically.
            logger.error(
                "Reconciliation anomaly, not repaired: %s chunk_count=%s points=%s",
                row["file_path"], row["chunk_count"], points,
            )
            anomalies.append((row["file_path"], points, row["chunk_count"]))

    # Zombie points: keys with points and no row. Sorted for a deterministic log and delete order.
    zombie_keys = {
        key: count for key, count in sorted(points_by_key.items()) if key not in known_keys
    }
    zombie_points = sum(zombie_keys.values())
    total_points = sum(points_by_key.values())

    # Per-source check: completed chunk_count sum against points stored under each source_id.
    # A WARN only. Computed before the repairs, so it shows the state as found. Only hub sources
    # are checked: an endpoint source has no queue rows here, so it would always mismatch.
    sums = chunk_sums_by_source(rows)
    source_checks = []
    for source_id in sorted(hub_source_ids):
        chunk_sum = sums.get(source_id, 0)
        points = points_by_source.get(source_id, 0)
        matches = chunk_sum == points
        if not matches:
            logger.warning(
                "Per-source check mismatch: source_id=%s chunk_sum=%d points=%d",
                source_id, chunk_sum, points,
            )
        source_checks.append({
            "source_id": source_id,
            "chunk_sum": chunk_sum,
            "points": points,
            "match": matches,
        })
    guard_limit = max_delete_fraction * total_points
    guard_tripped = zombie_points > guard_limit
    for key, count in zombie_keys.items():
        logger.warning("Zombie points, no queue row: %s points=%d", key, count)
    if guard_tripped:
        logger.error(
            "Zombie delete REFUSED: %d zombie point(s) exceed %.0f%% of the %d total points "
            "(limit %.0f). No points were deleted. Investigate before deleting by hand.",
            zombie_points, max_delete_fraction * 100, total_points, guard_limit,
        )

    # Repairs. A dry run writes nothing; the same lists are counted as 'would' figures.
    if not dry_run:
        for file_path, points in fills:
            _commit(
                state_db,
                "UPDATE ingestion_queue SET chunk_count = ? WHERE file_path = ?",
                (points, file_path),
            )
        for file_path, _points, _chunk_count in requeues:
            # Reuses the queue's own requeue method. Ingestion deletes the stale points for
            # this key before it inserts new ones, so the leftover points are not duplicated.
            state_db.reset_to_pending(file_path)

    zombie_points_deleted = 0
    if not dry_run and not guard_tripped:
        for key, count in zombie_keys.items():
            qdrant_client.delete(
                collection_name=DOCUMENTS_COLLECTION,
                points_selector=FilterSelector(filter=_source_filter(key)),
                wait=True,
            )
            zombie_points_deleted += count

    return {
        "dry_run": dry_run,
        "rows": len(rows),
        "kinds": dict(kinds),
        "filled": len(fills),
        "requeued": len(requeues),
        "anomalies": anomalies,
        "sources": source_checks,
        "source_mismatches": sum(1 for check in source_checks if not check["match"]),
        "zombie_keys": zombie_keys,
        "zombie_points": zombie_points,
        "zombie_points_deleted": zombie_points_deleted,
        "total_points": total_points,
        "guard_limit": guard_limit,
        "guard_tripped": guard_tripped,
    }


def exit_code(summary: dict) -> int:
    """1 when an anomaly remains or the guard refused the zombie deletes, else 0."""
    return 1 if (summary["anomalies"] or summary["guard_tripped"]) else 0


# ---------------------------------------------------------------------------
# Reporting and command line
# ---------------------------------------------------------------------------

def print_report(summary: dict) -> None:
    """Print the tally. In a dry run the repair and delete lines say 'would'."""
    verb = "would be" if summary["dry_run"] else "done"
    kinds = summary["kinds"]
    print(f"=== rag_reconcile ({'DRY-RUN' if summary['dry_run'] else 'APPLY'}) ===")
    print(f"  queue rows:              {summary['rows']}")
    for kind in (KIND_OK, KIND_OK_ZERO, KIND_TOMBSTONED, KIND_FILL_NULL, KIND_REQUEUE,
                 KIND_ANOMALY, KIND_NOT_COMPLETED):
        print(f"  {kind:<24} {kinds.get(kind, 0):>8}")
    print(f"  chunk_count filled ({verb}):   {summary['filled']}")
    print(f"  requeued ({verb}):             {summary['requeued']}")
    print(f"  anomalies (not repaired): {len(summary['anomalies'])}")
    for path, points, chunk_count in summary["anomalies"][:20]:
        print(f"    {path} — chunk_count={chunk_count} points={points}")
    # Per-source check. A mismatch is reported only; it neither repairs nor changes the exit code.
    print(f"  per-source check (WARN only): {summary['source_mismatches']} mismatch(es)")
    print(f"    {'source_id':<28} {'chunk_sum':>10} {'points':>10}  match")
    for check in summary["sources"]:
        print(f"    {check['source_id']:<28} {check['chunk_sum']:>10} {check['points']:>10}  "
              f"{'yes' if check['match'] else 'NO'}")
    print(f"  zombie keys:             {len(summary['zombie_keys'])}")
    print(f"  zombie points:           {summary['zombie_points']} "
          f"(of {summary['total_points']} total; limit {summary['guard_limit']:.0f})")
    if summary["dry_run"]:
        # A dry run deletes nothing. The count is what --apply would delete, unless the guard refuses.
        would_delete = 0 if summary["guard_tripped"] else summary["zombie_points"]
        print(f"  zombie points would be deleted: {would_delete}")
    else:
        print(f"  zombie points deleted:   {summary['zombie_points_deleted']}")
    print(f"  mass-delete guard:       {'TRIPPED (zombie delete refused)' if summary['guard_tripped'] else 'ok'}")
    if summary["dry_run"]:
        print()
        print("(dry-run: nothing written. Re-run with --apply to carry out the repairs.)")


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point. Returns the process exit code (see exit_code())."""
    parser = argparse.ArgumentParser(
        description="Reconcile rag_state.db chunk counts with Qdrant points, and delete zombie points."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Report only (the default).")
    mode.add_argument("--apply", action="store_true", help="Repair the rows and delete zombie points.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry_run = not args.apply

    with open(CONFIG_PATH, encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    db_base = config.get("db_base_path", "/mnt/nfs/__Backups/RAGNART--databases")
    db_path = f"{db_base}/sqlite/rag_state.db"
    max_delete_fraction = config["rag"]["sync"]["max_delete_fraction"]

    if dry_run:
        # The dry-run never takes the lock and never writes: SQLite is opened read-only,
        # and Qdrant is only read (facet). The registry is read-only too. It must already be at
        # schema version 2 (the sync applies the migration), or hub_source_ids() fails loudly.
        registry = open_registry_read_only(config)
        try:
            hub_source_ids = registry.hub_source_ids()
        finally:
            registry.close()
        state_db = StateDB(db_path, read_only=True)
        qdrant_client = get_client(config["rag"]["qdrant_url"], timeout=120)
        try:
            summary = run_reconciliation(
                state_db, qdrant_client, True, max_delete_fraction, hub_source_ids
            )
        finally:
            state_db.close()
        print_report(summary)
        return exit_code(summary)

    # Apply: take the single-writer ingestion lock before the live StateDB is opened.
    # Under the scheduled run, the run script already holds it and this returns at once.
    acquire_or_exit(lock_path_from_config(config))

    # Under the lock, the write open of the registry is safe. It also applies a pending migration.
    registry = open_registry(config)
    try:
        hub_source_ids = registry.hub_source_ids()
    finally:
        registry.close()

    state_db = StateDB(db_path)
    qdrant_client = get_client(config["rag"]["qdrant_url"], timeout=120)
    try:
        summary = run_reconciliation(
            state_db, qdrant_client, False, max_delete_fraction, hub_source_ids
        )
    finally:
        state_db.close()
    print_report(summary)
    return exit_code(summary)


if __name__ == "__main__":
    sys.exit(main())
