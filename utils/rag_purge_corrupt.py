#!/usr/bin/env python3
"""
Report corrupt content in Qdrant (low printable-char ratio). Read-only: nothing is deleted.

Files with parent_text containing mostly non-printable characters (formatting noise,
binary artifacts, encoding issues) are identified and listed, with the minimum printable
ratio seen for each path.

Corrupt points are cross-referenced with SQLite to show whether the file is still
queued for re-ingestion (IN_DB) or already completed (NOT_IN_DB).

Scope: every source by default. --source limits the scan to one source id, exactly as the
source is stored (for example nas). --user limits it to one owner. Both can be combined.

The tool never writes. It opens rag_state.db read-only and Qdrant for reading only. Deleting corrupt
points is not a function of this tool any more: the ingestion side owns the index and removes points
through the ingestion API (ragnart/sync_submit.py for NAS items).

Usage (run from project root):
    python utils/rag_purge_corrupt.py
    python utils/rag_purge_corrupt.py --threshold 0.8
    python utils/rag_purge_corrupt.py --user florian
    python utils/rag_purge_corrupt.py --source nas
    python utils/rag_purge_corrupt.py --save /tmp/corrupt_paths.txt
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml  # noqa: E402

from qdrant_client.models import FieldCondition, Filter, MatchValue  # noqa: E402

from ragnart.collection import DOCUMENTS_COLLECTION, get_client  # noqa: E402
from ragnart.schema import (  # noqa: E402
    FIELD_OWNER,
    FIELD_PARENT_TEXT,
    FIELD_SOURCE_ID,
    FIELD_SOURCE_PATH,
)
# Shared gate rule and threshold: the same definition the ingest pipeline uses.
from ragnart.textquality import GATE_THRESHOLD, printable_ratio  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def scan_qdrant_for_corrupt(
    client,
    threshold: float,
    user_filter: str | None = None,
    source_filter: str | None = None,
    batch_size: int = 500,
) -> dict:
    """
    Scroll Qdrant points, identify corrupt ones, and return metadata. Reads only.

    The scroll filter holds the conditions that were given: the source (when source_filter is set) and the
    owner (when user_filter is set), ANDed together. With neither, every point is scrolled.

    Returns:
        {
            "corrupt_paths": {path: printable_ratio, ...},
            "corrupt_points": {path: count, ...},
            "total_points": int,
            "total_corrupt_points": int,
        }
    """
    corrupt_paths = {}  # path -> min_printable_ratio
    corrupt_points = {}  # path -> point count
    total_points = 0
    total_corrupt_points = 0

    must = []
    if source_filter:
        must.append(FieldCondition(key=FIELD_SOURCE_ID, match=MatchValue(value=source_filter)))
    if user_filter:
        must.append(FieldCondition(key=FIELD_OWNER, match=MatchValue(value=user_filter)))
    # No condition means no filter: every source and every owner is scrolled.
    scroll_filter = Filter(must=must) if must else None

    # Scroll all points
    offset = 0
    while True:
        # Retry on transient Qdrant 500s ("timed out after 0ns") that occur when
        # the optimizer holds a segment lock during merge/vacuum.
        last_exc = None
        for attempt in range(5):
            try:
                points, next_offset = client.scroll(
                    collection_name=DOCUMENTS_COLLECTION,
                    limit=batch_size,
                    offset=offset,
                    with_payload=True,
                    scroll_filter=scroll_filter,
                )
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                wait = 10 * (attempt + 1)
                logger.warning(
                    "Scroll attempt %d/5 failed (optimizer busy?): %s — retrying in %ds",
                    attempt + 1, e, wait,
                )
                import time
                time.sleep(wait)
        if last_exc is not None:
            logger.error("Failed to scroll Qdrant after 5 attempts: %s", last_exc)
            raise last_exc

        if not points:
            break

        for point in points:
            total_points += 1
            if total_points % 50000 == 0:
                logger.info("Scanned %d points so far...", total_points)

            payload = point.payload or {}
            parent_text = payload.get(FIELD_PARENT_TEXT, "")
            source_path = payload.get(FIELD_SOURCE_PATH, "")

            if not source_path or not parent_text:
                continue

            ratio = printable_ratio(parent_text)
            if ratio < threshold:
                total_corrupt_points += 1

                # Track the minimum ratio seen for this path
                if source_path not in corrupt_paths:
                    corrupt_paths[source_path] = ratio
                    corrupt_points[source_path] = 1
                else:
                    corrupt_paths[source_path] = min(corrupt_paths[source_path], ratio)
                    corrupt_points[source_path] += 1

        offset = next_offset
        if not next_offset:
            break

    return {
        "corrupt_paths": corrupt_paths,
        "corrupt_points": corrupt_points,
        "total_points": total_points,
        "total_corrupt_points": total_corrupt_points,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report corrupt Qdrant points (read-only; nothing is deleted)"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=GATE_THRESHOLD,
        help=f"Printable-char ratio threshold (default {GATE_THRESHOLD}, from ragnart.textquality); points below this are corrupt",
    )
    parser.add_argument(
        "--user",
        metavar="OWNER",
        help="Filter to a specific owner (e.g. florian)",
    )
    parser.add_argument(
        "--source",
        metavar="SOURCE_ID",
        help="Filter to one source id, as stored (e.g. nas). Default: every source",
    )
    parser.add_argument(
        "--save",
        metavar="FILE",
        help="Write corrupt source_path list (one per line) to FILE",
    )
    args = parser.parse_args()

    # Load config
    config_path = Path(__file__).resolve().parent.parent / "ragnart.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    db_base = config.get("db_base_path", "/mnt/nfs/__Backups/RAGNART--databases")
    db_path = f"{db_base}/sqlite/rag_state.db"
    qdrant_url = config.get("rag", {}).get("qdrant_url", "http://192.168.1.93:6333")

    # Open Qdrant client and a read-only SQLite connection.
    # Use read-only URI to avoid any WAL writes on NFS — this utility never
    # modifies SQLite, only reads it for cross-reference reporting.
    qdrant_client = get_client(qdrant_url, timeout=120)
    state_conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    state_conn.row_factory = sqlite3.Row

    print()
    print(f"Scanning Qdrant for corrupt content (threshold: {args.threshold:.2f})")
    print(f"  Scope: {'source ' + args.source if args.source else 'every source'}")
    if args.user:
        print(f"  Filter: owner={args.user}")
    print()

    # Scan
    result = scan_qdrant_for_corrupt(
        qdrant_client,
        threshold=args.threshold,
        user_filter=args.user,
        source_filter=args.source,
        batch_size=500,
    )

    corrupt_paths = result["corrupt_paths"]
    corrupt_points = result["corrupt_points"]
    total_points = result["total_points"]
    total_corrupt_points = result["total_corrupt_points"]

    print(f"Total points scanned: {total_points:,}")
    print(f"Total corrupt points: {total_corrupt_points:,}")
    print()

    if not corrupt_paths:
        print("No corrupt content detected.")
        state_conn.close()
        return

    # Build report: check SQLite for each path (read-only)
    report = []
    for path in sorted(corrupt_paths.keys()):
        ratio = corrupt_paths[path]
        point_count = corrupt_points[path]

        try:
            cur = state_conn.execute(
                "SELECT status FROM ingestion_queue WHERE file_path = ?",
                (path,),
            )
            row = cur.fetchone()
            in_db = "IN_DB" if row else "NOT_IN_DB"
        except Exception as e:
            logger.warning("Failed to check SQLite for %s: %s", path, e)
            in_db = "UNKNOWN"

        pct = ratio * 100
        report.append((path, point_count, in_db, pct, ratio))

    # Print report (sorted by path)
    print(f"Corrupt source paths ({len(report)} unique):")
    print()
    for path, point_count, in_db, pct, ratio in report:
        print(f"  [{point_count:>4} pts]  {in_db:<12}  {pct:>5.1f}%  {path}")
    print()

    # Save to file if requested
    if args.save:
        with open(args.save, "w") as f:
            for path, _, _, _, _ in report:
                f.write(path + "\n")
        print(f"Saved corrupt path list to {args.save}")
        print()

    print("Read-only report: nothing was deleted.")
    print()

    state_conn.close()


if __name__ == "__main__":
    main()
