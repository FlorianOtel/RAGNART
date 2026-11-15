"""
One-shot RAG pipeline status: queue counts, Qdrant point counts and the ignored listing.

This is the display shared by two entry points, so the logic lives in one place:

  - utils/rag_status.py  (operator script; keeps its own --watch and --list-pending modes)
  - ragnart/cli.py       (`ragnart status`; opens the queue read-only)

The caller supplies the StateDB. The display only reads from it. Qdrant is only
read too: collection_exists() and count().
"""

from __future__ import annotations

from qdrant_client.models import FieldCondition, Filter, MatchValue

from .collection import DOCUMENTS_COLLECTION, collection_exists, get_client
from .schema import FIELD_OWNER
from .state import StateDB


def print_status(
    state_db: StateDB,
    qdrant_url: str,
    user: str | None = None,
    show_ignored: bool = False,
) -> None:
    """
    Print the queue counts, the Qdrant point counts and, optionally, the ignored files.

    Args:
        state_db:     Queue database to count from. Only read.
        qdrant_url:   Qdrant server to count points on. Only read.
        user:         If set, counts are filtered to this owner.
        show_ignored: If True, also list every ignored file with its retry count and reason.

    A Qdrant failure is printed as a line and does not stop the display.
    """
    # --- SQLite queue counts ---
    if user:
        conn = state_db._conn
        cur = conn.execute(
            "SELECT status, COUNT(*) AS n FROM ingestion_queue "
            "WHERE owner = ? GROUP BY status",
            (user,),
        )
        raw = {row["status"]: row["n"] for row in cur.fetchall()}
        counts = {
            "pending":    raw.get("pending", 0),
            "processing": raw.get("processing", 0),
            "completed":  raw.get("completed", 0),
            "ignored":    raw.get("ignored", 0),
            "total":      sum(raw.values()),
        }
        scope = f"owner={user}"
    else:
        counts = state_db.get_counts()
        scope = "all users"

    print(f"\nIngestion queue ({scope}):")
    print(f"  pending    : {counts['pending']}")
    print(f"  processing : {counts['processing']}")
    print(f"  completed  : {counts['completed']}")
    print(f"  ignored    : {counts['ignored']}")
    print(f"  ─────────────────")
    print(f"  total      : {counts['total']}")

    if counts["total"] > 0:
        pct = counts["completed"] / counts["total"] * 100
        print(f"  progress   : {pct:.1f}%")

    # --- Qdrant stats ---
    try:
        qdrant = get_client(qdrant_url)
        # collection_exists() also accepts an alias, so this finds the 'documents'
        # name whether it is a real collection or an alias (after --swap).
        if collection_exists(qdrant, DOCUMENTS_COLLECTION):
            total_pts = qdrant.count(DOCUMENTS_COLLECTION, exact=True).count
            if user:
                user_pts = qdrant.count(
                    DOCUMENTS_COLLECTION,
                    count_filter=Filter(must=[
                        FieldCondition(key=FIELD_OWNER, match=MatchValue(value=user))
                    ]),
                    exact=True,
                ).count
                print(f"\nQdrant '{DOCUMENTS_COLLECTION}' collection:")
                print(f"  total points    : {total_pts}")
                print(f"  {user:15s} : {user_pts}")
            else:
                print(f"\nQdrant '{DOCUMENTS_COLLECTION}' collection:")
                print(f"  total points    : {total_pts}")
        else:
            print(f"\nQdrant: collection '{DOCUMENTS_COLLECTION}' does not exist yet")
    except Exception as exc:
        print(f"\nQdrant: unavailable ({exc})")

    # --- Ignored files ---
    if show_ignored:
        ignored = state_db.get_ignored(owner=user or None)
        if ignored:
            print(f"\nIgnored files ({len(ignored)}) — permanently skipped after retry exhaustion:")
            for row in ignored:
                print(f"  [{row['retry_count']} retries] {row['file_path']}")
                if row["skip_reason"]:
                    print(f"    → {row['skip_reason']}")
        else:
            print("\nNo ignored files.")
