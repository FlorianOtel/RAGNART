#!/usr/bin/env python3
"""
Reset the RAG pipeline — drop Qdrant collection and reset the ingestion queue.

Full reset (no --user): drops the entire Qdrant 'documents' collection and
resets all SQLite rows back to 'pending'.

Partial reset (--user florian): deletes only Florian's Qdrant points and
resets only Florian's SQLite rows to 'pending'. Other users are untouched.

Alias refusal: if 'documents' resolves to a Qdrant alias (not a real
collection, as after rag_sparse_migrate.py --swap), a FULL reset refuses with
a non-zero exit and drops nothing. The manual procedure is in
docs/RAG-strategy.md §9.3.

Dry run (--dry-run, either mode): prints the counts of what would be deleted
and reset. It makes no Qdrant delete calls and no SQLite writes; the SQLite
file is opened read-only.

Usage (run from project root):
    python utils/rag_reset.py --dry-run                # counts only, full reset
    python utils/rag_reset.py --user florian --dry-run # counts only, partial reset
    python utils/rag_reset.py                          # full reset (prompts for confirmation)
    python utils/rag_reset.py --user florian           # partial reset for one user
    python utils/rag_reset.py --yes                    # skip confirmation prompt
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml  # noqa: E402

from qdrant_client import QdrantClient  # noqa: E402
from qdrant_client.models import FieldCondition, Filter, FilterSelector, MatchValue  # noqa: E402

from ragnart.collection import (  # noqa: E402
    DOCUMENTS_COLLECTION,
    collection_exists,
    collection_is_alias,
    ensure_collection,
    get_client,
)
from ragnart.lock import acquire_or_exit, lock_path_from_config  # noqa: E402
from ragnart.schema import FIELD_OWNER  # noqa: E402
from ragnart.state import StateDB  # noqa: E402

# Used only when config/ragnart.yaml omits rag.qdrant_url.
_DEFAULT_QDRANT_URL = "http://192.168.1.93:6333"

# Reference printed in the alias-refusal message.
_ALIAS_RUNBOOK = "docs/RAG-strategy.md §9.3"

# SQL fragment that resets queue rows to 'pending'. Shared by both reset modes.
_RESET_ASSIGNMENTS = (
    "status = 'pending', retry_count = 0, error_msg = NULL, "
    "started_at = NULL, completed_at = NULL, progress_detail = NULL"
)


class AliasResetRefused(Exception):
    """A full reset was requested but DOCUMENTS_COLLECTION is a Qdrant alias."""


def _owner_filter(owner: str) -> Filter:
    """Qdrant filter matching only points whose owner payload equals `owner`."""
    return Filter(must=[FieldCondition(key=FIELD_OWNER, match=MatchValue(value=owner))])


def _refuse_if_alias(qdrant: QdrantClient) -> None:
    """Raise AliasResetRefused if DOCUMENTS_COLLECTION is an alias.

    A full reset drops the collection by its real name. If the name is an alias,
    that drop would be wrong, and a mass delete through the alias is not
    implemented here. So we stop before touching anything.
    """
    if collection_is_alias(qdrant, DOCUMENTS_COLLECTION):
        raise AliasResetRefused(
            f"'{DOCUMENTS_COLLECTION}' is a Qdrant alias, not a real collection. "
            "A full reset refuses to act on an alias. Nothing was deleted. "
            f"See {_ALIAS_RUNBOOK} for the manual procedure."
        )


def partial_reset(qdrant: QdrantClient, conn: sqlite3.Connection, owner: str, dry_run: bool) -> None:
    """Delete one owner's Qdrant points and reset that owner's queue rows.

    With dry_run=True this only counts. It issues no Qdrant delete and no SQLite
    UPDATE. `conn` should be read-only in that case (see main()).
    """
    # Only query Qdrant if the collection exists. Otherwise there is nothing to count.
    has_collection = collection_exists(qdrant, DOCUMENTS_COLLECTION)
    points = 0
    if has_collection:
        points = qdrant.count(
            DOCUMENTS_COLLECTION, count_filter=_owner_filter(owner), exact=True
        ).count
    rows = conn.execute(
        "SELECT COUNT(*) FROM ingestion_queue WHERE owner = ?", (owner,)
    ).fetchone()[0]

    if dry_run:
        print(f"[dry-run] would delete {points} Qdrant point(s) for owner={owner}")
        print(f"[dry-run] would reset {rows} SQLite row(s) to 'pending' for owner={owner}")
        return

    if has_collection:
        qdrant.delete(
            collection_name=DOCUMENTS_COLLECTION,
            points_selector=FilterSelector(filter=_owner_filter(owner)),
        )
        print(f"Deleted Qdrant points for owner={owner}")

    rows_reset = conn.execute(
        f"UPDATE ingestion_queue SET {_RESET_ASSIGNMENTS} WHERE owner = ?", (owner,)
    ).rowcount
    conn.commit()
    print(f"Reset {rows_reset} SQLite row(s) to 'pending' for owner={owner}")


def full_reset(qdrant: QdrantClient, conn: sqlite3.Connection, dry_run: bool) -> None:
    """Drop the whole documents collection, re-create it empty, reset all queue rows.

    Refuses (raises AliasResetRefused) when the name is an alias. In that case
    no delete_collection call and no SQLite write happen.
    With dry_run=True this only counts. It issues no delete and no SQLite UPDATE.
    """
    _refuse_if_alias(qdrant)

    # The alias case was excluded above, so a hit here means a real collection.
    has_collection = collection_exists(qdrant, DOCUMENTS_COLLECTION)
    points = 0
    if has_collection:
        points = qdrant.count(DOCUMENTS_COLLECTION, exact=True).count
    rows = conn.execute("SELECT COUNT(*) FROM ingestion_queue").fetchone()[0]

    if dry_run:
        print(f"[dry-run] would drop Qdrant collection '{DOCUMENTS_COLLECTION}' ({points} point(s))")
        print(f"[dry-run] would reset {rows} SQLite row(s) to 'pending'")
        return

    if has_collection:
        qdrant.delete_collection(DOCUMENTS_COLLECTION)
        print(f"Dropped Qdrant collection '{DOCUMENTS_COLLECTION}'")
    ensure_collection(qdrant)
    print(f"Re-created empty collection '{DOCUMENTS_COLLECTION}'")

    rows_reset = conn.execute(f"UPDATE ingestion_queue SET {_RESET_ASSIGNMENTS}").rowcount
    conn.commit()
    print(f"Reset {rows_reset} SQLite row(s) to 'pending'")


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset the RAG pipeline")
    parser.add_argument("--user", metavar="OWNER",
                        help="Partial reset: only this owner's data (e.g. florian)")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print counts of what would change; change nothing")
    args = parser.parse_args()

    with open(Path(__file__).resolve().parent.parent / "ragnart.yaml") as f:
        config = yaml.safe_load(f)

    rag_cfg = config.get("rag", {})
    db_base = config.get("db_base_path", "/mnt/nfs/__Backups/RAGNART--databases")
    db_path = f"{db_base}/sqlite/rag_state.db"
    qdrant = get_client(rag_cfg.get("qdrant_url", _DEFAULT_QDRANT_URL))

    # Refuse a full reset on an alias before asking for confirmation, so the
    # operator is never prompted for an action that will be refused.
    if not args.user:
        try:
            _refuse_if_alias(qdrant)
        except AliasResetRefused as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            sys.exit(1)

    scope = f"owner={args.user}" if args.user else "ALL USERS"
    action = (
        f"Delete Qdrant points for {scope} and reset SQLite rows to 'pending'."
        if args.user else
        f"DROP the entire Qdrant '{DOCUMENTS_COLLECTION}' collection and reset all SQLite rows."
    )

    print(f"\n{action}")
    if args.dry_run:
        print("[dry-run] no changes will be made")
    elif not args.yes:
        confirm = input("Confirm? [y/N] ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            sys.exit(0)

    if args.dry_run:
        # StateDB() runs schema DDL and an UPDATE migration on open, which would
        # write to the database. A dry run must not write, so open read-only.
        state_db = None
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    else:
        # This is the non-dry-run branch, reached only after the confirmation gate above
        # has passed (either --yes, or an explicit "y" at the prompt). The single-writer
        # lock is taken here, before StateDB opens the queue and before any Qdrant delete
        # or collection drop.
        #
        # On contention, acquire_or_exit() prints the current holder to stderr and exits
        # with EXIT_LOCK_HELD (75). It does not wait. Nothing is reset in that case.
        # The lock is held until this process exits.
        acquire_or_exit(lock_path_from_config(config))

        state_db = StateDB(db_path)
        conn = state_db._conn

    try:
        if args.user:
            partial_reset(qdrant, conn, args.user, dry_run=args.dry_run)
        else:
            try:
                full_reset(qdrant, conn, dry_run=args.dry_run)
            except AliasResetRefused as exc:
                print(f"ERROR: {exc}", file=sys.stderr)
                sys.exit(1)
    finally:
        if state_db is None:
            conn.close()

    if state_db is not None:
        counts = state_db.get_counts()
        print(f"\nQueue: {counts['pending']} pending, {counts['completed']} completed, {counts['failed']} failed")
        state_db.close()


if __name__ == "__main__":
    main()
