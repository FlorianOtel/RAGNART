#!/usr/bin/env python3
"""
Remove one source's items from the hub: its Qdrant points and its intake rows.

An endpoint source (for example MBP or claude-MBP, registered with utils/rag_source_register.py)
submits items through the ingestion API. This tool deletes everything the hub holds for one such
source, so the source can be re-ingested from scratch or retired. It touches nothing else.

What is removed, and what is kept:
  - Removed: every Qdrant point whose source_id is the source's stored id, the source's intake rows
    in the hub registry, and the spool files those rows reference.
  - Kept: the source's registry row and its secret hash. The source stays registered, so its
    secret still authenticates and the endpoint can submit again. Unregistering a source is a
    separate decision and this tool never does it.

The source is looked up case-insensitively (RegistryDB.source_by_id), and every filter uses the
stored id. A source that is the home of at least one project in the hub registry (RegistryDB.list_projects,
home_source_id, case-insensitive) is refused unless --allow-projects-source is given, so its
projects' points cannot be removed by accident. The registry kind is not part of this rule: a source
of any kind that homes no project is removable.

Modes (the dry-run is the default):

    --dry-run (default)   Lists the source's items from Qdrant (grouped by source_path, with their
                          chunk counts), the totals, and the number of its intake rows. Writes
                          nothing and takes no lock.

    --apply               Under the ingestion lock. Deletes the points (filter source_id == the
                          stored id, wait=True), then the intake rows, then the spool files those
                          rows reference. Prints the counts.

Usage (run from project root):
    python utils/rag_source_remove.py --source MBP                  # dry-run (default)
    python utils/rag_source_remove.py --source MBP --apply
    python utils/rag_source_remove.py --source nas --allow-projects-source

Exit codes: 0 when the dry-run listed, or the apply removed. 1 when refused: the source is not
registered, the registry file is absent, or a source homing projects was named without
--allow-projects-source.
75 when --apply finds the ingestion lock held (taken by acquire_or_exit, which exits).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from qdrant_client.models import FieldCondition, Filter, FilterSelector, MatchValue  # noqa: E402

from ragnart.cli import registry_db_path  # noqa: E402
from ragnart.collection import DOCUMENTS_COLLECTION, get_client  # noqa: E402
from ragnart.ingest_api import _scroll_source_points, group_points_by_key  # noqa: E402
from ragnart.lock import acquire_or_exit, lock_path_from_config  # noqa: E402
from ragnart.registry import RegistryDB  # noqa: E402
from ragnart.schema import FIELD_SOURCE_ID  # noqa: E402

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "ragnart.yaml"


# ---------------------------------------------------------------------------
# Reading the source and its items
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    """Read ragnart.yaml. Read at call time, so tests can point CONFIG_PATH elsewhere."""
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def refusal_for(
    registry: RegistryDB, source: dict | None, source_name: str, allow_projects: bool
) -> str | None:
    """
    The reason the removal must stop, or None. Used by both the dry-run and the apply.

    A source that is the home of at least one project in the hub registry has project content among
    its points. It is refused unless allow_projects is set. Home ids compare case-insensitively.
    The registry kind and the set of sources with project support do not decide this.
    """
    if source is None:
        return f"no source is registered as {source_name!r}. Nothing was read or removed."
    homed = [
        project["id"] for project in registry.list_projects()
        if (project["home_source_id"] or "").casefold() == source["id"].casefold()
    ]
    if homed and not allow_projects:
        return (
            f"{source['id']!r} is the home of {len(homed)} ragnart project(s) in the hub registry. "
            "Its points include project content. Pass --allow-projects-source to remove it anyway. "
            "Nothing was read or removed."
        )
    return None


def list_items(client, source_id: str) -> list[dict]:
    """
    The items of one source, read live from Qdrant: one dict per source_path (key), with its chunk
    count, sorted by key. The filter is source_id == source_id, the stored id, never the typed one.
    """
    return group_points_by_key(_scroll_source_points(client, source_id))


def plan_removal(client, registry: RegistryDB, source: dict) -> dict:
    """
    Read what would be removed: the items, their total point count, and the intake rows. Writes nothing.
    """
    items = list_items(client, source["id"])
    return {
        "items": items,
        "points": sum(item["chunk_count"] for item in items),
        "intake_rows": registry.intake_for_source(source["id"]),
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _print_source(source: dict) -> None:
    """The source's identity. The secret hash is never printed."""
    print(f"source: id={source['id']} kind={source['kind']} connector={source['connector']} owner={source['owner']}")


def _print_items(plan: dict) -> None:
    """One line per item of the source: its key and chunk count, then the totals."""
    for item in plan["items"]:
        print(f"  {item['chunk_count']:>6} chunk(s)  {item['key']}")
    print(f"items: {len(plan['items'])}, points: {plan['points']}, intake rows: {len(plan['intake_rows'])}")


# ---------------------------------------------------------------------------
# Dry-run and apply
# ---------------------------------------------------------------------------

def run_dry_run(client, registry_path: str, source_name: str, allow_projects: bool) -> int:
    """List what --apply would remove. Writes nothing and takes no lock. Returns the exit code."""
    print("=== rag_source_remove (DRY-RUN) ===")
    if not Path(registry_path).exists():
        print(f"REFUSED: the hub registry {registry_path} is absent. Nothing was read or removed.")
        return 1
    # Read-only, so a dry-run cannot write to the registry.
    registry = RegistryDB(registry_path, read_only=True)
    try:
        source = registry.source_by_id(source_name)
        reason = refusal_for(registry, source, source_name, allow_projects)
        if reason:
            print(f"REFUSED: {reason}")
            return 1
        plan = plan_removal(client, registry, source)
    finally:
        registry.close()
    _print_source(source)
    _print_items(plan)
    print("(dry-run: nothing written. Re-run with --apply to delete these points and intake rows. "
          "The source stays registered.)")
    return 0


def run_apply(client, registry_path: str, source_name: str, allow_projects: bool) -> int:
    """
    Delete one source's points, intake rows and spool files. The caller already holds the lock.

    Order: points first, then the intake rows, then the spool files the rows named. A failure before
    the rows are deleted leaves them in place, so the run can be repeated.
    """
    print("=== rag_source_remove (APPLY) ===")
    if not Path(registry_path).exists():
        print(f"REFUSED: the hub registry {registry_path} is absent. Nothing was removed.")
        return 1
    registry = RegistryDB(registry_path)
    try:
        source = registry.source_by_id(source_name)
        reason = refusal_for(registry, source, source_name, allow_projects)
        if reason:
            print(f"REFUSED: {reason}")
            return 1
        _print_source(source)

        plan = plan_removal(client, registry, source)
        # The filter names the stored id, so case in the typed id cannot widen or narrow the delete.
        client.delete(
            collection_name=DOCUMENTS_COLLECTION,
            points_selector=FilterSelector(filter=Filter(must=[
                FieldCondition(key=FIELD_SOURCE_ID, match=MatchValue(value=source["id"])),
            ])),
            wait=True,
        )
        rows_deleted = registry.delete_intake_for_source(source["id"])
    finally:
        registry.close()

    spool_removed = 0
    for row in plan["intake_rows"]:
        spool_path = row["spool_path"]
        # A bytes request has a spool file. It is removed after its row is gone; a missing file is not an error.
        if spool_path and os.path.exists(spool_path):
            os.remove(spool_path)
            spool_removed += 1

    print(f"removed: points {plan['points']} (items {len(plan['items'])}), "
          f"intake rows {rows_deleted}, spool files {spool_removed}")
    print(f"the source {source['id']!r} stays registered; its secret still works.")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point. Returns the process exit code (see the module docstring)."""
    parser = argparse.ArgumentParser(
        description="Remove one source's Qdrant points and intake rows from the hub. "
                    "The source stays registered: its registry row and secret are kept."
    )
    parser.add_argument("--source", required=True, help="The source id, e.g. MBP. Case is ignored for the lookup.")
    parser.add_argument(
        "--allow-projects-source",
        action="store_true",
        help="Permit removing a source that is the home of at least one ragnart project in the hub "
             "registry. Without it, such a source is refused.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="List what would be removed (the default).")
    mode.add_argument("--apply", action="store_true", help="Delete the points, intake rows and spool files.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = _load_config()
    registry_path = registry_db_path(config)
    client = get_client(config["rag"]["qdrant_url"])

    if not args.apply:
        # The dry-run takes no lock and writes nothing: the registry is opened read-only.
        return run_dry_run(client, registry_path, args.source, args.allow_projects_source)

    # Apply: take the single-writer ingestion lock before the registry or Qdrant is written.
    acquire_or_exit(lock_path_from_config(config))
    return run_apply(client, registry_path, args.source, args.allow_projects_source)


if __name__ == "__main__":
    sys.exit(main())
