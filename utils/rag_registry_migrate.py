#!/usr/bin/env python3
"""
One-off v0.3 move of the ragnart project tables from rag_state.db to the hub registry.

Design point 4 of the v0.3 plan. Until v0.3, the ragnart project tables live in rag_state.db
(the legacy tables): projects, project_tags, project_links, source_links and grants. v0.3 keeps
them in sqlite/hub_registry.db (ragnart/registry.py). This script moves the data, checks the copy
row for row, and only then drops the legacy tables. It is not an automatic migration: StateDB
never drops a table on open.

Steps. Each one is re-runnable.

    --dry-run (default)            Opens rag_state.db read-only, with a raw sqlite3 connection
                                   (no migration runs). Reports the legacy row counts (a table
                                   that does not exist is 'absent'), what --apply would copy,
                                   and the registry state if the registry file exists.

    --apply                        Under the ingestion lock. Opens StateDB (this applies queue
                                   migration 3, the source_id column, if it is still pending)
                                   and RegistryDB (creates the file if needed). Then:
                                   1. Refuses if project_links or source_links has rows. Those
                                      tables have no target in the new schema; the operator
                                      decides what to do with them.
                                   2. Copies projects (home_source_id = nas for every row with a
                                      path, NULL for 'global'), project tags and grants.
                                   3. Verifies the registry row for row against the legacy
                                      tables: projects (id, path, name, owner), tags, grants.

    --apply --drop-legacy-tables   Does the copy and the verification above. Only when the
                                   verification passed, drops the five legacy tables and sets
                                   rag_state.db user_version to 4, in one transaction. If the
                                   legacy tables are already gone, it verifies the registry has
                                   its projects and reports 'already dropped'.

Why user_version 4. The queue's own migrations go up to 5 (ragnart/state.py). Migration 4 is
empty and is the reserved slot for this drop. Opening StateDB applies migration 4 and migration 5
without touching the legacy tables, so a database at version 5 can still hold them. This tool
decides "already dropped" by table presence, never by user_version. The drop sets user_version 4,
which marks that the legacy-table drop has run. Migration 5 then runs again on the next open. That
is harmless: its ADD COLUMN is ignored when the column already exists. The drop refuses to run
unless the source_id column exists, because a version written onto a database still at version 2
would make StateDB skip migration 3 for good.

Usage (run from project root):
    python utils/rag_registry_migrate.py                              # dry-run (default)
    python utils/rag_registry_migrate.py --dry-run
    python utils/rag_registry_migrate.py --apply
    python utils/rag_registry_migrate.py --apply --drop-legacy-tables

Exit codes: 0 when nothing was refused and the verification passed (or there was nothing to
verify). 1 when refused (project_links or source_links has rows) or when the verification failed.
In a dry-run: 1 when the apply would refuse, else 0.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ragnart.cli import registry_db_path, state_db_path  # noqa: E402
from ragnart.lock import acquire_or_exit, lock_path_from_config  # noqa: E402
from ragnart.registry import RegistryDB  # noqa: E402
from ragnart.schema import SOURCE_ID_NAS  # noqa: E402
from ragnart.state import StateDB  # noqa: E402

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "ragnart.yaml"

# The five legacy tables, in the order they are dropped.
LEGACY_TABLES = ("projects", "project_tags", "project_links", "source_links", "grants")

# Legacy tables with no target in the registry. A non-empty one stops the run.
REFUSE_IF_NONEMPTY = ("project_links", "source_links")

# The user_version rag_state.db is set to by the drop (see the module docstring).
DROP_USER_VERSION = 4


# ---------------------------------------------------------------------------
# Reading the legacy tables
# ---------------------------------------------------------------------------

def table_exists(conn: Any, table: str) -> bool:
    """True when `table` exists in the database behind `conn`."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def _legacy_rows(conn: Any, table: str, columns: str) -> list[tuple]:
    """The rows of one legacy table as tuples. An absent table reads as no rows."""
    if not table_exists(conn, table):
        return []
    return [tuple(row) for row in conn.execute(f"SELECT {columns} FROM {table} ORDER BY {columns}")]


def read_legacy(conn: Any) -> dict:
    """
    Read every legacy table the migration needs.

    Returns:
        {
          "present":  True when the projects table exists (False means already dropped),
          "counts":   {table: row count, or None when the table is absent} for all five tables,
          "projects": [(id, path, name, owner), ...],
          "tags":     [(project_id, key, value), ...],
          "grants":   [(project_id, principal, role, granted_by, granted_at), ...],
        }
    """
    counts = {}
    for table in LEGACY_TABLES:
        if table_exists(conn, table):
            counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        else:
            counts[table] = None
    return {
        "present": table_exists(conn, "projects"),
        "counts": counts,
        "projects": _legacy_rows(conn, "projects", "id, path, name, owner"),
        "tags": _legacy_rows(conn, "project_tags", "project_id, key, value"),
        "grants": _legacy_rows(conn, "grants", "project_id, principal, role, granted_by, granted_at"),
    }


def refusal_for(counts: dict) -> str | None:
    """The reason the run must stop, or None. A non-empty link table is the only refusal."""
    offending = [f"{table} has {counts[table]} row(s)" for table in REFUSE_IF_NONEMPTY if counts[table]]
    if not offending:
        return None
    return (
        "; ".join(offending)
        + ". These tables have no target in the registry schema. The operator decides what to do "
        "with them, and nothing is copied or dropped."
    )


# ---------------------------------------------------------------------------
# Copy and verification
# ---------------------------------------------------------------------------

def copy_legacy_to_registry(legacy: dict, registry: RegistryDB) -> dict:
    """
    Copy projects, tags and grants into the registry. Safe to run twice.

    Each project is upserted with home_source_id = nas when it has a path. The reserved 'global'
    row has no path, so its home source is NULL. Each project's tag set is replaced wholesale, so
    the copy matches the legacy rows exactly. Grants are copied verbatim.

    Returns the number of legacy rows handed to the registry per table.
    """
    tags_by_project: dict[str, dict[str, str]] = {}
    for project_id, key, value in legacy["tags"]:
        tags_by_project.setdefault(project_id, {})[key] = value

    for project_id, path, name, owner in legacy["projects"]:
        home_source_id = SOURCE_ID_NAS if path is not None else None
        registry.upsert_project(project_id, path, name, owner, home_source_id)
        registry.replace_project_tags(project_id, tags_by_project.get(project_id, {}))

    for grant in legacy["grants"]:
        registry.upsert_grant(*grant)

    return {
        "projects": len(legacy["projects"]),
        "tags": len(legacy["tags"]),
        "grants": len(legacy["grants"]),
    }


def _difference(table: str, legacy_rows: list, registry_rows: list) -> str:
    """One line describing how two row sets differ. Compared as sets, so row order does not matter."""
    legacy_set, registry_set = set(legacy_rows), set(registry_rows)
    missing = legacy_set - registry_set
    extra = registry_set - legacy_set
    sample = list(missing | extra)[:3]
    return (
        f"{table}: {len(legacy_rows)} legacy row(s), {len(registry_rows)} registry row(s); "
        f"{len(missing)} missing from the registry, {len(extra)} extra in it; sample {sample!r}"
    )


def verify_copy(legacy: dict, registry: RegistryDB) -> list[str]:
    """
    Compare the registry with the legacy tables row for row. Returns one line per table that
    differs. An empty list means the copy matches.

    The comparison covers the columns that were copied. home_source_id is new in the registry,
    so it is not compared. Each table is compared as a set of tuples, which is exact because each
    legacy table has a unique key.
    """
    problems: list[str] = []

    legacy_projects = [(pid, path, name, owner) for pid, path, name, owner in legacy["projects"]]
    registry_projects = [
        (p["id"], p["path"], p["name"], p["owner"]) for p in registry.list_projects()
    ]
    if set(legacy_projects) != set(registry_projects):
        problems.append(_difference("projects", legacy_projects, registry_projects))

    registry_tags = registry.list_all_tags()
    if set(legacy["tags"]) != set(registry_tags):
        problems.append(_difference("project_tags", legacy["tags"], registry_tags))

    registry_grants = registry.list_grants()
    if set(legacy["grants"]) != set(registry_grants):
        problems.append(_difference("grants", legacy["grants"], registry_grants))

    return problems


# ---------------------------------------------------------------------------
# Dropping the legacy tables
# ---------------------------------------------------------------------------

def drop_legacy_tables(state_db: StateDB) -> None:
    """
    Drop the five legacy tables and set rag_state.db user_version to 4, in one transaction.

    Refuses unless the source_id column exists on ingestion_queue (migration 3 has run). Without
    it, user_version 4 would make StateDB skip migration 3 for good.
    """
    conn = state_db._conn
    columns = {row[1] for row in conn.execute("PRAGMA table_info(ingestion_queue)")}
    if "source_id" not in columns:
        raise RuntimeError(
            "ingestion_queue has no source_id column: queue migration 3 has not run. "
            "user_version 4 would skip it, so the legacy tables are not dropped."
        )

    if conn.in_transaction:
        conn.commit()
    conn.execute("BEGIN")
    try:
        for table in LEGACY_TABLES:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"PRAGMA user_version = {int(DROP_USER_VERSION)}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------------------
# Whole-run drivers
# ---------------------------------------------------------------------------

def _registry_state(registry: RegistryDB | None) -> dict | None:
    """Counts and user_version of the registry, or None when there is no registry file."""
    if registry is None:
        return None
    return {
        "user_version": registry._conn.execute("PRAGMA user_version").fetchone()[0],
        "projects": len(registry.list_projects()),
        "tags": len(registry.list_all_tags()),
        "grants": len(registry.list_grants()),
    }


def dry_run_report(conn: Any, registry: RegistryDB | None) -> dict:
    """Plan only. Reads the legacy tables and the registry, writes nothing."""
    legacy = read_legacy(conn)
    return {
        "dry_run": True,
        "user_version": conn.execute("PRAGMA user_version").fetchone()[0],
        "counts": legacy["counts"],
        "would_copy": {
            "projects": len(legacy["projects"]),
            "tags": len(legacy["tags"]),
            "grants": len(legacy["grants"]),
        },
        "refused": refusal_for(legacy["counts"]),
        "registry": _registry_state(registry),
    }


def run_apply(state_db: StateDB, registry: RegistryDB, drop_legacy: bool) -> dict:
    """
    Copy and verify; drop only when asked, and only when the verification passed.

    Returns a summary. Its `mismatches` list is empty when the copy verified (or when the legacy
    tables were already gone and the registry holds the projects).
    """
    conn = state_db._conn
    legacy = read_legacy(conn)
    report: dict[str, Any] = {
        "dry_run": False,
        "counts": legacy["counts"],
        "refused": refusal_for(legacy["counts"]),
        "already_dropped": False,
        "copied": None,
        "mismatches": [],
        "dropped": False,
    }
    if report["refused"]:
        return report

    if not legacy["present"]:
        # The legacy tables are gone: a re-run after the drop. Nothing to copy or compare.
        report["already_dropped"] = True
        if not registry.list_projects():
            report["mismatches"].append("legacy tables are absent and the registry has no projects")
        return report

    report["copied"] = copy_legacy_to_registry(legacy, registry)
    report["mismatches"] = verify_copy(legacy, registry)
    if drop_legacy and not report["mismatches"]:
        drop_legacy_tables(state_db)
        report["dropped"] = True
    return report


def exit_code(report: dict) -> int:
    """
    The process exit code for a dry-run or an apply report. See the module docstring.

    A refusal is 1 in both modes. A failed verification is 1 for an apply. A dry-run that would
    pass is 0, because it changes nothing and has no verification to fail.
    """
    if report["refused"]:
        return 1
    if report["dry_run"]:
        return 0
    return 1 if report["mismatches"] else 0


# ---------------------------------------------------------------------------
# Reporting and command line
# ---------------------------------------------------------------------------

def _print_counts(counts: dict) -> None:
    """The legacy row counts, one table per line. 'absent' when the table does not exist."""
    print("legacy tables (rows; absent when the table does not exist):")
    for table in LEGACY_TABLES:
        value = counts[table]
        print(f"  {table:<16} {'absent' if value is None else value}")


def print_report(report: dict) -> None:
    """Print a dry-run or an apply report."""
    print(f"=== rag_registry_migrate ({'DRY-RUN' if report['dry_run'] else 'APPLY'}) ===")
    if report["dry_run"]:
        print(f"rag_state.db user_version: {report['user_version']}")
        _print_counts(report["counts"])
        would = report["would_copy"]
        print(f"would copy: projects {would['projects']}, tags {would['tags']}, grants {would['grants']}")
        registry = report["registry"]
        if registry is None:
            print("registry (hub_registry.db): absent")
        else:
            print(f"registry (hub_registry.db): user_version {registry['user_version']}, "
                  f"projects {registry['projects']}, tags {registry['tags']}, grants {registry['grants']}")
        if report["refused"]:
            print(f"REFUSED (would refuse on --apply): {report['refused']}")
        else:
            print()
            print("(dry-run: nothing written. Re-run with --apply to copy and verify.)")
        return

    _print_counts(report["counts"])
    if report["refused"]:
        print(f"REFUSED: {report['refused']}")
        print("Nothing was copied, verified or dropped.")
        return
    if report["already_dropped"]:
        print("already dropped: the legacy tables are absent.")
    else:
        copied = report["copied"]
        print(f"copied: projects {copied['projects']}, tags {copied['tags']}, grants {copied['grants']}")
    if report["mismatches"]:
        print("verification FAILED. Nothing was dropped.")
        for line in report["mismatches"]:
            print(f"  {line}")
    elif not report["already_dropped"]:
        print("verification PASSED: projects, tags and grants match the legacy tables row for row.")
    if report["dropped"]:
        print("legacy tables dropped; rag_state.db user_version set to "
              f"{DROP_USER_VERSION}.")


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point. Returns the process exit code (see exit_code())."""
    parser = argparse.ArgumentParser(
        description="One-off v0.3 move of the ragnart project tables from rag_state.db to the hub registry."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Report only (the default).")
    mode.add_argument("--apply", action="store_true", help="Copy the legacy tables into the registry and verify.")
    parser.add_argument(
        "--drop-legacy-tables",
        action="store_true",
        help="After a passed verification, drop the legacy tables and set user_version 4. Needs --apply.",
    )
    args = parser.parse_args(argv)
    if args.drop_legacy_tables and not args.apply:
        parser.error("--drop-legacy-tables needs --apply: the dry-run never drops anything")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with open(CONFIG_PATH, encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    state_path = state_db_path(config)
    registry_path = registry_db_path(config)

    if not args.apply:
        # The dry-run never takes the lock and never writes. rag_state.db is opened with a raw
        # read-only connection, so no migration can run. The registry is opened read-only too.
        conn = sqlite3.connect(Path(state_path).absolute().as_uri() + "?mode=ro", uri=True)
        registry = RegistryDB(registry_path, read_only=True) if Path(registry_path).exists() else None
        try:
            report = dry_run_report(conn, registry)
        finally:
            conn.close()
            if registry is not None:
                registry.close()
        print_report(report)
        return exit_code(report)

    # Apply: take the single-writer ingestion lock before any database is opened for writing.
    acquire_or_exit(lock_path_from_config(config))
    state_db = StateDB(state_path)
    registry = RegistryDB(registry_path)
    try:
        report = run_apply(state_db, registry, args.drop_legacy_tables)
    finally:
        state_db.close()
        registry.close()
    print_report(report)
    return exit_code(report)


if __name__ == "__main__":
    sys.exit(main())
