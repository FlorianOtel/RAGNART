"""
Hub registry: the ragnart project tables, kept apart from the ingestion queue (v0.3, session A).

The registry is one SQLite file, sqlite/hub_registry.db, beside rag_state.db. It holds:
  - sources:            the stores RAGNART ingests from (one row per configured source);
  - projects:           the ragnart projects, with their home source;
  - project_tags:       the tags of each project;
  - project_references: links from a project to sessions or other projects (v0.4 writes them);
  - grants:             who may use a project.

The ingestion queue (ingestion_queue) stays in rag_state.db (ragnart/state.py). The two
files are separate, so each has its own schema version (PRAGMA user_version). The
registry's version starts at 1 and has its own migrations, _MIGRATIONS below.

Opening a RegistryDB with read_only=False creates the file if needed and applies any
pending migration. Opening it with read_only=True never writes.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from .schema import SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_NAS, SOURCE_ID_OPENCODE_K11

logger = logging.getLogger(__name__)

# Schema migrations, applied in __init__. The same rules as ragnart/state.py _MIGRATIONS:
# the version lives in PRAGMA user_version, migrations run in ascending order, and each
# statement runs on its own with any sqlite3.OperationalError ignored, so a migration that
# crashed part-way can be re-run.
_MIGRATIONS: list[tuple[int, list[str]]] = [
    (1, [
        # The sources (stores). secret stays NULL until session B stores a source secret.
        """
        CREATE TABLE IF NOT EXISTS sources (
            id             TEXT PRIMARY KEY,
            connector      TEXT NOT NULL,
            owner_binding  TEXT,
            secret         TEXT
        )
        """,
        # The ragnart projects. home_source_id names the source the project's marker was found in.
        """
        CREATE TABLE IF NOT EXISTS projects (
            id              TEXT PRIMARY KEY,
            path            TEXT,
            name            TEXT,
            owner           TEXT,
            home_source_id  TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS project_tags (
            project_id  TEXT NOT NULL,
            key         TEXT NOT NULL,
            value       TEXT NOT NULL,
            PRIMARY KEY (project_id, key)
        )
        """,
        # Links from a project to a session or another project. kind is checked by the
        # database. working is the v0.4 working flag; v0.3 writes the default only.
        """
        CREATE TABLE IF NOT EXISTS project_references (
            project_id  TEXT NOT NULL,
            kind        TEXT NOT NULL CHECK (kind IN ('session', 'project')),
            target_id   TEXT NOT NULL,
            working     INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (project_id, kind, target_id)
        )
        """,
        # Same columns as the grants table in ragnart/state.py migration 2.
        """
        CREATE TABLE IF NOT EXISTS grants (
            project_id  TEXT NOT NULL,
            principal   TEXT NOT NULL,
            role        TEXT NOT NULL,
            granted_by  TEXT,
            granted_at  TEXT,
            PRIMARY KEY (project_id, principal)
        )
        """,
        # The three sources built in v0.3. The values are the SOURCE_ID_* constants and fixed
        # text, not input. owner_binding says where the source's owner mapping is configured.
        f"INSERT OR IGNORE INTO sources (id, connector, owner_binding) "
        f"VALUES ('{SOURCE_ID_NAS}', 'filesystem', 'crawl roots mapping in ragnart.yaml')",
        f"INSERT OR IGNORE INTO sources (id, connector, owner_binding) "
        f"VALUES ('{SOURCE_ID_CLAUDE_LINUX_SHARED_NFS}', 'claude_code', 'claude_chats roots mapping in ragnart.yaml')",
        f"INSERT OR IGNORE INTO sources (id, connector, owner_binding) "
        f"VALUES ('{SOURCE_ID_OPENCODE_K11}', 'opencode', 'opencode.owner in ragnart.yaml')",
        # The reserved 'global' project: no path, no owner, no home source. The same values
        # as the seed in ragnart/state.py migration 2, without the columns this registry drops.
        "INSERT OR IGNORE INTO projects (id, path, name, owner, home_source_id) "
        "VALUES ('global', NULL, 'Global', NULL, NULL)",
    ]),
]


class RegistryDB:
    """The hub registry: sources, ragnart projects, their tags, references and grants."""

    def __init__(self, db_path: str, read_only: bool = False) -> None:
        """
        Open the registry database.

        With ``read_only=True`` the file is opened through a ``mode=ro`` URI and nothing is
        written: no directory is created, WAL mode is not set and the migrations do not run.
        A write on a read-only RegistryDB raises sqlite3.OperationalError. A missing file
        raises sqlite3.OperationalError too; it is never created.
        """
        self._db_path = db_path
        if read_only:
            # as_uri() percent-encodes characters that are special in a URI.
            uri = Path(db_path).absolute().as_uri() + "?mode=ro"
            self._conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            return
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._apply_migrations()

    def _apply_migrations(self) -> None:
        """
        Bring the registry up to the newest version in _MIGRATIONS.

        Reads PRAGMA user_version, runs every migration above it in order and records each
        applied version. A registry already at the newest version runs nothing.
        """
        current = self._conn.execute("PRAGMA user_version").fetchone()[0]
        for version, statements in _MIGRATIONS:
            if version <= current:
                continue
            for statement in statements:
                try:
                    self._conn.execute(statement)
                except sqlite3.OperationalError:
                    pass  # The work is already in the database (e.g. a table that exists).
            # user_version is set last, so a crash before this line re-runs the whole version.
            self._conn.execute(f"PRAGMA user_version = {int(version)}")
            self._conn.commit()
            logger.info("Applied hub-registry schema migration %d", version)

    # ------------------------------------------------------------------
    # Sources

    def list_sources(self) -> list[dict]:
        """
        Return every registered source as {id, connector, owner_binding}, sorted by id.

        The secret column is never returned, so a caller cannot read a secret through this method.
        """
        cur = self._conn.execute(
            "SELECT id, connector, owner_binding FROM sources ORDER BY id"
        )
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Projects

    def upsert_project(
        self,
        project_id: str,
        path: str | None,
        name: str | None,
        owner: str | None,
        home_source_id: str | None,
    ) -> None:
        """
        Insert a ragnart project, or update the existing row with the same id.

        Idempotent: running the same copy twice leaves one row. Works over the seeded 'global'
        row too, but the sync never calls it for 'global', because 'global' is not a marker project.
        """
        self._conn.execute(
            "INSERT INTO projects (id, path, name, owner, home_source_id) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "    path = excluded.path, name = excluded.name, owner = excluded.owner, "
            "    home_source_id = excluded.home_source_id",
            (project_id, path, name, owner, home_source_id),
        )
        self._conn.commit()

    def list_projects(self) -> list[dict]:
        """
        Return every row of the projects table, including the reserved 'global' row.

        Each dict has id, path, name, owner and home_source_id.
        """
        cur = self._conn.execute(
            "SELECT id, path, name, owner, home_source_id FROM projects ORDER BY id"
        )
        return [dict(row) for row in cur.fetchall()]

    def remove_project(self, project_id: str) -> None:
        """
        Delete a project row and every row keyed on it: tags, references and grants.

        A reference is keyed on the project in two ways: as the referring project (project_id),
        or as a project-kind target (kind 'project', target_id). Both are removed. Items attached
        to the project are not touched here; the sync planner re-attaches them in the same run.
        Never call this for the reserved 'global' id.
        """
        self._conn.execute("DELETE FROM project_tags WHERE project_id = ?", (project_id,))
        self._conn.execute(
            "DELETE FROM project_references "
            "WHERE project_id = ? OR (kind = 'project' AND target_id = ?)",
            (project_id, project_id),
        )
        self._conn.execute("DELETE FROM grants WHERE project_id = ?", (project_id,))
        self._conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))
        self._conn.commit()

    # ------------------------------------------------------------------
    # Tags

    def replace_project_tags(self, project_id: str, tags: dict[str, str]) -> None:
        """
        Replace the whole tag set of one project with `tags`, in one transaction.

        A tag removed from the marker file therefore disappears here too.
        """
        self._conn.execute("DELETE FROM project_tags WHERE project_id = ?", (project_id,))
        self._conn.executemany(
            "INSERT INTO project_tags (project_id, key, value) VALUES (?, ?, ?)",
            [(project_id, key, value) for key, value in tags.items()],
        )
        self._conn.commit()

    def project_tags(self, project_id: str) -> dict[str, str]:
        """Return the tag set of one project as a {key: value} dict. Empty dict if it has none."""
        cur = self._conn.execute(
            "SELECT key, value FROM project_tags WHERE project_id = ? ORDER BY key",
            (project_id,),
        )
        return {row["key"]: row["value"] for row in cur.fetchall()}

    def project_ids_by_tag(self, key: str, value: str) -> list[str]:
        """
        Return the ids of every ragnart project whose tag `key` equals `value`.

        The result is sorted and empty when no project carries that exact tag.
        Used by scope resolution for `tag:<key>=<value>` terms.
        """
        cur = self._conn.execute(
            "SELECT project_id FROM project_tags WHERE key = ? AND value = ? ORDER BY project_id",
            (key, value),
        )
        return [row["project_id"] for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Grants

    def upsert_grant(
        self,
        project_id: str,
        principal: str,
        role: str,
        granted_by: str | None,
        granted_at: str | None,
    ) -> None:
        """
        Insert a grant, or replace the grant that already exists for the same project and principal.

        Idempotent: copying the same grant twice leaves one row. Used by the one-off legacy
        migration (utils/rag_registry_migrate.py), which copies grants verbatim.
        """
        self._conn.execute(
            "INSERT OR REPLACE INTO grants (project_id, principal, role, granted_by, granted_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (project_id, principal, role, granted_by, granted_at),
        )
        self._conn.commit()

    def list_grants(self) -> list[tuple]:
        """Return every grant as a (project_id, principal, role, granted_by, granted_at) tuple."""
        cur = self._conn.execute(
            "SELECT project_id, principal, role, granted_by, granted_at FROM grants "
            "ORDER BY project_id, principal"
        )
        return [tuple(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Tags, every project at once

    def list_all_tags(self) -> list[tuple]:
        """Return every tag row as a (project_id, key, value) tuple, across all projects."""
        cur = self._conn.execute(
            "SELECT project_id, key, value FROM project_tags ORDER BY project_id, key"
        )
        return [tuple(row) for row in cur.fetchall()]

    def close(self) -> None:
        self._conn.close()
