"""
Hub registry: the ragnart project tables, kept apart from the ingestion queue (v0.3, session A).

The registry is one SQLite file, sqlite/hub_registry.db, beside rag_state.db. It holds:
  - sources:            the stores RAGNART ingests from: hub sources (ingested by the hub) and
                        endpoint sources (submitted to by a host through the ingestion API);
  - intake:             requests received from endpoint sources, waiting for the hub's intake
                        worker, and their results;
  - projects:           the ragnart projects, with their home source;
  - project_tags:       the tags of each project;
  - project_references: links from a project to sessions or other projects (v0.4 writes them);
  - grants:             who may use a project.

The ingestion queue (ingestion_queue) stays in rag_state.db (ragnart/state.py). The two
files are separate, so each has its own schema version (PRAGMA user_version). The
registry's schema version is 2; its migrations are in _MIGRATIONS below.

Source ids are case-preserving for storage and display, and every comparison of them is
case-insensitive. A unique index on sources(id COLLATE NOCASE) makes 'MBP' and 'mbp' the
same source, so only one of them can be registered.

Opening a RegistryDB with read_only=False creates the file if needed and applies any
pending migration. Opening it with read_only=True never writes.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .schema import (
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
    validate_source_id,
)

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
    (2, [
        # secret is renamed, not replaced: migration 1 left it empty, so nothing is lost. The
        # column now holds only the SHA-256 hex digest of a source's secret, never the secret.
        "ALTER TABLE sources RENAME COLUMN secret TO secret_hash",
        # kind is 'hub' for a source the hub ingests itself, 'endpoint' for a source that a host
        # submits to through the ingestion API. The default makes every existing row a hub source.
        # The CHECK refuses any other value, so a bad kind cannot be written even by direct SQL.
        "ALTER TABLE sources ADD COLUMN kind TEXT NOT NULL DEFAULT 'hub' "
        "CHECK (kind IN ('hub', 'endpoint'))",
        # owner is the fixed owner of an endpoint source. Hub sources keep owner_binding instead.
        "ALTER TABLE sources ADD COLUMN owner TEXT",
        # Source ids are unique case-insensitively, so 'MBP' and 'mbp' cannot both be registered.
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_sources_id_nocase ON sources(id COLLATE NOCASE)",
        # The three sources built in v0.3 are hub sources. Set explicitly, although the default
        # already gives them 'hub'. The values are the SOURCE_ID_* constants, not input.
        "UPDATE sources SET kind = 'hub' "
        f"WHERE id IN ('{SOURCE_ID_NAS}', '{SOURCE_ID_CLAUDE_LINUX_SHARED_NFS}', '{SOURCE_ID_OPENCODE_K11}')",
        # One row per request from an endpoint source. kind says what to do, status says how far
        # the hub's intake worker has got. Payload columns are filled as the kind needs them:
        # text for an upsert sent as text, spool_path for an upsert sent as bytes, new_key for a
        # change_key. metadata holds a JSON object. modified_at is the item's modification time as
        # an ISO-8601 UTC string, when the endpoint sent one. The result columns are set by the worker.
        """
        CREATE TABLE IF NOT EXISTS intake (
            id                TEXT PRIMARY KEY,
            source_id         TEXT NOT NULL,
            kind              TEXT NOT NULL CHECK (kind IN ('upsert', 'delete', 'change_key', 'change_metadata')),
            key               TEXT NOT NULL,
            new_key           TEXT,
            file_type         TEXT,
            text              TEXT,
            metadata          TEXT,
            modified_at       TEXT,
            spool_path        TEXT,
            status            TEXT NOT NULL DEFAULT 'received'
                              CHECK (status IN ('received', 'processing', 'completed', 'ignored', 'failed')),
            admission_reason  TEXT,
            error             TEXT,
            chunk_count       INTEGER,
            content_hash      TEXT,
            pipeline_version  TEXT,
            created_at        TEXT NOT NULL,
            updated_at        TEXT NOT NULL
        )
        """,
        # The intake worker takes the oldest 'received' rows, so this index serves that query.
        "CREATE INDEX IF NOT EXISTS idx_intake_status_created ON intake (status, created_at)",
    ]),
]

# The result columns of an intake row that the hub's intake worker may set. The request's own
# fields (source, kind, key, text, metadata, ...) are fixed at insert time and never listed here.
_UPDATABLE_INTAKE_FIELDS = frozenset({
    "status",
    "spool_path",
    "admission_reason",
    "error",
    "chunk_count",
    "content_hash",
    "pipeline_version",
})


def _now() -> str:
    """
    Return the current UTC time as an ISO-8601 string with a fixed six-digit microsecond field.

    The fixed width keeps string order equal to time order, which the intake queue relies on.
    """
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _intake_row_to_dict(row: sqlite3.Row) -> dict:
    """Return an intake row as a dict, with its metadata JSON decoded back into a dict."""
    data = dict(row)
    if data["metadata"] is not None:
        data["metadata"] = json.loads(data["metadata"])
    return data


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

        The secret_hash column is never returned, so a caller cannot read a secret hash through
        this method. source_by_id() returns the hash, for the one caller that must check it.
        """
        cur = self._conn.execute(
            "SELECT id, connector, owner_binding FROM sources ORDER BY id"
        )
        return [dict(row) for row in cur.fetchall()]

    def register_source(
        self,
        source_id: str,
        connector: str,
        kind: str,
        owner: str | None,
        secret_hash: str,
    ) -> None:
        """
        Register a new source. Only the SHA-256 hex digest of its secret is stored, never the secret.

        The id is checked with validate_source_id() first, then stored as given (case preserved).
        An id that matches an existing one case-insensitively is refused: the unique index on
        sources(id COLLATE NOCASE) rejects it, and that rejection is raised here as ValueError.

        Args:
            source_id:   The id to register, e.g. 'MBP'.
            connector:   The connector type, e.g. 'filesystem' or 'claude_code'.
            kind:        'hub' (ingested by the hub itself) or 'endpoint' (submitted to by a host).
            owner:       The fixed owner of an endpoint source; None for a hub source.
            secret_hash: The SHA-256 hex digest of the source's secret.

        Raises:
            ValueError: the id is invalid or reserved, the kind is unknown, or the id is already
                        registered in any letter case.
        """
        validate_source_id(source_id)
        if kind not in ("hub", "endpoint"):
            raise ValueError(f"unknown source kind {kind!r}: use 'hub' or 'endpoint'")
        try:
            self._conn.execute(
                "INSERT INTO sources (id, connector, kind, owner, secret_hash) VALUES (?, ?, ?, ?, ?)",
                (source_id, connector, kind, owner, secret_hash),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError(
                f"source id {source_id!r} is already registered (ids are compared case-insensitively)"
            ) from exc
        self._conn.commit()

    def source_by_id(self, source_id: str) -> dict | None:
        """
        Return the source whose id equals source_id, compared case-insensitively, or None.

        The dict has id (the stored id, in its stored case), connector, kind, owner and
        secret_hash. The request gate uses secret_hash to check a presented secret. The
        plaintext secret is never stored, so it cannot be returned.
        """
        row = self._conn.execute(
            "SELECT id, connector, kind, owner, secret_hash FROM sources WHERE id = ? COLLATE NOCASE",
            (source_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    def endpoint_sources(self) -> list[dict]:
        """
        Return every endpoint source (kind 'endpoint') as {id, connector, owner, secret_hash}.

        The ingestion API's auth gate compares a presented secret's hash with each of these.
        Hub sources are left out: they have no secret, so they can never authenticate.
        """
        cur = self._conn.execute(
            "SELECT id, connector, owner, secret_hash FROM sources WHERE kind = 'endpoint' ORDER BY id"
        )
        return [dict(row) for row in cur.fetchall()]

    def hub_source_ids(self) -> set[str]:
        """
        Return the ids of the hub sources (kind 'hub'): the sources the hub ingests itself.

        The hub-side tools (reconciliation, purges) act only on these. Endpoint sources are
        left out, so their points are never compared, deleted or reported by those tools.
        """
        rows = self._conn.execute("SELECT id FROM sources WHERE kind = 'hub'").fetchall()
        return {row["id"] for row in rows}

    # ------------------------------------------------------------------
    # Intake: requests from endpoint sources

    def insert_intake(
        self,
        source_id: str,
        kind: str,
        key: str,
        *,
        new_key: str | None = None,
        file_type: str | None = None,
        text: str | None = None,
        metadata: dict | None = None,
        modified_at: str | None = None,
        spool_path: str | None = None,
        request_id: str | None = None,
    ) -> str:
        """
        Record one request from an endpoint source with status 'received', and return its request id.

        The request id is a uuid4 hex string unless the caller passes one. The API passes its own,
        so the spool file can be named after the request before the row exists: the row is then
        inserted with spool_path already set, and the intake worker never sees a bytes upsert
        without its file. The table checks kind (upsert, delete, change_key or change_metadata).
        metadata is a dict, stored as JSON and returned as a dict. modified_at is stored as given;
        the API has already checked it is an ISO-8601 UTC string.
        """
        if request_id is None:
            request_id = uuid.uuid4().hex
        now = _now()
        self._conn.execute(
            "INSERT INTO intake (id, source_id, kind, key, new_key, file_type, text, metadata, "
            "modified_at, spool_path, status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'received', ?, ?)",
            (
                request_id, source_id, kind, key, new_key, file_type, text,
                None if metadata is None else json.dumps(metadata),
                modified_at, spool_path, now, now,
            ),
        )
        self._conn.commit()
        return request_id

    def intake_by_id(self, request_id: str) -> dict | None:
        """Return the intake row with this request id as a dict (metadata decoded), or None."""
        row = self._conn.execute("SELECT * FROM intake WHERE id = ?", (request_id,)).fetchone()
        return _intake_row_to_dict(row) if row is not None else None

    def next_intake(self) -> dict | None:
        """
        Claim the oldest 'received' request and mark it 'processing', in one statement.

        The UPDATE ... RETURNING is a single SQLite statement, so two claimants cannot take the
        same row. Oldest means the smallest created_at; rowid breaks ties, so requests stored in
        the same microsecond keep their insertion order. Returns the claimed row, or None when no
        request is waiting.
        """
        rows = self._conn.execute(
            "UPDATE intake SET status = 'processing', updated_at = ? "
            "WHERE id = (SELECT id FROM intake WHERE status = 'received' "
            "            ORDER BY created_at, rowid LIMIT 1) "
            "RETURNING *",
            (_now(),),
        ).fetchall()
        self._conn.commit()
        return _intake_row_to_dict(rows[0]) if rows else None

    def reset_processing_intake(self) -> int:
        """
        Put every request left 'processing' back to 'received', and return how many were reset.

        A worker that died mid-request leaves its row 'processing', and next_intake() never claims
        such a row again. The intake worker calls this right after it takes the ingestion lock. The
        lock means no other worker is running, so every 'processing' row is an orphan of an earlier
        run. updated_at is refreshed, so the row shows when it was reset.
        """
        cur = self._conn.execute(
            "UPDATE intake SET status = 'received', updated_at = ? WHERE status = 'processing'",
            (_now(),),
        )
        self._conn.commit()
        return cur.rowcount

    def update_intake(self, request_id: str, **fields) -> None:
        """
        Set result columns of one intake row, and bump its updated_at.

        Only the names in _UPDATABLE_INTAKE_FIELDS may be set (status, spool_path,
        admission_reason, error, chunk_count, content_hash, pipeline_version). Any other name
        raises ValueError before the SQL is built, so a column name never comes from outside.
        """
        unknown = set(fields) - _UPDATABLE_INTAKE_FIELDS
        if unknown:
            raise ValueError(f"cannot update intake field(s): {sorted(unknown)}")
        assignments = [f"{name} = ?" for name in fields] + ["updated_at = ?"]
        self._conn.execute(
            f"UPDATE intake SET {', '.join(assignments)} WHERE id = ?",
            (*fields.values(), _now(), request_id),
        )
        self._conn.commit()

    def intake_for_source(self, source_id: str) -> list[dict]:
        """
        Return every intake row of one source, compared case-insensitively, as dicts.

        Used by the per-source removal tool: its dry-run counts the rows, and its apply reads their
        spool paths before the rows are deleted.
        """
        rows = self._conn.execute(
            "SELECT * FROM intake WHERE source_id = ? COLLATE NOCASE", (source_id,)
        ).fetchall()
        return [_intake_row_to_dict(row) for row in rows]

    def delete_intake_for_source(self, source_id: str) -> int:
        """
        Delete every intake row of one source, compared case-insensitively, and return the count.

        Used by the per-source removal tool. The rows are the source's request history and any
        work still waiting, so removing the source removes them too.
        """
        cur = self._conn.execute(
            "DELETE FROM intake WHERE source_id = ? COLLATE NOCASE", (source_id,)
        )
        self._conn.commit()
        return cur.rowcount

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
