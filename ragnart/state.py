"""
SQLite-backed ingestion queue for the RAG pipeline.

Tracks every NFS file through its ingestion lifecycle:
  pending → processing → completed
                       → pending  (auto-retry; retry_count < max_retries)
                       → ignored  (exhausted max_retries; skip_reason = last error)

'failed' is not used by the automatic path. It only appears if set manually via SQL.
discover_or_update() resets any 'failed' row to 'pending' as a safety net.

Uses a dedicated rag_state.db so chat_store.db stays unaffected.
All methods are synchronous, matching the ChatStore pattern in chat_store.py.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ingestion_queue (
    file_path       TEXT PRIMARY KEY,
    owner           TEXT NOT NULL,
    last_modified   REAL NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    error_msg       TEXT,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 5,
    started_at      TEXT,
    completed_at    TEXT,
    progress_detail TEXT,
    skip_reason     TEXT
);
"""

# Schema migrations, applied in __init__ after CREATE TABLE IF NOT EXISTS.
#
# The schema version lives in SQLite's own `PRAGMA user_version`. A database at
# version N has every migration with version <= N applied. Migrations run in
# ascending version order; each one is a list of SQL statements.
#
# Each statement runs on its own and any sqlite3.OperationalError is ignored.
# A statement whose work is already in the database (an ADD COLUMN for a column
# that exists, for example) is skipped, so a migration can be re-run safely after
# a crash part-way through. The live DB has user_version 0 but already has
# skip_reason, so version 1 must tolerate that.
_MIGRATIONS: list[tuple[int, list[str]]] = [
    (1, [
        # Add skip_reason column introduced with the 'ignored' status.
        "ALTER TABLE ingestion_queue ADD COLUMN skip_reason TEXT",
        # Raise max_retries from 3 to 5 for all existing rows that still have the old default.
        "UPDATE ingestion_queue SET max_retries = 5 WHERE max_retries < 5",
    ]),
    (2, [
        # Per-item columns. project_id is always set: 'global' means the item is
        # outside every ragnart project (design D2).
        "ALTER TABLE ingestion_queue ADD COLUMN project_id TEXT NOT NULL DEFAULT 'global'",
        "ALTER TABLE ingestion_queue ADD COLUMN source_type TEXT",
        "ALTER TABLE ingestion_queue ADD COLUMN file_type TEXT",
        # Where the item's bytes are read (design D7). NULL for OpenCode rows, which are read via the API.
        "ALTER TABLE ingestion_queue ADD COLUMN origin_path TEXT",
        "ALTER TABLE ingestion_queue ADD COLUMN size_bytes INTEGER",
        "ALTER TABLE ingestion_queue ADD COLUMN content_hash TEXT",
        "ALTER TABLE ingestion_queue ADD COLUMN hashed_at TEXT",
        "ALTER TABLE ingestion_queue ADD COLUMN first_seen_at TEXT",
        "ALTER TABLE ingestion_queue ADD COLUMN pipeline_version TEXT",
        "ALTER TABLE ingestion_queue ADD COLUMN chunk_count INTEGER",
        "ALTER TABLE ingestion_queue ADD COLUMN missing_since TEXT",
        # Session launch directory, recorded verbatim (design D4). NULL for files.
        "ALTER TABLE ingestion_queue ADD COLUMN launch_dir TEXT",
        # Rows that already completed take their first-seen time from completion time.
        "UPDATE ingestion_queue SET first_seen_at = completed_at "
        "WHERE first_seen_at IS NULL AND completed_at IS NOT NULL",
        # Project tables. Empty until ragnart projects exist.
        """
        CREATE TABLE IF NOT EXISTS projects (
            id              TEXT PRIMARY KEY,
            path            TEXT,
            name            TEXT,
            parent          TEXT,
            owner           TEXT,
            inherit_access  INTEGER NOT NULL DEFAULT 1,
            mode            TEXT
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
        """
        CREATE TABLE IF NOT EXISTS project_links (
            project_id         TEXT NOT NULL,
            linked_project_id  TEXT NOT NULL,
            PRIMARY KEY (project_id, linked_project_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS source_links (
            project_id   TEXT NOT NULL,
            source_path  TEXT NOT NULL,
            PRIMARY KEY (project_id, source_path)
        )
        """,
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
        # The reserved 'global' project (design D2): no path, no parent, never a parent.
        "INSERT OR IGNORE INTO projects (id, path, name, parent, owner, inherit_access, mode) "
        "VALUES ('global', NULL, 'Global', NULL, NULL, 0, NULL)",
    ]),
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class StateDB:
    """Ingestion queue CRUD for the RAG pipeline."""

    def __init__(self, db_path: str, read_only: bool = False) -> None:
        """
        Open the queue database.

        With ``read_only=True`` the file is opened through a ``mode=ro`` URI and
        nothing is written: no directory is created, WAL mode is not set, and the
        schema and migrations do not run. The read-only CLI uses this. A write on
        a read-only StateDB raises sqlite3.OperationalError. A missing file raises
        sqlite3.OperationalError too; it is never created.
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
        self._conn.executescript(_SCHEMA)
        self._apply_migrations()

    def _apply_migrations(self) -> None:
        """
        Bring the database up to the newest version in _MIGRATIONS.

        Reads PRAGMA user_version, runs every migration above it in order, and
        records each applied version. A database already at the newest version
        runs nothing.
        """
        current = self._conn.execute("PRAGMA user_version").fetchone()[0]
        for version, statements in _MIGRATIONS:
            if version <= current:
                continue
            for statement in statements:
                try:
                    self._conn.execute(statement)
                except sqlite3.OperationalError:
                    pass  # ALTER TABLE fails if column already exists — safe to ignore
            # user_version is set last, so a crash before this line re-runs the whole version.
            self._conn.execute(f"PRAGMA user_version = {int(version)}")
            self._conn.commit()
            logger.info("Applied ingestion-queue schema migration %d", version)

    # ------------------------------------------------------------------
    # Startup

    def crash_recovery(self) -> list[str]:
        """
        Reset any rows stuck in 'processing' back to 'pending'.

        Call once on daemon startup. Files left in 'processing' after a crash
        or OOM kill are re-queued rather than staying stuck indefinitely.

        Returns the list of file paths that were reset (for operator logging).
        """
        cur = self._conn.execute(
            "SELECT file_path FROM ingestion_queue WHERE status = 'processing'"
        )
        stuck = [row["file_path"] for row in cur.fetchall()]
        if stuck:
            self._conn.execute(
                "UPDATE ingestion_queue "
                "SET status = 'pending', started_at = NULL, progress_detail = NULL "
                "WHERE status = 'processing'"
            )
            self._conn.commit()
            logger.warning(
                "Crash recovery: reset %d stuck file(s) to pending: %s",
                len(stuck),
                stuck,
            )
        return stuck

    # ------------------------------------------------------------------
    # Discovery

    def discover_or_update(
        self,
        file_path: str,
        owner: str,
        last_modified: float,
        origin_path: str | None = None,
        source_type: str | None = None,
        file_type: str | None = None,
    ) -> None:
        """
        Register a newly discovered file or re-queue it for ingestion.

        The identity fields (origin_path, source_type, file_type) and first_seen_at
        are written only on insert: they are set once, at discovery. The update
        branches below leave them alone.

        - New file:    inserted as 'pending' with the identity fields and first_seen_at = now.
        - Modified:    disk mtime > stored mtime → reset to 'pending' (any status,
                       including 'ignored' — the file was replaced on disk).
        - Failed:      status = 'failed' (mtime unchanged) → reset to 'pending'
                       with retry_count = 0 so the daemon gives it a fresh set of
                       retries. Failed files have no Qdrant points (step 0 of the
                       worker loop deleted them before the failure), so no Qdrant
                       cleanup is needed.
        - Ignored:     status = 'ignored' (mtime unchanged) → no-op. The file is
                       permanently skipped until it changes on disk.
        - Everything else (pending / processing / completed, mtime unchanged): no-op.
        """
        cur = self._conn.execute(
            "SELECT last_modified, status FROM ingestion_queue WHERE file_path = ?",
            (file_path,),
        )
        row = cur.fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO ingestion_queue "
                "(file_path, owner, last_modified, status, origin_path, source_type, file_type, first_seen_at) "
                "VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)",
                (file_path, owner, last_modified, origin_path, source_type, file_type, _now()),
            )
        elif last_modified > row["last_modified"]:
            # File changed on disk — re-queue regardless of current status (including ignored).
            self._conn.execute(
                "UPDATE ingestion_queue "
                "SET status = 'pending', last_modified = ?, "
                "    error_msg = NULL, retry_count = 0, skip_reason = NULL, "
                "    started_at = NULL, completed_at = NULL, progress_detail = NULL "
                "WHERE file_path = ?",
                (last_modified, file_path),
            )
        elif row["status"] == "failed":
            # Permanently failed but file unchanged — give it a fresh set of retries.
            self._conn.execute(
                "UPDATE ingestion_queue "
                "SET status = 'pending', last_modified = ?, "
                "    error_msg = NULL, retry_count = 0, "
                "    started_at = NULL, completed_at = NULL, progress_detail = NULL "
                "WHERE file_path = ?",
                (last_modified, file_path),
            )
        # ignored (mtime unchanged), pending, processing, completed → no-op
        self._conn.commit()

    def find_deleted(
        self,
        existing_paths: set[str],
        owners: list[str] | None = None,
    ) -> tuple[list[str], list[str]]:
        """
        Identify files no longer present in the scan without modifying SQLite.

        Args:
            existing_paths: every path the scan saw.
            owners: if given, only rows owned by one of these owners are considered.
                    A sync run for one user must not treat another user's rows as
                    deleted. None considers every row.

        Returns:
            (completed_gone, all_gone):
              - completed_gone: status='completed' paths that are gone; these have
                Qdrant points and must be cleaned up before purge_deleted() is called.
              - all_gone: ALL gone paths (any status) that should be removed from SQLite.

        Call purge_deleted(all_gone) after Qdrant cleanup so that a crash/kill between
        the two calls leaves SQLite intact — the next sync retries automatically.
        """
        if owners is None:
            cur = self._conn.execute(
                "SELECT file_path, status FROM ingestion_queue"
            )
        elif not owners:
            # No owners means no rows are in scope, so nothing can be gone.
            return [], []
        else:
            # One placeholder per owner keeps the values parameterised.
            placeholders = ",".join("?" * len(owners))
            cur = self._conn.execute(
                f"SELECT file_path, status FROM ingestion_queue WHERE owner IN ({placeholders})",
                tuple(owners),
            )
        all_rows = [(row["file_path"], row["status"]) for row in cur.fetchall()]
        gone = [(p, s) for p, s in all_rows if p not in existing_paths]
        if not gone:
            return [], []
        all_gone = [p for p, _ in gone]
        completed_gone = [p for p, s in gone if s == "completed"]
        return completed_gone, all_gone

    def paths_with_prefix(self, prefix: str) -> set[str]:
        """
        Return every queued file_path that starts with prefix (any status).

        Used by rag_sync_nfs.py to keep a source's rows out of find_deleted()
        when that source could not be scanned (e.g. opencode:// when the
        OpenCode API is unreachable), so its Qdrant points are preserved.
        """
        cur = self._conn.execute(
            "SELECT file_path FROM ingestion_queue WHERE substr(file_path, 1, ?) = ?",
            (len(prefix), prefix),
        )
        return {row["file_path"] for row in cur.fetchall()}

    def count_with_prefix(self, prefix: str) -> int:
        """
        Return how many queued rows have a file_path starting with prefix (any status).

        Read-only. The scanner uses it with "<root>/" to tell whether the queue still
        holds rows under an NFS root that now yields no files.
        """
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM ingestion_queue WHERE substr(file_path, 1, ?) = ?",
            (len(prefix), prefix),
        ).fetchone()
        return int(row["n"])

    def count_origin_with_prefix(self, prefix: str) -> int:
        """
        Return how many queued rows have an origin_path starting with prefix (any status).

        Read-only. The scanner uses it with "<root>/" for claude_chats roots: a claude-session
        key is not a path, so its real location is only in origin_path.
        """
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM ingestion_queue WHERE substr(origin_path, 1, ?) = ?",
            (len(prefix), prefix),
        ).fetchone()
        return int(row["n"])

    def purge_deleted(self, paths: list[str]) -> None:
        """
        Remove the given paths from SQLite. Call only after Qdrant cleanup is complete.

        Separating this from find_deleted() ensures that a kill/crash during Qdrant
        deletion leaves the SQLite rows intact; the next scan retries automatically.
        """
        if not paths:
            return
        self._conn.executemany(
            "DELETE FROM ingestion_queue WHERE file_path = ?",
            [(p,) for p in paths],
        )
        self._conn.commit()
        logger.info("Removed %d deleted file(s) from queue", len(paths))

    def handle_deleted(self, existing_paths: set[str]) -> list[str]:
        """
        Remove rows for files that are no longer present in the scan.

        Deletes ALL rows (any status) whose file_path is absent from
        existing_paths. Returns only the paths that were 'completed' so
        the caller can delete the corresponding Qdrant points.

        Deprecated: prefer find_deleted() + purge_deleted() so Qdrant cleanup
        happens between the two calls (crash-safe ordering).
        """
        completed_gone, all_gone = self.find_deleted(existing_paths)
        self.purge_deleted(all_gone)
        return completed_gone

    # ------------------------------------------------------------------
    # Worker state transitions

    def fetch_pending(self, limit: int = 10, owner: str | None = None) -> list[str]:
        """Return up to `limit` pending file paths, oldest first. Optionally filter by owner."""
        if owner:
            cur = self._conn.execute(
                "SELECT file_path FROM ingestion_queue "
                "WHERE status = 'pending' AND owner = ? ORDER BY rowid LIMIT ?",
                (owner, limit),
            )
        else:
            cur = self._conn.execute(
                "SELECT file_path FROM ingestion_queue "
                "WHERE status = 'pending' ORDER BY rowid LIMIT ?",
                (limit,),
            )
        return [row["file_path"] for row in cur.fetchall()]

    def claim_next_pending(self) -> dict | None:
        """
        Claim the oldest pending row: pending → processing, in one call.

        Returns {"file_path", "owner", "origin_path", "last_modified"} for the claimed
        row, or None when no row is pending. The continuous worker pool in utils/rag_ingest_daemon.py calls
        this once each time a worker becomes free.

        Why no explicit SQL transaction is needed: the daemon's workers are asyncio
        coroutines on a single OS thread, and this method contains no `await`. Nothing
        else on the event loop can run between the SELECT and the UPDATE, so no other
        worker can claim the same row in between. The `status = 'pending'` guard on the
        UPDATE keeps it from touching a row that has already left 'pending'.
        """
        row = self._conn.execute(
            "SELECT file_path, owner, origin_path, last_modified FROM ingestion_queue "
            "WHERE status = 'pending' ORDER BY rowid LIMIT 1"
        ).fetchone()
        if row is None:
            return None

        claimed = dict(row)
        self._conn.execute(
            "UPDATE ingestion_queue "
            "SET status = 'processing', started_at = ?, progress_detail = 'starting' "
            "WHERE file_path = ? AND status = 'pending'",
            (_now(), claimed["file_path"]),
        )
        self._conn.commit()
        return claimed

    def mark_processing(self, file_path: str) -> None:
        """Transition pending → processing; record start timestamp."""
        self._conn.execute(
            "UPDATE ingestion_queue "
            "SET status = 'processing', started_at = ?, progress_detail = 'starting' "
            "WHERE file_path = ?",
            (_now(), file_path),
        )
        self._conn.commit()

    def set_progress(self, file_path: str, detail: str) -> None:
        """Update progress_detail while a file is being processed (mid-flight)."""
        self._conn.execute(
            "UPDATE ingestion_queue SET progress_detail = ? WHERE file_path = ?",
            (detail, file_path),
        )
        self._conn.commit()

    def mark_completed(
        self,
        file_path: str,
        chunk_count: int | None = None,
        source_type: str | None = None,
        file_type: str | None = None,
        pipeline_version: str | None = None,
        size_bytes: int | None = None,
        content_hash: str | None = None,
        hashed_at: str | None = None,
        completed_at: str | None = None,
        launch_dir: str | None = None,
    ) -> None:
        """
        Transition processing → completed; record completion timestamp.

        Every extra argument is optional. An argument that is None leaves its column
        unchanged, so mark_completed(file_path) alone only sets the status, as before.
        completed_at, when given, is stored as-is. Pass the same ISO string that went
        into the Qdrant payload's ingested_at, so the two agree exactly.
        """
        # Columns to write when the caller supplied a value. The names are fixed
        # literals here, never caller input, so building the SQL from them is safe.
        optional_columns = {
            "chunk_count": chunk_count,
            "source_type": source_type,
            "file_type": file_type,
            "pipeline_version": pipeline_version,
            "size_bytes": size_bytes,
            "content_hash": content_hash,
            "hashed_at": hashed_at,
            "launch_dir": launch_dir,
        }
        set_clauses = [
            "status = 'completed'",
            "completed_at = ?",
            "progress_detail = NULL",
            "error_msg = NULL",
        ]
        params: list = [completed_at or _now()]
        for column, value in optional_columns.items():
            if value is not None:
                set_clauses.append(f"{column} = ?")
                params.append(value)
        params.append(file_path)
        self._conn.execute(
            f"UPDATE ingestion_queue SET {', '.join(set_clauses)} WHERE file_path = ?",
            params,
        )
        self._conn.commit()

    def mark_failed(self, file_path: str, error: str) -> None:
        """
        Record a failure and decide whether to auto-retry.

        - retry_count < max_retries  → status = 'pending'   (will be retried)
        - retry_count >= max_retries → status = 'ignored'   (skip_reason = last error)

        'ignored' files are never re-queued by discover_or_update() unless the file
        changes on disk. Use rag_status.py --ignored to review them.
        """
        cur = self._conn.execute(
            "SELECT retry_count, max_retries FROM ingestion_queue WHERE file_path = ?",
            (file_path,),
        )
        row = cur.fetchone()
        if row is None:
            logger.error("mark_failed called for unknown file: %s", file_path)
            return

        new_count = row["retry_count"] + 1
        if new_count < row["max_retries"]:
            self._conn.execute(
                "UPDATE ingestion_queue "
                "SET status = 'pending', retry_count = ?, error_msg = ?, progress_detail = NULL "
                "WHERE file_path = ?",
                (new_count, error[:2000], file_path),
            )
            logger.warning(
                "Retry %d/%d queued: %s — %s",
                new_count, row["max_retries"], file_path, error[:120],
            )
        else:
            self._conn.execute(
                "UPDATE ingestion_queue "
                "SET status = 'ignored', retry_count = ?, skip_reason = ?, "
                "    error_msg = NULL, progress_detail = NULL "
                "WHERE file_path = ?",
                (new_count, error[:2000], file_path),
            )
            logger.error(
                "Ignored after %d retries: %s — %s",
                new_count, file_path, error[:200],
            )
        self._conn.commit()

    def mark_ignored(self, file_path: str, reason: str) -> None:
        """
        Permanently skip a file — never re-queued by discover_or_update() unless
        the file is replaced on disk (mtime change).

        Use for files that fundamentally cannot be parsed regardless of retries:
        IRM/DRM-encrypted, corrupted, unsupported proprietary format.

        The file must have no Qdrant points at the time of this call (i.e. it
        should currently be 'failed' or 'pending', not 'completed'). mark_ignored()
        does not touch Qdrant — the caller is responsible for any cleanup.
        """
        self._conn.execute(
            "UPDATE ingestion_queue "
            "SET status = 'ignored', skip_reason = ?, "
            "    error_msg = NULL, progress_detail = NULL "
            "WHERE file_path = ?",
            (reason[:2000], file_path),
        )
        self._conn.commit()
        logger.info("Marked as ignored: %s — %s", file_path, reason)

    def reset_to_pending(self, file_path: str) -> None:
        """
        Requeue one row for a fresh ingestion attempt.

        Sets status to 'pending' with retry_count 0, and clears the error, skip reason
        and timing fields, as discover_or_update() does when a file changes on disk.
        Used by utils/rag_purge_rescan.py. The caller removes the row's Qdrant points
        first, so no stale points survive the requeue.
        """
        self._conn.execute(
            "UPDATE ingestion_queue "
            "SET status = 'pending', retry_count = 0, skip_reason = NULL, error_msg = NULL, "
            "    started_at = NULL, completed_at = NULL, progress_detail = NULL "
            "WHERE file_path = ?",
            (file_path,),
        )
        self._conn.commit()
        logger.info("Requeued for re-parse: %s", file_path)

    # ------------------------------------------------------------------
    # Reporting

    def get_counts(self) -> dict[str, int]:
        """Return row counts grouped by status."""
        cur = self._conn.execute(
            "SELECT status, COUNT(*) AS n FROM ingestion_queue GROUP BY status"
        )
        counts = {row["status"]: row["n"] for row in cur.fetchall()}
        return {
            "pending":    counts.get("pending", 0),
            "processing": counts.get("processing", 0),
            "completed":  counts.get("completed", 0),
            "failed":     counts.get("failed", 0),
            "ignored":    counts.get("ignored", 0),
            "total":      sum(counts.values()),
        }

    def get_failed(self, owner: str | None = None) -> list[dict]:
        """Return all permanently failed files with their error messages."""
        if owner:
            cur = self._conn.execute(
                "SELECT file_path, owner, retry_count, error_msg "
                "FROM ingestion_queue WHERE status = 'failed' AND owner = ? "
                "ORDER BY file_path",
                (owner,),
            )
        else:
            cur = self._conn.execute(
                "SELECT file_path, owner, retry_count, error_msg "
                "FROM ingestion_queue WHERE status = 'failed' ORDER BY file_path"
            )
        return [dict(row) for row in cur.fetchall()]

    def list_rows(self, status: str | None = None) -> list[dict]:
        """
        Return file_path and status for every queue row, sorted by file_path.

        With ``status`` set, only rows in that status are returned.
        """
        if status:
            cur = self._conn.execute(
                "SELECT file_path, status FROM ingestion_queue "
                "WHERE status = ? ORDER BY file_path",
                (status,),
            )
        else:
            cur = self._conn.execute(
                "SELECT file_path, status FROM ingestion_queue ORDER BY file_path"
            )
        return [dict(row) for row in cur.fetchall()]

    def get_ignored(self, owner: str | None = None) -> list[dict]:
        """Return all ignored files with their retry count and skip reason (last error)."""
        if owner:
            cur = self._conn.execute(
                "SELECT file_path, owner, retry_count, skip_reason "
                "FROM ingestion_queue WHERE status = 'ignored' AND owner = ? "
                "ORDER BY file_path",
                (owner,),
            )
        else:
            cur = self._conn.execute(
                "SELECT file_path, owner, retry_count, skip_reason "
                "FROM ingestion_queue WHERE status = 'ignored' ORDER BY file_path"
            )
        return [dict(row) for row in cur.fetchall()]

    def close(self) -> None:
        self._conn.close()
