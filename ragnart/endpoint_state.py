"""
The endpoint host's own state database (v0.3 session B1).

Each ingestion host (the operator's Mac, in B1) keeps its own state database. It holds one row per
item that the host has submitted to the hub, per endpoint source: what was sent, the request id the hub
returned, and the outcome the host last saw. It is NOT the hub's registry, its queue or its StateDB:
the hub never reads this file, and this module shares no code with ragnart/state.py.

The file is a plain SQLite database, opened with the stdlib sqlite3 module only. This module loads no
docling, qdrant_client or embedder (tests/test_ingestion_side_imports.py enforces that).

Opened read-only (a dry run), the file is never created or written. A missing file reads as an empty
state. Every write method refuses to run on a read-only instance, so a dry run cannot change the state
even by mistake.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# The schema version this code reads and writes. It is stored in PRAGMA user_version.
SCHEMA_VERSION = 1

# The statuses an item row may hold. The CHECK constraint in the schema enforces the same list.
STATUSES = ("pending", "completed", "ignored", "failed")

# One row per (source, item key). The key of a filesystem item is its absolute real path.
_SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    source_id     TEXT NOT NULL,
    key           TEXT NOT NULL,
    size          INTEGER,
    mtime         REAL,
    content_hash  TEXT,
    status        TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'ignored', 'failed')),
    request_id    TEXT,
    reason        TEXT,
    chunk_count   INTEGER,
    missing_since TEXT,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (source_id, key)
);
"""


def _now() -> str:
    """The current time as an ISO-8601 UTC string, to the second. Used for updated_at."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class EndpointState:
    """
    The endpoint's item rows, one connection per process.

    Args:
        path:      The database file. Its directory is created for a writable instance.
        read_only: Open for reading only. A missing file then reads as an empty state, and nothing is
                   created. Every write method raises RuntimeError on a read-only instance.
    """

    def __init__(self, path: str, read_only: bool = False) -> None:
        self.path = path
        self.read_only = read_only
        if read_only and os.path.exists(path):
            # 'mode=ro' makes SQLite refuse any write at the driver level, as a second guard.
            uri = Path(path).resolve().as_uri() + "?mode=ro"
            self._conn = sqlite3.connect(uri, uri=True)
            self._conn.row_factory = sqlite3.Row
            self._check_version()
        elif read_only:
            # Nothing exists yet, so there is nothing to read. An in-memory database with the
            # schema answers every read with "no rows", and the file is never created.
            self._conn = sqlite3.connect(":memory:")
            self._conn.row_factory = sqlite3.Row
            self._conn.executescript(_SCHEMA)
        else:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            self._conn = sqlite3.connect(path)
            self._conn.row_factory = sqlite3.Row
            # WAL lets a reader (a status check in another shell) run beside the writer.
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._initialise_schema()

    def close(self) -> None:
        """Close the connection."""
        self._conn.close()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _initialise_schema(self) -> None:
        """Create the schema on a new file, or check the version of an existing one."""
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        if version == 0:
            with self._conn:
                self._conn.executescript(_SCHEMA)
            self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        else:
            self._check_version()

    def _check_version(self) -> None:
        """Refuse a database whose version this code does not read."""
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        if version != SCHEMA_VERSION:
            raise RuntimeError(
                f"endpoint state {self.path} has schema version {version}; this code reads {SCHEMA_VERSION}"
            )

    def _refuse_if_read_only(self) -> None:
        """Raise before any write on a read-only instance."""
        if self.read_only:
            raise RuntimeError(f"endpoint state {self.path} is open read-only; nothing may be written")

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def rows(self, source_id: str) -> dict[str, dict]:
        """Return every item row of one source, keyed by item key, as plain dicts."""
        cursor = self._conn.execute("SELECT * FROM items WHERE source_id = ?", (source_id,))
        return {row["key"]: dict(row) for row in cursor.fetchall()}

    def count_rows(self) -> int:
        """The number of item rows across every source. A rebuild from the hub is allowed only when this is 0."""
        return self._conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def put(
        self,
        source_id: str,
        key: str,
        *,
        size: int,
        mtime: float,
        content_hash: str,
        status: str,
        request_id: str | None = None,
        reason: str | None = None,
        chunk_count: int | None = None,
        missing_since: str | None = None,
    ) -> None:
        """
        Insert or fully replace the row of one item.

        Used when the host has just submitted an item (status 'pending' with the request id), or
        when the hub refused it at the door (status 'ignored' with the reason).
        """
        self._refuse_if_read_only()
        if status not in STATUSES:
            raise ValueError(f"unknown status {status!r}")
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO items (source_id, key, size, mtime, content_hash, status, "
                "request_id, reason, chunk_count, missing_since, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (source_id, key, size, mtime, content_hash, status,
                 request_id, reason, chunk_count, missing_since, _now()),
            )

    def observe(self, source_id: str, key: str, size: int, mtime: float) -> None:
        """
        Store the size and mtime of an item that was seen on disk again.

        A reappearance also clears missing_since, so an item that came back within the grace period
        is not deleted.
        """
        self._refuse_if_read_only()
        with self._conn:
            self._conn.execute(
                "UPDATE items SET size = ?, mtime = ?, missing_since = NULL, updated_at = ? "
                "WHERE source_id = ? AND key = ?",
                (size, mtime, _now(), source_id, key),
            )

    def set_missing(self, source_id: str, key: str, since: str) -> None:
        """Record that an item was not found on disk, first seen missing at `since` (ISO-8601 UTC)."""
        self._refuse_if_read_only()
        with self._conn:
            self._conn.execute(
                "UPDATE items SET missing_since = ?, updated_at = ? WHERE source_id = ? AND key = ?",
                (since, _now(), source_id, key),
            )

    def set_outcome(
        self, source_id: str, key: str, status: str, reason: str | None, chunk_count: int | None
    ) -> None:
        """Record the outcome of an item's request, as the hub's status poll reported it."""
        self._refuse_if_read_only()
        if status not in STATUSES:
            raise ValueError(f"unknown status {status!r}")
        with self._conn:
            self._conn.execute(
                "UPDATE items SET status = ?, reason = ?, chunk_count = ?, updated_at = ? "
                "WHERE source_id = ? AND key = ?",
                (status, reason, chunk_count, _now(), source_id, key),
            )

    def delete(self, source_id: str, key: str) -> None:
        """Remove the row of one item. Removing a row that does not exist is a no-op."""
        self._refuse_if_read_only()
        with self._conn:
            self._conn.execute("DELETE FROM items WHERE source_id = ? AND key = ?", (source_id, key))
