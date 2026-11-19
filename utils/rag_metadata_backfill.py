#!/usr/bin/env python3
"""
One-time v0.2 metadata backfill: gives every existing item its session-B metadata.

Items ingested before v0.2 have points in the Qdrant `documents` collection and
rows in `rag_state.db` that lack `project_id`, `source_type`, `file_type`,
`modified_at`, `ingested_at`, `content_hash`, `origin_path` and `launch_dir`.
This script adds them in place. It never re-embeds: it changes payloads (Qdrant
`set_payload`) and SQLite columns, and leaves the vectors alone.

Usage (run from project root):
    python utils/rag_metadata_backfill.py                          # dry-run (default)
    python utils/rag_metadata_backfill.py --dry-run --prefix /mnt/nfs/Florian/
    python utils/rag_metadata_backfill.py --apply [--prefix PATH]
                                          [--assumed-mb-per-sec 50] [--checkpoint-file PATH]

Row kinds (see classify()):
    nas                  A NAS file. Hashed; payload set in place under its own key.
    claude               A Claude Code transcript under a claude_chats root. Read once
                         for its MD5 and first top-level cwd; re-keyed from its .jsonl
                         path to its claude-session:// key, in Qdrant and SQLite.
    claude_journal_skip  A workflow journal.jsonl. An orchestration log, not a
                         conversation: left completely untouched.
    opencode             An opencode:// session. Its launch directory is read from the
                         OpenCode API. No content hash (it is read via the API).

Crash safety, per row, in this order:
    1. Read the file (hash, first cwd) or the OpenCode API. Nothing is written yet.
    2. Qdrant set_payload, filtered on the OLD key, wait=True.
    3. Count the points under the NEW key.
    4. ONE SQLite UPDATE (re-key plus metadata), committed.

Idempotency marker: `pipeline_version IS NULL`. A row counts as migrated only once
its step-4 UPDATE has committed. A crash before step 4 leaves the row NULL, so a
re-run repeats steps 1-4. A re-run after step 2 is harmless: the old-key filter then
matches no points, and step 3 still counts the new key.

The checkpoint file gets one line every 100 rows, for an operator to watch. Nothing
reads it back; correctness never depends on it.

A per-row failure (file vanished, read error, Qdrant or OpenCode error) is logged with
its key and reason and counted. That row keeps `pipeline_version` NULL, so a re-run
retries it. The script exits non-zero if any row failed.

Collisions: one session uuid can exist as two transcript files (copies in two project
directories). Its old-key row can then find a row already under the same claude-session://
key. That is resolved, not failed (see _resolve_collision()): the newer file wins, every
Qdrant point under the key is deleted, and the key's row is re-queued as pending, so the
next ingestion run submits the winner.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from qdrant_client.models import FieldCondition, Filter, FilterSelector, MatchValue

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ragnart.collection import DOCUMENTS_COLLECTION, get_client  # noqa: E402
from ragnart.hashing import hash_file  # noqa: E402
from ragnart.lock import (  # noqa: E402
    acquire_or_exit,
    lock_path_from_config,
    nas_ingest_lock_path_from_config,
)
from ragnart.opencode_client import get_json, load_password  # noqa: E402
from ragnart.scanner import claude_jsonl_key  # noqa: E402
from ragnart.schema import (  # noqa: E402
    FIELD_CONTENT_HASH,
    FIELD_FILE_TYPE,
    FIELD_INGESTED_AT,
    FIELD_MODIFIED_AT,
    FIELD_PROJECT_ID,
    FIELD_SOURCE_PATH,
    FIELD_SOURCE_TYPE,
    GLOBAL_PROJECT_ID,
)
from ragnart.state import StateDB  # noqa: E402

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "ragnart.yaml"

# Value written to pipeline_version by this script. NULL means "not migrated yet".
PIPELINE_VERSION = "p1"

# Rows between two checkpoint lines.
CHECKPOINT_EVERY = 100

# Row kinds returned by classify().
KIND_NAS = "nas"
KIND_CLAUDE = "claude"
KIND_JOURNAL_SKIP = "claude_journal_skip"
KIND_OPENCODE = "opencode"

# Values written to source_type / file_type. These are data values, not field names.
SOURCE_TYPE_FILE = "file"
SOURCE_TYPE_CLAUDE = "claude_session"
SOURCE_TYPE_OPENCODE = "opencode_session"
FILE_TYPE_CLAUDE = "claude_chat"
FILE_TYPE_OPENCODE = "opencode"

OPENCODE_PREFIX = "opencode://"


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify(file_path: str, claude_roots: list[str]) -> str:
    """
    Return the row kind for one queue key.

    - an `opencode://` key is an OpenCode session;
    - a key under a claude_chats root is a Claude Code transcript, unless
      claude_jsonl_key() excludes it (a workflow journal), in which case it is skipped;
    - anything else is a NAS file.

    The root match requires a path separator after the root, so a root
    `/a/projects` does not claim `/a/projects2/...`.
    """
    if file_path.startswith(OPENCODE_PREFIX):
        return KIND_OPENCODE

    under_claude_root = any(file_path.startswith(root.rstrip("/") + "/") for root in claude_roots)
    if under_claude_root:
        if claude_jsonl_key(Path(file_path)) is None:
            return KIND_JOURNAL_SKIP
        return KIND_CLAUDE

    return KIND_NAS


# ---------------------------------------------------------------------------
# Candidate rows
# ---------------------------------------------------------------------------

def candidates(state_db: StateDB, prefix: str | None = None) -> list[dict]:
    """
    Return completed rows that have not been migrated yet, ordered by file_path.

    Ordering by file_path keeps a re-run deterministic. With `prefix`, only rows whose
    file_path starts with it are returned. This reads `state_db._conn` directly, the way
    the other utils/rag_*.py scripts do for ad-hoc reads.
    """
    sql = (
        "SELECT file_path, last_modified, completed_at FROM ingestion_queue "
        "WHERE status = 'completed' AND pipeline_version IS NULL"
    )
    params: list[Any] = []
    if prefix:
        sql += " AND substr(file_path, 1, ?) = ?"
        params += [len(prefix), prefix]
    sql += " ORDER BY file_path"
    return [dict(row) for row in state_db._conn.execute(sql, params).fetchall()]


# ---------------------------------------------------------------------------
# Reading source files
# ---------------------------------------------------------------------------

def _cwd_from_line(line: bytes) -> str | None:
    """
    Return the top-level `cwd` of one transcript line, or None.

    A line that is not JSON (a partial or corrupt line) yields None, and the scan goes on.
    An empty cwd also yields None: the caller wants the first NON-EMPTY cwd.
    """
    try:
        entry = json.loads(line)
    except ValueError:  # JSONDecodeError and UnicodeDecodeError are both ValueError
        return None
    if not isinstance(entry, dict):
        return None
    cwd = entry.get("cwd")
    if isinstance(cwd, str) and cwd:
        return cwd
    return None


def _hash_and_first_cwd(path: str) -> tuple[int, str, str | None]:
    """
    Read a Claude Code transcript once: return (size_bytes, md5_hexdigest, first_cwd).

    The MD5 must equal what ingestion computes with hash_file(), so the bytes are the
    same. The file is read line by line, so memory stays at about one line. The line
    is only parsed while no cwd has been found, and only when it contains the text
    "cwd", which keeps the scan cheap on long transcripts.
    """
    digest = hashlib.md5()
    size_bytes = 0
    first_cwd: str | None = None
    with open(path, "rb") as handle:
        for line in handle:
            digest.update(line)
            size_bytes += len(line)
            if first_cwd is None and b'"cwd"' in line:
                first_cwd = _cwd_from_line(line)
    return size_bytes, digest.hexdigest(), first_cwd


def _opencode_launch_dir(file_path: str, oc_api_url: str, oc_password: str) -> str | None:
    """
    Return the session's launch directory from GET /api/session/<id>, verbatim.

    The response is wrapped in "data". The directory is returned exactly as the server
    gives it: never normalised, never stripped. An empty value becomes None (NULL).
    """
    session_id = file_path[len(OPENCODE_PREFIX):]
    session = get_json(oc_api_url, f"/api/session/{session_id}", oc_password).get("data", {})
    directory = (session.get("location") or {}).get("directory")
    return directory or None


# ---------------------------------------------------------------------------
# Qdrant and SQLite helpers
# ---------------------------------------------------------------------------

def _source_filter(source_path: str) -> Filter:
    """Filter matching every point stored for one source_path value."""
    return Filter(
        must=[FieldCondition(key=FIELD_SOURCE_PATH, match=MatchValue(value=source_path))]
    )


def _set_payload(qdrant_client: Any, old_key: str, payload: dict) -> None:
    """
    Merge `payload` into every point whose source_path is `old_key`.

    The filter always uses the key the points currently carry. wait=True makes sure
    Qdrant has applied the change before the SQLite side is written.
    """
    qdrant_client.set_payload(
        collection_name=DOCUMENTS_COLLECTION,
        payload=payload,
        points=_source_filter(old_key),
        wait=True,
    )


def _count_points(qdrant_client: Any, source_path: str) -> int:
    """Exact number of points stored for source_path (one point is one child chunk)."""
    result = qdrant_client.count(
        collection_name=DOCUMENTS_COLLECTION,
        count_filter=_source_filter(source_path),
        exact=True,
    )
    return int(result.count)


def _commit_row(state_db: StateDB, sql: str, params: tuple) -> None:
    """Run one UPDATE on the queue and commit it. This is the single step that migrates a row."""
    state_db._conn.execute(sql, params)
    state_db._conn.commit()


def _now() -> str:
    """Current UTC time as ISO-8601, the form used for hashed_at."""
    return datetime.now(timezone.utc).isoformat()


def _iso_utc(epoch_seconds: float) -> str:
    """Epoch seconds as ISO-8601 in UTC, the form used for modified_at."""
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Per-row migration
# ---------------------------------------------------------------------------

def _migrate_opencode(row, qdrant_client, state_db, oc_api_url, oc_password) -> None:
    """OpenCode session: read the launch directory, set payload in place, record the row."""
    file_path = row["file_path"]
    # Read first: an API error stops the row before anything is written.
    launch_dir = _opencode_launch_dir(file_path, oc_api_url, oc_password)
    _set_payload(qdrant_client, file_path, {
        FIELD_PROJECT_ID: GLOBAL_PROJECT_ID,
        FIELD_SOURCE_TYPE: SOURCE_TYPE_OPENCODE,
        FIELD_FILE_TYPE: FILE_TYPE_OPENCODE,
        FIELD_MODIFIED_AT: _iso_utc(row["last_modified"]),
        FIELD_INGESTED_AT: row["completed_at"],
    })
    # No content_hash and no origin_path: an OpenCode session is read via the API.
    _commit_row(
        state_db,
        "UPDATE ingestion_queue SET source_type = ?, file_type = ?, launch_dir = ?, "
        "pipeline_version = ? WHERE file_path = ?",
        (SOURCE_TYPE_OPENCODE, FILE_TYPE_OPENCODE, launch_dir, PIPELINE_VERSION, file_path),
    )


def _migrate_nas(row, qdrant_client, state_db, now_iso) -> None:
    """NAS file: hash it, set payload under its unchanged key, record the row."""
    file_path = row["file_path"]
    # Read first. A vanished file raises here, before any Qdrant write.
    size_bytes, content_hash = hash_file(file_path)
    file_type = Path(file_path).suffix.lstrip(".").lower()
    _set_payload(qdrant_client, file_path, {
        FIELD_PROJECT_ID: GLOBAL_PROJECT_ID,
        FIELD_SOURCE_TYPE: SOURCE_TYPE_FILE,
        FIELD_FILE_TYPE: file_type,
        FIELD_MODIFIED_AT: _iso_utc(row["last_modified"]),
        FIELD_INGESTED_AT: row["completed_at"],
        FIELD_CONTENT_HASH: content_hash,
    })
    chunk_count = _count_points(qdrant_client, file_path)
    # origin_path is the file itself. launch_dir stays NULL: a NAS file has no launch directory.
    _commit_row(
        state_db,
        "UPDATE ingestion_queue SET origin_path = ?, source_type = ?, file_type = ?, "
        "size_bytes = ?, content_hash = ?, hashed_at = ?, chunk_count = ?, pipeline_version = ? "
        "WHERE file_path = ?",
        (file_path, SOURCE_TYPE_FILE, file_type, size_bytes, content_hash, now_iso,
         chunk_count, PIPELINE_VERSION, file_path),
    )


def _existing_row(state_db: StateDB, key: str) -> dict | None:
    """The queue row stored under `key` (only file_path and origin_path), or None."""
    row = state_db._conn.execute(
        "SELECT file_path, origin_path FROM ingestion_queue WHERE file_path = ?",
        (key,),
    ).fetchone()
    return dict(row) if row is not None else None


def _pick_winner(old_key: str, existing_origin: str | None) -> tuple[str, str | None]:
    """
    Return (winner, loser) between the old row's transcript and the existing row's origin.

    The newer file by mtime wins. An exact mtime tie goes to the lexicographically greatest
    path, the same rule scan_claude_chats uses. The two rules must agree: otherwise the next
    scan would see a different origin_path than the one stored here, and re-queue the row.
    A missing file always loses. If both are missing, nothing can be read, so this raises.
    """
    old_exists = os.path.isfile(old_key)
    existing_exists = existing_origin is not None and os.path.isfile(existing_origin)
    if not old_exists and not existing_exists:
        raise FileNotFoundError(
            f"both collision copies are missing: {old_key} and {existing_origin}"
        )
    if not existing_exists:
        return old_key, existing_origin
    if not old_exists:
        return existing_origin, old_key
    # Both exist: the greatest (mtime, path) pair is the winner.
    ranked = sorted([old_key, existing_origin], key=lambda path: (os.path.getmtime(path), path))
    return ranked[-1], ranked[0]


def _resolve_collision(old_key: str, new_key: str, existing: dict, qdrant_client, state_db) -> None:
    """
    Resolve a row whose old key collides with an existing row under the new key.

    Both rows describe the same session. The winner is the newer copy (see _pick_winner).
    The loser's copy is a duplicate location and is not indexed.

    Steps, in this order:
      1. Delete every Qdrant point under new_key, and any under old_key. The winner is
         re-ingested from scratch, so no point from either copy may survive. A point
         that an earlier attempt already moved to new_key is removed here too.
      2. One SQLite transaction: delete the old row, and reset the new-key row to 'pending'
         with origin_path = winner and last_modified = winner's mtime.

    Crash safety: the Qdrant deletes come first, then the single SQLite commit. A crash
    before the commit leaves both rows as they were. A re-run finds the old row again,
    repeats this function, and its delete matches nothing already gone. Nothing is lost,
    and the result is the same.
    """
    winner, loser = _pick_winner(old_key, existing["origin_path"])
    winner_mtime = os.path.getmtime(winner)

    for key in (new_key, old_key):
        qdrant_client.delete(
            collection_name=DOCUMENTS_COLLECTION,
            points_selector=FilterSelector(filter=_source_filter(key)),
            wait=True,
        )

    # One transaction for both statements. sqlite3 opens it implicitly on the first DELETE,
    # and the commit makes both changes durable together. A failure rolls both back.
    conn = state_db._conn
    try:
        conn.execute("DELETE FROM ingestion_queue WHERE file_path = ?", (old_key,))
        conn.execute(
            "UPDATE ingestion_queue SET origin_path = ?, last_modified = ?, status = 'pending', "
            "pipeline_version = NULL, chunk_count = NULL, started_at = NULL, completed_at = NULL, "
            "error_msg = NULL, retry_count = 0 WHERE file_path = ?",
            (winner, winner_mtime, new_key),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    logger.info(
        "Collision resolved for %s (old key %s): winner=%s loser=%s",
        new_key, old_key, winner, loser,
    )


def _migrate_claude(row, qdrant_client, state_db, now_iso) -> bool:
    """
    Claude Code transcript: hash and read its cwd, re-key in Qdrant and SQLite.

    Returns True when the row collided with an existing row under its new key and was
    resolved by _resolve_collision(). Returns False for an ordinary re-key.
    """
    old_key = row["file_path"]
    new_key = claude_jsonl_key(Path(old_key))

    # A row already under the new key means the same session was queued from another copy.
    # Resolve it before any Qdrant write for this row, so the re-key below cannot fail on it.
    existing = _existing_row(state_db, new_key)
    if existing is not None:
        _resolve_collision(old_key, new_key, existing, qdrant_client, state_db)
        return True

    # Read before the rename. A vanished transcript raises here, before any Qdrant write.
    size_bytes, content_hash, launch_dir = _hash_and_first_cwd(old_key)
    # The filter uses the OLD key, because the points still carry it. The payload
    # carries the NEW key, which is the re-key.
    _set_payload(qdrant_client, old_key, {
        FIELD_SOURCE_PATH: new_key,
        FIELD_PROJECT_ID: GLOBAL_PROJECT_ID,
        FIELD_SOURCE_TYPE: SOURCE_TYPE_CLAUDE,
        FIELD_FILE_TYPE: FILE_TYPE_CLAUDE,
        FIELD_MODIFIED_AT: _iso_utc(row["last_modified"]),
        FIELD_INGESTED_AT: row["completed_at"],
        FIELD_CONTENT_HASH: content_hash,
    })
    # Count under the new key, so the number is right whether or not set_payload has
    # already run in an earlier attempt.
    chunk_count = _count_points(qdrant_client, new_key)
    # One UPDATE moves the row to its new key and writes the metadata. The old .jsonl
    # path is kept in origin_path, where the bytes are read. The existence check above
    # guarantees no row sits under new_key, so this UPDATE cannot collide.
    _commit_row(
        state_db,
        "UPDATE ingestion_queue SET file_path = ?, origin_path = ?, source_type = ?, "
        "file_type = ?, size_bytes = ?, content_hash = ?, hashed_at = ?, launch_dir = ?, "
        "chunk_count = ?, pipeline_version = ? WHERE file_path = ?",
        (new_key, old_key, SOURCE_TYPE_CLAUDE, FILE_TYPE_CLAUDE, size_bytes, content_hash,
         now_iso, launch_dir, chunk_count, PIPELINE_VERSION, old_key),
    )
    return False


def process_one(
    row: dict,
    kind: str,
    qdrant_client: Any,
    state_db: StateDB,
    now_iso: str,
    oc_api_url: str | None = None,
    oc_password: str | None = None,
) -> bool:
    """
    Migrate one row of the given kind. Raises on any failure; the caller records it.

    A journal row returns at once and writes nothing. A completed row must have
    completed_at, because it is copied verbatim into ingested_at.

    Returns True only when a claude row collided with an existing row and was resolved
    (see _resolve_collision()). Every other outcome returns False.
    """
    if kind == KIND_JOURNAL_SKIP:
        return False
    if row["completed_at"] is None:
        raise ValueError("completed row has no completed_at; cannot set ingested_at")

    if kind == KIND_OPENCODE:
        if oc_api_url is None or oc_password is None:
            raise ValueError("OpenCode row needs oc_api_url and oc_password")
        _migrate_opencode(row, qdrant_client, state_db, oc_api_url, oc_password)
        return False
    if kind == KIND_NAS:
        _migrate_nas(row, qdrant_client, state_db, now_iso)
        return False
    if kind == KIND_CLAUDE:
        return _migrate_claude(row, qdrant_client, state_db, now_iso)
    raise ValueError(f"unknown row kind {kind!r}")


# ---------------------------------------------------------------------------
# Whole-run driver
# ---------------------------------------------------------------------------

def _write_checkpoint(checkpoint_file: str, last_path: str, done: int, total: int) -> None:
    """Append one progress line. For the operator only; nothing reads it back."""
    stamp = datetime.now(timezone.utc).isoformat()
    with open(checkpoint_file, "a", encoding="utf-8") as handle:
        handle.write(f"{stamp}\t{done}/{total}\t{last_path}\n")


def _dry_run_summary(classified: list[tuple[dict, str]], assumed_mb_per_sec: float) -> dict:
    """
    Tally the plan without touching Qdrant, the OpenCode API, or file contents.

    Only the size of each NAS and Claude file is read, with os.path.getsize(), to
    estimate the hashing volume. A file that cannot be stat'ed is listed as unreadable.
    """
    total_bytes = 0
    unreadable: list[tuple[str, str]] = []
    for row, kind in classified:
        if kind not in (KIND_NAS, KIND_CLAUDE):
            continue
        try:
            total_bytes += os.path.getsize(row["file_path"])
        except OSError as exc:
            unreadable.append((row["file_path"], str(exc)))
    return {
        "dry_run": True,
        "total": len(classified),
        "kinds": Counter(kind for _, kind in classified),
        "bytes": total_bytes,
        "est_seconds": total_bytes / (assumed_mb_per_sec * 1_000_000),
        "unreadable": unreadable,
    }


def run_backfill(
    config: dict,
    state_db: StateDB,
    qdrant_client: Any,
    prefix: str | None,
    dry_run: bool,
    assumed_mb_per_sec: float,
    checkpoint_file: str | None = None,
    oc_password: str | None = None,
) -> dict:
    """
    Plan the backfill (dry_run=True) or carry it out (dry_run=False).

    A dry-run makes no Qdrant or OpenCode call and writes nothing, so qdrant_client and
    oc_password may be None. In apply mode every row is attempted; a failure is logged with
    its key and reason, and the run goes on with the next row.

    Returns a summary dict. Its keys differ by mode: see _dry_run_summary() for a dry-run,
    and the apply branch below for a real run.
    """
    claude_roots = [entry["path"] for entry in config.get("claude_chats", {}).get("roots", [])]
    oc_api_url = config.get("opencode", {}).get("api_url")

    classified = [(row, classify(row["file_path"], claude_roots)) for row in candidates(state_db, prefix)]
    if dry_run:
        return _dry_run_summary(classified, assumed_mb_per_sec)

    total = len(classified)
    now_iso = _now()
    kinds: Counter = Counter(kind for _, kind in classified)
    processed = 0
    skipped_journal = 0
    collisions = 0
    errors: list[tuple[str, str]] = []

    for done, (row, kind) in enumerate(classified, start=1):
        if kind == KIND_JOURNAL_SKIP:
            skipped_journal += 1
        else:
            try:
                if process_one(row, kind, qdrant_client, state_db, now_iso, oc_api_url, oc_password):
                    collisions += 1
                processed += 1
            except Exception as exc:  # per-row: log, count, continue; the row stays NULL
                reason = f"{type(exc).__name__}: {exc}"
                errors.append((row["file_path"], reason))
                logger.error("Backfill failed for %s — %s", row["file_path"], reason)

        if checkpoint_file and done % CHECKPOINT_EVERY == 0:
            _write_checkpoint(checkpoint_file, row["file_path"], done, total)

    return {
        "dry_run": False,
        "total": total,
        "kinds": kinds,
        "processed": processed,
        "skipped_journal": skipped_journal,
        "collisions": collisions,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Reporting and command line
# ---------------------------------------------------------------------------

def print_dry_run_report(summary: dict, prefix: str | None, assumed_mb_per_sec: float) -> None:
    """Print the planned work for a dry-run."""
    kinds = summary["kinds"]
    print("=== rag_metadata_backfill (DRY-RUN) ===")
    print(f"  prefix: {prefix or '(none)'}")
    print(f"  candidates (completed, pipeline_version NULL): {summary['total']}")
    print(f"  {KIND_NAS:<20} {kinds[KIND_NAS]:>8}  hashed; payload set in place")
    print(f"  {KIND_CLAUDE:<20} {kinds[KIND_CLAUDE]:>8}  hashed; re-keyed to claude-session://")
    print(f"  {KIND_JOURNAL_SKIP:<20} {kinds[KIND_JOURNAL_SKIP]:>8}  workflow journals; left untouched")
    print(f"  {KIND_OPENCODE:<20} {kinds[KIND_OPENCODE]:>8}  launch directory via OpenCode API")
    print(f"  hashing volume (nas + claude): {summary['bytes']} bytes, "
          f"about {summary['est_seconds']:.0f} s at {assumed_mb_per_sec} MB/s assumed")
    print(f"  unreadable (stat failed):      {len(summary['unreadable'])}")
    for path, reason in summary["unreadable"][:20]:
        print(f"    {path} — {reason}")
    print()
    print("(dry-run: nothing written. Re-run with --apply to carry out the plan.)")


def print_apply_summary(summary: dict) -> None:
    """Print the final tally for an apply run, with each failed row."""
    print("=== rag_metadata_backfill (APPLY) finished ===")
    print(f"  candidates:        {summary['total']}")
    print(f"  processed:         {summary['processed']}")
    print(f"  skipped (journal): {summary['skipped_journal']}")
    print(f"  collisions resolved: {summary['collisions']}")
    print(f"  errors:            {len(summary['errors'])}")
    for path, reason in summary["errors"]:
        print(f"    {path} — {reason}")


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point. Returns the process exit code: 0 clean, 1 if any row failed."""
    parser = argparse.ArgumentParser(
        description="One-time v0.2 metadata backfill for the documents collection and rag_state.db."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Report the plan only (the default).")
    mode.add_argument("--apply", action="store_true", help="Write the changes to Qdrant and SQLite.")
    parser.add_argument(
        "--prefix",
        metavar="PATH",
        help="Only rows whose file_path starts with PATH (for example the pilot subtree).",
    )
    parser.add_argument(
        "--assumed-mb-per-sec",
        type=float,
        default=50.0,
        help="Read speed assumed when estimating the hashing time in a dry-run (default 50).",
    )
    parser.add_argument(
        "--checkpoint-file",
        metavar="PATH",
        help="Progress file for --apply, one line per 100 rows. Default: next to rag_state.db.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry_run = not args.apply

    with open(CONFIG_PATH) as handle:
        config = yaml.safe_load(handle)
    db_base = config.get("db_base_path", "/mnt/nfs/__Backups/RAGNART--databases")
    db_path = f"{db_base}/sqlite/rag_state.db"

    if dry_run:
        # The dry-run never takes the lock and never writes. It opens SQLite read-only,
        # so a missing or unexpected file cannot be created or migrated by accident.
        state_db = StateDB(db_path, read_only=True)
        try:
            summary = run_backfill(
                config, state_db, None, args.prefix, True, args.assumed_mb_per_sec
            )
        finally:
            state_db.close()
        print_dry_run_report(summary, args.prefix, args.assumed_mb_per_sec)
        return 0

    # Apply: take both single-writer ingestion locks before the live StateDB is opened.
    # The index lock comes first, then the NAS lock (this tool writes rag_state.db, which the
    # NAS run owns). Contention exits at once with EXIT_LOCK_HELD (75) and names the holder.
    acquire_or_exit(lock_path_from_config(config))
    acquire_or_exit(nas_ingest_lock_path_from_config(config))

    # The OpenCode password is needed for opencode rows. It is loaded before any write, so a
    # missing password file stops the run with nothing changed.
    oc_password = load_password(config.get("opencode", {}).get("password_env_file"))
    checkpoint_file = args.checkpoint_file or f"{db_base}/sqlite/metadata_backfill.checkpoint"

    state_db = StateDB(db_path)
    qdrant_client = get_client(config["rag"]["qdrant_url"], timeout=120)
    try:
        summary = run_backfill(
            config,
            state_db,
            qdrant_client,
            args.prefix,
            False,
            args.assumed_mb_per_sec,
            checkpoint_file=checkpoint_file,
            oc_password=oc_password,
        )
    finally:
        state_db.close()
    print_apply_summary(summary)
    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
