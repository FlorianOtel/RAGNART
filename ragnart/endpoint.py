"""
Endpoint runner (v0.3 session B1): submits one endpoint host's items to the hub's ingestion API.

An endpoint host (the operator's Mac, in B1) runs this module, from its LaunchAgent or by hand:

    python -m ragnart.endpoint run --config ~/.ragnart/endpoint.yaml [--dry-run] [--grace-hours N] [--source ID]

One run goes through each configured source once, in this order:

  a) Status poll: rows that are pending with a request id are asked for their outcome.
  b) Scan: the source's root is walked and each item is described (size, mtime, content hash). A
     filesystem source walks its files; a claude_code source walks Claude Code transcripts and renders
     each session to text to hash it.
  c) Plan: new, changed and failed items are submitted; a vanished file that matches a new file by
     content is a move (the hub rewrites its key; filesystem only); other vanished items are marked
     missing and are deleted once they have been missing for the grace period.
  d) Submit: the planned requests go to the hub. Each one is recorded in the state database as it
     is accepted, so an interrupted run leaves a consistent state.
  e) Reconcile: the hub's own listing of this source's items is compared with the state. A hub item
     with no local row is deleted; a completed row missing from the hub is submitted again.
  f) Project markers: each `.ragnart/project.toml` under the root is offered to the hub. The hub
     refuses projects in B1, and the refusal is logged at WARNING.

--dry-run runs (a), (b) and (e) as reads, prints the plan, and writes nothing: no hub write and no
state write. The state database is opened read-only.

Dependencies: the standard library, PyYAML and httpx (through ragnart.endpoint_client). No docling,
qdrant_client or embedder is loaded (tests/test_ingestion_side_imports.py enforces that).
"""

from __future__ import annotations

import argparse
import fcntl
import logging
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from ragnart.chat_render import _parse_claude_chat
from ragnart.endpoint_client import HubClient, HubError
from ragnart.endpoint_state import EndpointState
from ragnart.hashing import hash_file, hash_text
from ragnart.moves import detect_moves
from ragnart.projects import parse_project_toml
from ragnart.ragignore import RagIgnoreMatcher
from ragnart.scanner import claude_jsonl_key
from ragnart.schema import validate_source_id

logger = logging.getLogger("ragnart.endpoint")

# Exit codes. 75 is the same EX_TEMPFAIL code the hub's ingestion lock uses (ragnart/lock.py).
EXIT_OK = 0
EXIT_FAILED = 1       # at least one source failed; the other sources still ran
EXIT_CONFIG = 2       # the config file is unreadable or invalid, or a --source id is unknown
EXIT_LOCK_HELD = 75   # another run holds the lock on this state database

CONNECTOR_FILESYSTEM = "filesystem"
CONNECTOR_CLAUDE_CODE = "claude_code"
CONNECTORS = (CONNECTOR_FILESYSTEM, CONNECTOR_CLAUDE_CODE)

# The file type recorded for a claude_code session. The hub stores the same type for its own sessions.
CLAUDE_FILE_TYPE = "claude_chat"

DEFAULT_GRACE_HOURS = 24

# Hard rule: nothing under /Volumes is walked or submitted. On the Mac the NAS is mounted there.
VOLUMES_DIR = "/Volumes"

# Names skipped everywhere in a source tree, whatever a .ragignore file says.
BUILTIN_EXCLUDED_NAMES = frozenset({".git", ".DS_Store"})
# Name prefixes skipped everywhere: AppleDouble sidecars (._name) and Office lock files (~$name).
BUILTIN_EXCLUDED_PREFIXES = ("._", "~$")

# A folder holding this marker is a ragnart project. The folder itself is still scanned as ordinary items.
PROJECT_MARKER_DIR = ".ragnart"
PROJECT_MARKER_FILE = "project.toml"

# The statuses with which the hub refuses an upload at the door: 413 too large, 422 not accepted.
# Nothing is queued for such an upload, so the item is recorded as ignored with the hub's reason.
REFUSED_AT_DOOR = (413, 422)

# A request is over once it reaches one of these. 'received' and 'processing' are not terminal.
TERMINAL_STATUSES = ("completed", "ignored", "failed")

_TOP_LEVEL_KEYS = frozenset({"hub_url", "state_db", "log_file", "grace_hours", "sources"})
_SOURCE_KEYS = frozenset({"id", "connector", "root", "secret_file", "ignore_file"})


class ConfigError(ValueError):
    """The config is missing a value, has an unknown key, or has a value out of range."""


class SourceError(RuntimeError):
    """A source cannot be walked at all (its root is missing, or lies under /Volumes). Nothing changes for it."""


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceConfig:
    """One source of this host: an id the hub knows it by, a connector, a root and a secret file."""
    id: str
    connector: str
    root: str
    secret_file: str
    # An optional global ignore file. Without one, only the .ragignore files under the root apply.
    ignore_file: str | None


@dataclass(frozen=True)
class EndpointConfig:
    """The whole config file, with ~ expanded in every path."""
    hub_url: str
    state_db: str
    log_file: str | None
    grace_hours: float
    sources: tuple[SourceConfig, ...]


@dataclass(frozen=True)
class FoundFile:
    """
    An item the walk found, with what the plan needs.

    For a filesystem file the key is its absolute real path. For a claude_code session the key is
    claude-session://... and path is the real path of the transcript the session is rendered from.
    """
    key: str
    file_type: str
    size: int
    mtime: float
    content_hash: str
    path: str


@dataclass(frozen=True)
class Action:
    """
    One thing the hub must hear, or one state change, decided by plan_actions().

    kind:
      'upsert'  submit the item (found; row is its state row, or None when it has none)
      'move'    change the key of a row from key to new_key, keeping its points (found, row)
      'observe' store the new size and mtime of an unchanged file (found)
      'missing' mark a row missing since `since` (row)
      'delete'  ask the hub to delete key, and drop its row (row is None for an orphan on the hub)
    """
    kind: str
    key: str
    found: FoundFile | None = None
    new_key: str | None = None
    row: dict | None = None
    since: str | None = None


@dataclass
class Scan:
    """What one walk of a source root found."""
    found: dict[str, FoundFile] = field(default_factory=dict)
    # Folders under the root that hold a .ragnart/project.toml.
    project_dirs: list[str] = field(default_factory=list)
    # False when the walk or a read failed part-way. An incomplete scan never marks an item as
    # vanished, because an unreadable folder looks exactly like a deleted file.
    complete: bool = True


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _required_text(raw: dict, name: str, where: str) -> str:
    """Return a required string value, or raise ConfigError."""
    value = raw.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where}: '{name}' is required and must be a non-empty string")
    return value


def _optional_text(raw: dict, name: str, where: str) -> str | None:
    """Return an optional string value (None when absent), or raise ConfigError."""
    if raw.get(name) is None:
        return None
    return _required_text(raw, name, where)


def _check_known_keys(raw: dict, allowed: frozenset, where: str) -> None:
    """Refuse an unknown key, so a misspelt option is an error rather than silently ignored."""
    unknown = set(raw) - allowed
    if unknown:
        raise ConfigError(f"{where}: unknown key(s) {sorted(unknown)}")


def _checked_grace_hours(value: object) -> float:
    """Return a grace period in hours: a number of zero or more (a bool is not a number here)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ConfigError(f"grace_hours must be a number of hours, zero or more; got {value!r}")
    return float(value)


def load_config(path: str) -> EndpointConfig:
    """
    Read and validate an endpoint.yaml file.

    Raises ConfigError for a missing or invalid value, an unknown key, or a duplicate source id
    (ids compare case-insensitively, as on the hub). OSError and yaml.YAMLError propagate.
    """
    with open(path, encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: the file must hold a YAML mapping")
    _check_known_keys(raw, _TOP_LEVEL_KEYS, path)

    sources_raw = raw.get("sources")
    if not isinstance(sources_raw, list) or not sources_raw:
        raise ConfigError(f"{path}: 'sources' must be a non-empty list")

    sources: list[SourceConfig] = []
    seen_ids: set[str] = set()
    for index, entry in enumerate(sources_raw):
        where = f"sources[{index}]"
        if not isinstance(entry, dict):
            raise ConfigError(f"{where} must be a mapping")
        _check_known_keys(entry, _SOURCE_KEYS, where)

        source_id = _required_text(entry, "id", where)
        try:
            validate_source_id(source_id)
        except ValueError as exc:
            raise ConfigError(f"{where}: {exc}") from exc
        if source_id.casefold() in seen_ids:
            raise ConfigError(f"{where}: duplicate source id {source_id!r} (ids compare case-insensitively)")
        seen_ids.add(source_id.casefold())

        connector = _required_text(entry, "connector", where)
        if connector not in CONNECTORS:
            raise ConfigError(f"{where}: connector {connector!r} is not one of {list(CONNECTORS)}")

        ignore_file = _optional_text(entry, "ignore_file", where)
        sources.append(SourceConfig(
            id=source_id,
            connector=connector,
            root=os.path.expanduser(_required_text(entry, "root", where)),
            secret_file=os.path.expanduser(_required_text(entry, "secret_file", where)),
            ignore_file=os.path.expanduser(ignore_file) if ignore_file is not None else None,
        ))

    log_file = _optional_text(raw, "log_file", path)
    return EndpointConfig(
        hub_url=_required_text(raw, "hub_url", path),
        state_db=os.path.expanduser(_required_text(raw, "state_db", path)),
        log_file=os.path.expanduser(log_file) if log_file is not None else None,
        grace_hours=_checked_grace_hours(raw.get("grace_hours", DEFAULT_GRACE_HOURS)),
        sources=tuple(sources),
    )


# ---------------------------------------------------------------------------
# Run lock and logging
# ---------------------------------------------------------------------------

def _take_run_lock(state_db: str):
    """
    Take the run lock of one state database, or return None when another run holds it.

    The lock is a flock on '<state_db>.lock', so a LaunchAgent run and a manual run never overlap.
    It is non-blocking: the second run gets None at once. The kernel releases the lock when the
    process exits, even after a crash, so a stale lock file cannot block a later run.
    """
    lock_path = os.path.abspath(state_db) + ".lock"
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    handle = open(lock_path, "a", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def _configure_logging(log_file: str | None) -> None:
    """Send this module's log lines to stdout, and to log_file when one is configured."""
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    logger.setLevel(logging.INFO)
    # Clear first, so a second call (in one process) does not double every line.
    logger.handlers.clear()

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)


# ---------------------------------------------------------------------------
# Paths: what may be walked
# ---------------------------------------------------------------------------

def allowed_real_path(real_path: str, root_real: str) -> bool:
    """
    True when a real (symlink-resolved) path may be walked or submitted.

    Refused: anything under /Volumes (hard rule), and anything outside the source root. The test is on
    the real path, so a symlink inside the root cannot lead the walk out of it. Both arguments are real
    paths (already through os.path.realpath).
    """
    if real_path == VOLUMES_DIR or real_path.startswith(VOLUMES_DIR + os.sep):
        return False
    root_prefix = root_real.rstrip(os.sep) + os.sep
    return real_path == root_real or real_path.startswith(root_prefix)


def _checked_root(root: str) -> str:
    """
    Return the real path of a source root, or raise SourceError.

    A root under /Volumes is refused outright. A root that is not a directory (an unmounted disk,
    a typo) is refused too: walking it would find nothing, and every item would look vanished.
    """
    real = os.path.realpath(root)
    if real == VOLUMES_DIR or real.startswith(VOLUMES_DIR + os.sep):
        raise SourceError(f"root {root} resolves to {real}, under {VOLUMES_DIR}: never walked")
    if not os.path.isdir(real):
        raise SourceError(f"root {root} is not a directory: nothing is changed for this source")
    return real


def _builtin_excluded(name: str) -> bool:
    """True for a file or folder name that is skipped everywhere (see BUILTIN_EXCLUDED_*)."""
    return name in BUILTIN_EXCLUDED_NAMES or name.startswith(BUILTIN_EXCLUDED_PREFIXES)


def _may_enter_folder(dirpath: str, name: str, root_real: str, matcher: RagIgnoreMatcher) -> bool:
    """True when the walk may descend into dirpath/name."""
    full = os.path.join(dirpath, name)
    # A symlinked folder is never entered, even when it points back inside the root.
    if os.path.islink(full):
        return False
    if _builtin_excluded(name) or matcher.is_dir_excluded(dirpath, name):
        return False
    return allowed_real_path(os.path.realpath(full), root_real)


# ---------------------------------------------------------------------------
# Scan (filesystem connector)
# ---------------------------------------------------------------------------

def _log_walk_failure(scan: Scan, source_id: str, error: OSError) -> None:
    """os.walk reports an unreadable folder to its onerror callback instead of raising. The scan is then incomplete."""
    scan.complete = False
    logger.warning("%s: cannot read %s: %s", source_id, error.filename, error.strerror)


def _describe_file(real: str, file_type: str, row: dict | None) -> FoundFile:
    """
    Return a file's size, mtime and content hash.

    The hash is read from the stored row when the stat matches it (same size and mtime), so an
    unchanged file is not read again. A new file, or one whose stat changed, is hashed.
    """
    stat = os.stat(real)
    if row is not None and row["size"] == stat.st_size and row["mtime"] == stat.st_mtime:
        content_hash = row["content_hash"]
    else:
        _, content_hash = hash_file(real)
    return FoundFile(
        key=real,
        file_type=file_type,
        size=stat.st_size,
        mtime=stat.st_mtime,
        content_hash=content_hash,
        path=real,
    )


def scan_filesystem(root_real: str, source: SourceConfig, accepted: set[str], rows: dict[str, dict]) -> Scan:
    """
    Walk the root and return the accepted files, each with its size, mtime and content hash.

    Skipped: symlinked folders; any path outside the root or under /Volumes (checked on the real path);
    built-in names; paths that a .ragignore file excludes; file types the hub does not accept. Only the
    .ragignore files under the root apply, unless the source names a global ignore file.
    """
    scan = Scan()
    # With no global ignore file configured, os.devnull stands in: it has no lines, so the global layer is empty.
    matcher = RagIgnoreMatcher(root_real, global_path=source.ignore_file or os.devnull)

    # followlinks=False: os.walk does not descend into a symlinked folder.
    for dirpath, dirnames, filenames in os.walk(
        root_real, followlinks=False, onerror=lambda error: _log_walk_failure(scan, source.id, error),
    ):
        # Prune in place, so the walk never enters a folder that is refused.
        dirnames[:] = [name for name in dirnames if _may_enter_folder(dirpath, name, root_real, matcher)]

        if os.path.isfile(os.path.join(dirpath, PROJECT_MARKER_DIR, PROJECT_MARKER_FILE)):
            scan.project_dirs.append(dirpath)

        for name in filenames:
            full = os.path.join(dirpath, name)
            if _builtin_excluded(name) or matcher.is_path_excluded(full):
                continue
            file_type = os.path.splitext(name)[1].lstrip(".").lower()
            if file_type not in accepted:
                continue
            real = os.path.realpath(full)
            if not allowed_real_path(real, root_real):
                logger.info("%s: skipped %s (its real path %s is outside the root or under /Volumes)", source.id, full, real)
                continue
            try:
                scan.found[real] = _describe_file(real, file_type, rows.get(real))
            except OSError as exc:
                # A file that cannot be read now makes the scan incomplete, so nothing is treated as vanished.
                scan.complete = False
                logger.warning("%s: cannot read %s: %s", source.id, real, exc.strerror)
    return scan


# ---------------------------------------------------------------------------
# Scan (claude_code connector)
# ---------------------------------------------------------------------------

def _claude_may_enter_folder(dirpath: str, name: str, root_real: str) -> bool:
    """
    True when the transcript walk may descend into dirpath/name.

    No .ragignore applies to transcripts: the hub's Claude Code connector applies none either. A symlinked
    folder is never entered, and the real path must stay under the root and off /Volumes.
    """
    full = os.path.join(dirpath, name)
    if os.path.islink(full):
        return False
    return allowed_real_path(os.path.realpath(full), root_real)


def _describe_session(key: str, path: str, row: dict | None) -> FoundFile:
    """
    Return one session's size, mtime and content hash.

    The content hash is the hash of the rendered text, so it is computed only when the file's size or
    mtime differs from the stored row. Otherwise the stored hash stands and the transcript is not rendered.
    """
    stat = os.stat(path)
    if (
        row is not None
        and row["size"] == stat.st_size
        and row["mtime"] == stat.st_mtime
        and row["content_hash"]
    ):
        content_hash = row["content_hash"]
    else:
        # _parse_claude_chat returns empty text when the file cannot be read, and an empty session would
        # delete the item's points on the hub. So readability is checked first, and an OSError propagates.
        with open(path, "rb"):
            pass
        text, _metadata, _launch_dir = _parse_claude_chat(Path(path))
        content_hash = hash_text(text)
    return FoundFile(
        key=key,
        file_type=CLAUDE_FILE_TYPE,
        size=stat.st_size,
        mtime=stat.st_mtime,
        content_hash=content_hash,
        path=path,
    )


def scan_claude_code(root_real: str, source: SourceConfig, rows: dict[str, dict]) -> Scan:
    """
    Walk the root for Claude Code transcripts and return one FoundFile per session key.

    Discovery is the hub's Claude Code connector's (ragnart/scanner.py claude_jsonl_key): main sessions,
    direct subagent transcripts and workflow agents are admitted, and a workflow journal is excluded
    (claude_jsonl_key returns None for it). Symlinked transcripts and folders are never followed.

    Several files with one key are copies of one session. The newest copy by mtime wins, and an exact
    tie goes to the greatest path. This is the duplicate rule of docs/RAGNART-design.md §4.2, which the
    scanner applies to its own queue; it is reimplemented here because the scanner's copy is inline in
    its database writes.
    """
    scan = Scan()
    # Maps each key to the (mtime, path) of its winning copy so far.
    winners: dict[str, tuple[float, str]] = {}

    # followlinks=False: os.walk does not descend into a symlinked folder.
    for dirpath, dirnames, filenames in os.walk(
        root_real, followlinks=False, onerror=lambda error: _log_walk_failure(scan, source.id, error),
    ):
        dirnames[:] = [name for name in dirnames if _claude_may_enter_folder(dirpath, name, root_real)]

        for name in filenames:
            if not name.endswith(".jsonl"):
                continue
            full = os.path.join(dirpath, name)
            if os.path.islink(full) or not allowed_real_path(os.path.realpath(full), root_real):
                logger.info("%s: skipped %s (a symlink, or its real path is outside the root or under /Volumes)", source.id, full)
                continue
            key = claude_jsonl_key(Path(full))
            if key is None:
                # A workflow journal: an orchestration log, not a conversation. It is never submitted.
                continue
            try:
                mtime = os.stat(full).st_mtime
            except OSError as exc:
                scan.complete = False
                logger.warning("%s: cannot read %s: %s", source.id, full, exc.strerror)
                continue
            # Newest copy wins; an exact tie goes to the greatest path. Tuple comparison does both.
            best = winners.get(key)
            if best is None or (mtime, full) > best:
                winners[key] = (mtime, full)

    for key, (_mtime, path) in winners.items():
        try:
            scan.found[key] = _describe_session(key, path, rows.get(key))
        except OSError as exc:
            # An unreadable session makes the scan incomplete, so nothing is treated as vanished.
            scan.complete = False
            logger.warning("%s: cannot read %s: %s", source.id, path, exc.strerror)
    return scan


# ---------------------------------------------------------------------------
# Plan: decide, without doing anything
# ---------------------------------------------------------------------------

def _iso_utc(mtime: float) -> str:
    """A file's mtime as an ISO-8601 UTC string with its offset, as the hub requires for modified_at."""
    return datetime.fromtimestamp(mtime, timezone.utc).isoformat()


def plan_actions(
    found: dict[str, FoundFile],
    rows: dict[str, dict],
    complete: bool,
    now: datetime,
    grace: timedelta,
    hub_keys: set[str],
    allow_moves: bool,
) -> list[Action]:
    """
    Decide what the hub must hear for one source, and which local rows change. Performs no I/O.

    found:     the items on disk, keyed by key (a real path for a filesystem file).
    rows:      the state rows of this source, keyed by key (as they stand before this run's writes).
    complete:  False when the scan was partial. Then nothing is treated as vanished, and no move is made.
    now, grace: a row missing for at least `grace` is deleted.
    hub_keys:  the keys the hub lists for this source.
    allow_moves: True for the filesystem connector. A claude_code session never moves: its key is its
                 uuid, so a vanished session is missing and then deleted, like any vanished item.

    The actions come back in the order they must run.
    """
    actions: list[Action] = []

    # 1. Moves. A completed row whose file vanished, and a new file with the same content hash, pair up
    # when each hash occurs exactly once on each side (ragnart/moves.py, design docs/RAGNART-design.md §4.2).
    # A move keeps the points: the hub rewrites the key, so nothing is re-embedded.
    moves: dict[str, str] = {}
    if complete and allow_moves:
        vanished_hashes = {
            key: row["content_hash"]
            for key, row in rows.items()
            if key not in found and row["status"] == "completed"
        }
        new_hashes = {key: f.content_hash for key, f in found.items() if key not in rows}
        moves = detect_moves(vanished_hashes, new_hashes)
    moved_old = set(moves)
    moved_new = set(moves.values())
    for old, new in moves.items():
        actions.append(Action("move", old, found=found[new], new_key=new, row=rows[old]))

    # 2. Files on disk. A new file, a file whose content changed, and a failed file are submitted.
    # An unchanged file whose stored size, mtime or missing mark is out of date is observed (state only).
    # Anything else (unchanged, and completed, ignored or pending) needs nothing.
    upserted: set[str] = set()
    for key, f in found.items():
        if key in moved_new:
            continue
        row = rows.get(key)
        if row is None or row["content_hash"] != f.content_hash or row["status"] == "failed":
            actions.append(Action("upsert", key, found=f, row=row))
            upserted.add(key)
        elif (row["size"], row["mtime"], row["missing_since"]) != (f.size, f.mtime, None):
            actions.append(Action("observe", key, found=f))

    # 3. Files that are gone. The first sighting records missing_since. Once the item has been missing for
    # the grace period, it is deleted. A reappearance clears missing_since (see EndpointState.observe).
    deleted: set[str] = set()
    if complete:
        for key, row in rows.items():
            if key in found or key in moved_old:
                continue
            if row["missing_since"] is None:
                since_text = now.isoformat(timespec="seconds")
                actions.append(Action("missing", key, row=row, since=since_text))
            else:
                since_text = row["missing_since"]
            if now - datetime.fromisoformat(since_text) >= grace:
                actions.append(Action("delete", key, row=row))
                deleted.add(key)

    # 4. Reconcile with the hub's listing, judged against the local rows as this run leaves them.
    # A key the hub lists with no local row is this source's leftover, and is deleted. The keys this run
    # moved or deleted are excluded: the hub still lists them until its worker has applied the request.
    # When there are no local rows at all, the orphan rule is not applied: an empty state (a lost state
    # database) must never delete the hub's items.
    handled = moved_old | deleted
    planned_local = (set(rows) - handled) | moved_new | upserted
    if rows:
        for key in sorted(hub_keys - planned_local - handled):
            actions.append(Action("delete", key))

    # A completed row with chunks that the hub no longer lists is submitted again, while the file is on disk.
    # Rows already submitted this run are skipped.
    for key, row in rows.items():
        if (
            row["status"] == "completed"
            and (row["chunk_count"] or 0) > 0
            and key in found
            and key not in hub_keys
            and key not in upserted
        ):
            actions.append(Action("upsert", key, found=found[key], row=row))

    return actions


# ---------------------------------------------------------------------------
# Execute: each action is a hub request, then a state write
# ---------------------------------------------------------------------------

def _describe(action: Action) -> str:
    """A short text of one action, for the log."""
    if action.kind == "move":
        return f"change key {action.key} -> {action.new_key}"
    if action.kind == "missing":
        return f"mark missing {action.key} (since {action.since})"
    return f"{action.kind} {action.key}"


def _send_upsert(action: Action, source: SourceConfig, client: HubClient) -> dict:
    """
    Send one item to the hub and return the hub's answer.

    A filesystem file goes as its bytes. A claude_code session is rendered here, and goes as text with
    the three chat fields the API allows as metadata. An empty session still goes: the hub records it
    completed with no chunks (ragnart/ingest.py ingest_content).
    """
    found = action.found
    if source.connector == CONNECTOR_CLAUDE_CODE:
        text, metadata, _launch_dir = _parse_claude_chat(Path(found.path))
        return client.upsert_text(found.key, text, metadata=metadata, modified_at=_iso_utc(found.mtime))
    return client.upsert_bytes(found.key, found.key, found.file_type, modified_at=_iso_utc(found.mtime))


def _submit_upsert(
    action: Action,
    source: SourceConfig,
    client: HubClient,
    state: EndpointState,
    counts: Counter,
    hub_keys: set[str],
) -> None:
    """
    Send one item. Refused at the door (413, 422) means recorded as ignored with the hub's reason.

    Before a refused item is recorded ignored, its old points are deleted when it had any. An item had
    points when its row was completed with chunks, or when the hub lists its key. This is the same rule
    as the hub's own path, where an item that becomes ignored loses its old points (ragnart/ingest.py
    deletes before admission). Without the delete, the file's old text would stay searchable.
    """
    found = action.found
    try:
        answer = _send_upsert(action, source, client)
    except HubError as exc:
        if exc.status_code not in REFUSED_AT_DOOR:
            raise
        row = action.row
        had_points = (
            (row is not None and row["status"] == "completed" and (row["chunk_count"] or 0) > 0)
            or found.key in hub_keys
        )
        if had_points:
            # The delete is accepted before the row changes. If it fails, the row keeps its old state and
            # the next run tries the upsert, and the refusal, again.
            client.delete(found.key)
        # Nothing was queued. The row is ignored, and the file is submitted again only when its content changes.
        state.put(
            source.id, found.key,
            size=found.size, mtime=found.mtime, content_hash=found.content_hash,
            status="ignored", reason=str(exc.detail),
        )
        counts["refused"] += 1
        logger.info("%s: refused by the hub %s: %s (old points deleted: %s)", source.id, found.key, exc.detail, had_points)
        return
    state.put(
        source.id, found.key,
        size=found.size, mtime=found.mtime, content_hash=found.content_hash,
        status="pending", request_id=answer["request_id"],
    )
    logger.info("%s: submitted %s (request %s)", source.id, found.key, answer["request_id"])


def _submit_move(action: Action, source: SourceConfig, client: HubClient, state: EndpointState) -> None:
    """Ask the hub to change a key. The row moves to the new key and keeps its chunk count until the hub reports one."""
    found = action.found
    answer = client.change_key(action.key, action.new_key)
    state.delete(source.id, action.key)
    state.put(
        source.id, action.new_key,
        size=found.size, mtime=found.mtime, content_hash=found.content_hash,
        status="pending", request_id=answer["request_id"],
        chunk_count=action.row["chunk_count"],
    )
    logger.info("%s: moved %s -> %s (request %s)", source.id, action.key, action.new_key, answer["request_id"])


def _execute(
    actions: list[Action],
    source: SourceConfig,
    client: HubClient,
    state: EndpointState,
    dry_run: bool,
    hub_keys: set[str],
) -> Counter:
    """Run the actions in order, or only log them on a dry run. Returns how many of each kind there were."""
    counts: Counter = Counter()
    for action in actions:
        counts[action.kind] += 1
        if dry_run:
            logger.info("%s: would %s", source.id, _describe(action))
            continue
        if action.kind == "upsert":
            _submit_upsert(action, source, client, state, counts, hub_keys)
        elif action.kind == "move":
            _submit_move(action, source, client, state)
        elif action.kind == "observe":
            state.observe(source.id, action.key, action.found.size, action.found.mtime)
        elif action.kind == "missing":
            state.set_missing(source.id, action.key, action.since)
            logger.info("%s: %s", source.id, _describe(action))
        elif action.kind == "delete":
            # The row is dropped as soon as the hub accepts the request. A delete is idempotent, and the
            # reconcile step above sends it again if the hub still lists the key on a later run.
            client.delete(action.key)
            state.delete(source.id, action.key)
            logger.info("%s: deleted %s", source.id, action.key)
        else:
            raise ValueError(f"unknown action kind {action.kind!r}")
    return counts


def _poll_outstanding(source: SourceConfig, client: HubClient, state: EndpointState, rows: dict[str, dict], dry_run: bool) -> Counter:
    """
    Ask the hub about each pending row, and record a terminal outcome.

    The in-memory rows are updated in every mode, so the plan that follows sees the same rows in a
    dry run as in a real run. Only the state write is skipped on a dry run.
    """
    counts: Counter = Counter()
    for key, row in rows.items():
        if row["status"] != "pending" or not row["request_id"]:
            continue
        try:
            answer = client.status(row["request_id"])
        except HubError as exc:
            if exc.status_code != 404:
                raise
            # The hub does not know the request, so it was never applied. The row becomes failed, and the plan submits it again.
            status, reason, chunk_count = "failed", "the hub does not know this request", row["chunk_count"]
        else:
            if answer["status"] not in TERMINAL_STATUSES:
                continue  # still queued or running: nothing to record yet
            status = answer["status"]
            reason = {
                "completed": None,
                "ignored": answer.get("admission_reason"),
                "failed": answer.get("error"),
            }[status]
            # A change_key request reports no chunk count, so the row keeps the one it had.
            chunk_count = answer.get("chunk_count")
            if chunk_count is None:
                chunk_count = row["chunk_count"]

        row.update(status=status, reason=reason, chunk_count=chunk_count)
        counts[status] += 1
        logger.info("%s: %s %s (%s)", source.id, status, key, reason or f"chunks={chunk_count}")
        if not dry_run:
            state.set_outcome(source.id, key, status, reason, chunk_count)
    return counts


def _publish_projects(source: SourceConfig, client: HubClient, project_dirs: list[str], dry_run: bool) -> Counter:
    """Offer each project marker to the hub. In B1 the hub refuses every one, and the refusal is logged at WARNING."""
    counts: Counter = Counter()
    for dirpath in project_dirs:
        marker = os.path.join(dirpath, PROJECT_MARKER_DIR, PROJECT_MARKER_FILE)
        project = parse_project_toml(marker)
        if project is None:
            # parse_project_toml has already logged why the marker is invalid.
            counts["invalid_marker"] += 1
            continue
        if dry_run:
            logger.info("%s: would offer project %s from %s", source.id, project["id"], dirpath)
            continue
        answer = client.publish_project(project["id"], project["name"], path=dirpath)
        if answer.get("refused"):
            counts["refused"] += 1
            logger.warning("%s: refused by the hub: %s", source.id, answer.get("reason"))
        else:
            counts["published"] += 1
            logger.info("%s: published project %s from %s", source.id, project["id"], dirpath)
    return counts


def run_source(
    source: SourceConfig,
    *,
    client: HubClient,
    state: EndpointState,
    grace: timedelta,
    now: datetime,
    dry_run: bool,
) -> Counter:
    """
    Run one source once: poll, scan, plan, submit, reconcile, publish. Returns the counts of the run.

    Raises SourceError when the root cannot be walked, and HubError for a hub failure the run cannot
    recover from. Every state write happens after its hub request is accepted, so when the run stops
    early the state holds only completed steps, and the next run starts from them.

    The two connectors differ only in how the walk finds items (scan_filesystem, scan_claude_code). The
    poll, plan, submit, reconcile and publish steps serve both.
    """
    root_real = _checked_root(source.root)
    rows = state.rows(source.id)

    counts: Counter = Counter()
    counts.update(_poll_outstanding(source, client, state, rows, dry_run))

    if source.connector == CONNECTOR_CLAUDE_CODE:
        scan = scan_claude_code(root_real, source, rows)
    else:
        accepted = {ext.lstrip(".").lower() for ext in client.info()["accepted_file_types"]}
        scan = scan_filesystem(root_real, source, accepted, rows)
    counts["found"] = len(scan.found)

    # A walk that finds no item while the state holds rows for this source is incomplete: the 'Incomplete
    # when' rule of docs/RAGNART-design.md §4.7. A root that is empty, unmounted or misread would otherwise
    # make every row look vanished, and the next step would delete them all after the grace period.
    if scan.complete and not scan.found and rows:
        scan.complete = False
        logger.warning(
            "%s: the walk found no item but the state holds %d row(s) for this source; the walk is incomplete",
            source.id, len(rows),
        )
    if not scan.complete:
        logger.warning("%s: the walk was incomplete; no item is treated as vanished on this run", source.id)

    hub_keys = {item["key"] for item in client.items()}
    actions = plan_actions(
        scan.found, rows, scan.complete, now, grace, hub_keys,
        allow_moves=source.connector == CONNECTOR_FILESYSTEM,
    )
    counts.update(_execute(actions, source, client, state, dry_run, hub_keys))
    counts.update(_publish_projects(source, client, scan.project_dirs, dry_run))

    logger.info("%s: summary %s", source.id, dict(sorted(counts.items())))
    return counts


def run(
    config: EndpointConfig,
    *,
    dry_run: bool = False,
    grace_hours: float | None = None,
    source_id: str | None = None,
    now: datetime | None = None,
) -> int:
    """
    Run the configured sources (or the one named by source_id) once. Returns an exit code.

    A failure in one source is logged and the others still run. The exit code is EXIT_FAILED when any
    source failed. The caller must hold the run lock (main() does).
    """
    if grace_hours is not None and grace_hours < 0:
        raise ConfigError(f"grace_hours must be zero or more; got {grace_hours!r}")
    grace = timedelta(hours=config.grace_hours if grace_hours is None else grace_hours)
    now = now if now is not None else datetime.now(timezone.utc)

    sources = list(config.sources)
    if source_id is not None:
        sources = [source for source in sources if source.id.casefold() == source_id.casefold()]
        if not sources:
            raise ConfigError(f"no source {source_id!r} in the config")

    state = EndpointState(config.state_db, read_only=dry_run)
    failed = 0
    try:
        for source in sources:
            try:
                # The client is not closed here. The process ends after the run, and closing would also
                # close a client that a caller passed in (the tests do).
                client = HubClient(config.hub_url, source.secret_file)
                run_source(source, client=client, state=state, grace=grace, now=now, dry_run=dry_run)
            except SourceError as exc:
                failed += 1
                logger.error("%s: %s", source.id, exc)
            except Exception:
                failed += 1
                logger.exception("%s: the run failed", source.id)
    finally:
        state.close()
    return EXIT_OK if failed == 0 else EXIT_FAILED


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    """The command line: one subcommand, 'run'."""
    parser = argparse.ArgumentParser(
        prog="python -m ragnart.endpoint",
        description="Submit this endpoint host's items to the RAGNART hub's ingestion API.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    run_cmd = commands.add_parser("run", help="run every configured source once")
    run_cmd.add_argument("--config", required=True, help="path to endpoint.yaml")
    run_cmd.add_argument("--dry-run", action="store_true", help="print the plan; write nothing to the hub or the state")
    run_cmd.add_argument("--grace-hours", type=float, default=None, help="override grace_hours from the config")
    run_cmd.add_argument("--source", default=None, help="run only the source with this id")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns the exit code (see the EXIT_* constants)."""
    args = _build_parser().parse_args(argv)
    try:
        config = load_config(args.config)
    except (OSError, yaml.YAMLError, ConfigError) as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    _configure_logging(config.log_file)
    lock = _take_run_lock(config.state_db)
    if lock is None:
        logger.warning("another endpoint run holds the lock on %s: this run does nothing", config.state_db)
        return EXIT_LOCK_HELD
    try:
        return run(config, dry_run=args.dry_run, grace_hours=args.grace_hours, source_id=args.source)
    except ConfigError as exc:
        logger.error("config error: %s", exc)
        return EXIT_CONFIG
    finally:
        lock.close()


if __name__ == "__main__":
    sys.exit(main())
