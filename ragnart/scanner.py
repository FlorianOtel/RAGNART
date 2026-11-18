"""
NFS filesystem scanner — discovers files and populates the ingestion queue.

Used by:
  - utils/rag_sync_nfs.py  (CLI invocation; run on Server 2 by scripts/rag-ingest-run.sh)
  (The orchestrator's POST /v1/rag/ingest/sync was removed 2026-11-04: the RAG
  state DB must only be opened from one host. See main.py's RAG SEARCH ENDPOINT note.)

Inclusion rules come from config["rag"]["scanner"]["include_extensions"] in
ragnart.yaml. Exclusion rules come from config/ragignore (global) and from
per-directory .ragignore files; both are applied by ragnart/ragignore.py.

ragnart.yaml must not carry the removed exclude_dir_names, exclude_dir_suffixes
or exclude_file_patterns keys. A config that still has one of them raises
ValueError, so a stale exclusion is never silently ignored.
"""

from __future__ import annotations

import logging
import os
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

from .hashing import hash_file
from .opencode_client import get_json, load_password
from .projects import parse_project_toml
from .ragignore import RagIgnoreMatcher
from .schema import source_id_for_key
from .state import StateDB

logger = logging.getLogger(__name__)

# The directory that marks a ragnart project, and the marker file inside it.
# The directory is never walked as content: its files are configuration, not documents.
RAGNART_DIRNAME = ".ragnart"
PROJECT_MARKER_FILENAME = "project.toml"


# The ragnart.yaml keys that exclusions used to live in. Exclusions now live in
# config/ragignore and per-directory .ragignore files. A config that still sets
# one of these keys is refused.
_REMOVED_EXCLUDE_KEYS = ("exclude_dir_names", "exclude_dir_suffixes", "exclude_file_patterns")


# ---------------------------------------------------------------------------
# Inclusion helper
# ---------------------------------------------------------------------------

def is_includable(path: str, include_exts: frozenset[str]) -> bool:
    """
    True if the file's extension is on the include whitelist (case-insensitive).

    This is the inclusion half of the scanner's rules only. Exclusion rules
    (config/ragignore and .ragignore files) are not applied here. Callers that
    need them use RagIgnoreMatcher as well.
    """
    return Path(path).suffix.lower() in include_exts


def include_extensions_from_config(config: dict) -> frozenset[str]:
    """
    Return rag.scanner.include_extensions as a frozenset.

    Raises ValueError when rag.scanner is missing, when it still sets a removed
    exclusion key, or when include_extensions is missing. Shared by scan_nfs_roots
    and the read-only CLI, so both refuse the same configs.
    """
    scanner_cfg = config.get("rag", {}).get("scanner")
    if not scanner_cfg:
        raise ValueError(
            "ragnart.yaml is missing the 'rag.scanner' section. "
            "Add include_extensions."
        )

    # Exclusion keys moved to config/ragignore. A stale key would be silently
    # ignored, so refuse to run until it is removed.
    stale = [k for k in _REMOVED_EXCLUDE_KEYS if k in scanner_cfg]
    if stale:
        raise ValueError(
            f"ragnart.yaml rag.scanner still sets removed exclusion key(s): {', '.join(stale)}. "
            "Exclusions now live in config/ragignore and per-directory .ragignore files. "
            "Remove these keys from ragnart.yaml."
        )

    _REQUIRED = ("include_extensions",)
    missing = [k for k in _REQUIRED if k not in scanner_cfg]
    if missing:
        raise ValueError(
            f"ragnart.yaml rag.scanner is missing required key(s): {', '.join(missing)}"
        )

    return frozenset(scanner_cfg["include_extensions"])


def nfs_roots_from_config(
    config: dict,
    user_filter: str | None = None,
) -> list[tuple[str, str]]:
    """
    Return (nfs_root_path, owner) pairs from the 'users' and 'shared' config sections.

    The order is the config order: per-user roots first, then shared roots.
    If user_filter is set, only that owner's roots are returned and shared roots
    are left out.
    """
    roots_to_scan: list[tuple[str, str]] = []  # (nfs_root_path, owner)

    for _email, cfg in config.get("users", {}).items():
        owner = cfg["owner"]
        if user_filter and owner != user_filter:
            continue
        for root in cfg.get("nfs_roots", []):
            roots_to_scan.append((root, owner))

    # Shared roots are included unless we're filtering to a specific user
    if not user_filter:
        shared = config.get("shared", {})
        for root in shared.get("nfs_roots", []):
            roots_to_scan.append((root, shared["owner"]))

    return roots_to_scan


def _dirpath_is_excluded(matcher: RagIgnoreMatcher, root: str, dirpath: str) -> bool:
    """
    True if the walked directory `dirpath` is excluded by the ignore rules.

    The crawl root itself cannot be excluded: RagIgnoreMatcher refuses paths equal to
    its root, and nothing above the root is consulted. Every other directory the walk
    yields has already survived its parent's pruning, so this check normally returns
    False. It is kept so a marker can never be read from an excluded directory even if
    the pruning step changes.
    """
    if os.path.normpath(dirpath) == os.path.normpath(root):
        return False
    return matcher.is_dir_excluded(os.path.dirname(dirpath), os.path.basename(dirpath))


def _read_project_marker(dirpath: str, owner: str) -> dict | None:
    """
    Return the project marker of `dirpath` as a dict, or None.

    The marker is `<dirpath>/.ragnart/project.toml`. A missing file returns None
    silently. A present but invalid file returns None, and parse_project_toml has
    already logged the reason at ERROR. The directory then stays an ordinary
    directory and its files are still scanned.

    On success the dict is {path, owner, name, id, inherit_access, mode, tags}.
    `path` is the directory with any trailing separator removed, so it matches the
    paths that file rows carry.
    """
    toml_path = os.path.join(dirpath, RAGNART_DIRNAME, PROJECT_MARKER_FILENAME)
    if not os.path.isfile(toml_path):
        return None
    parsed = parse_project_toml(toml_path)
    if parsed is None:
        return None
    return {
        "path": dirpath.rstrip(os.sep) or os.sep,
        "owner": owner,
        **parsed,
    }


def _hash_new_file(filepath: str) -> tuple[int | None, str | None, str | None]:
    """
    Hash a genuinely new file and return (size_bytes, content_hash, hashed_at).

    A file that cannot be read is admitted without a hash rather than dropped. A
    missing hash only means the file cannot be a move target or source, and the
    ingestion worker hashes it again before ingesting. The failure is logged.
    """
    try:
        size_bytes, content_hash = hash_file(filepath)
    except OSError as exc:
        logger.warning("could not hash new file %s, admitting it without a hash: %s", filepath, exc)
        return None, None, None
    return size_bytes, content_hash, datetime.now(timezone.utc).isoformat()


def scan_nfs_roots(
    state_db: StateDB,
    config: dict,
    user_filter: str | None = None,
) -> dict:
    """
    Walk all configured NFS roots and update the ingestion queue.

    Reads the top-level 'users' and 'shared' sections from config to discover
    which NFS paths to scan and which owner string to assign.

    For each discovered file:
      - New files      → inserted as 'pending'
      - Modified files → mtime on disk > stored mtime → reset to 'pending'
      - Unchanged      → no-op

    Completed rows whose files no longer exist are removed from SQLite;
    their Qdrant points should be cleaned up separately.

    Args:
        state_db:    StateDB instance to populate.
        config:      Full config dict (reads 'users' + 'shared' sections).
        user_filter: If set (e.g. "florian"), only scan that owner's NFS roots.

    Project markers:
        A directory holding `.ragnart/project.toml` is a ragnart project. The marker is
        read before the directory's children are pruned, so a marker is never missed
        because of a rule on its children. A directory excluded by .ragignore is not a
        project, so its marker is skipped. The `.ragnart` directory itself is never
        walked as content.

    New-file hashing:
        A file with no queue row yet is hashed here, so a move can be detected in the
        same sync that finds the old path vanished. Files that already have a row are
        not hashed.

    Returns:
        {
          'scanned': N,                  # files admitted to the queue this scan
          'existing_paths': set[str],    # every admitted path, for find_deleted()
          'skipped_roots': list[str],    # roots that could not be trusted this scan
          'project_markers': list[dict], # {path, owner, name, id, inherit_access, mode, tags}
          'new_hashes': dict[str, str],  # file_path -> content_hash, for genuinely new files only
        }

        A root is skipped when it is not a directory, or when it is a directory that
        yielded zero files while the queue still holds rows under it. An unmounted
        NFS mount point looks like the second case. Callers must not delete rows
        for a skipped root, because its files were not seen.
    """
    # Inclusion rules and the removed-key check (fail loud, no silent fallback).
    include_exts = include_extensions_from_config(config)

    roots_to_scan = nfs_roots_from_config(config, user_filter)

    if not roots_to_scan:
        logger.warning(
            "No NFS roots to scan. "
            "Fill in 'users' / 'shared' sections in ragnart.yaml."
        )

    scanned = 0
    existing_paths: set[str] = set()
    skipped_roots: list[str] = []
    project_markers: list[dict] = []
    new_hashes: dict[str, str] = {}
    visited_real_dirs: set[str] = set()   # global — prevents re-walking the same real dir via different symlinks
    visited_real_files: set[str] = set()  # global — prevents ingesting the same real file via different symlink paths

    for root, owner in roots_to_scan:
        if not os.path.isdir(root):
            logger.warning("NFS root not accessible, skipping: %s", root)
            skipped_roots.append(root)
            continue

        # One matcher per root. Its global rules and per-directory .ragignore files
        # are all interpreted relative to this root.
        matcher = RagIgnoreMatcher(root)

        root_count = 0
        logger.info("Scanning %s  (owner=%s)", root, owner)

        for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=True):
            # Directory dedup — skip any real dir path already visited (handles circular symlinks
            # and the same dir reachable via multiple symlinks across roots)
            real_dirpath = os.path.realpath(dirpath)
            if real_dirpath in visited_real_dirs:
                dirnames.clear()
                continue
            visited_real_dirs.add(real_dirpath)

            # Project marker. Read before the pruning below, so a marker directory is
            # never missed because of a rule on its own children. An excluded directory
            # is not a project, so its marker is skipped.
            if RAGNART_DIRNAME in dirnames and not _dirpath_is_excluded(matcher, root, dirpath):
                marker = _read_project_marker(dirpath, owner)
                if marker is not None:
                    project_markers.append(marker)

            # Prune excluded subtrees in-place — prevents os.walk from descending.
            # Because an excluded directory is never entered, a .ragignore inside it
            # is never read, so nothing under it can be re-included.
            # The .ragnart marker directory is pruned unconditionally: it holds
            # configuration, not content.
            dirnames[:] = [
                d for d in dirnames
                if d != RAGNART_DIRNAME and not matcher.is_dir_excluded(dirpath, d)
            ]

            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                # Inclusion first (cheap whitelist), then the exclusion rules.
                if not is_includable(filepath, include_exts):
                    continue
                if matcher.is_path_excluded(filepath):
                    continue
                real_filepath = os.path.realpath(filepath)
                if real_filepath in visited_real_files:
                    continue
                visited_real_files.add(real_filepath)
                try:
                    mtime = os.path.getmtime(filepath)
                except OSError:
                    continue

                # Hash only a file with no queue row yet. A file already in the queue keeps
                # the hash it was given at discovery, so no existing file is re-read here.
                size_bytes = content_hash = hashed_at = None
                if not state_db.row_exists(filepath):
                    size_bytes, content_hash, hashed_at = _hash_new_file(filepath)
                    if content_hash is not None:
                        new_hashes[filepath] = content_hash

                state_db.discover_or_update(
                    filepath,
                    owner,
                    mtime,
                    origin_path=filepath,
                    source_type="file",
                    file_type=Path(filepath).suffix.lstrip(".").lower(),
                    size_bytes=size_bytes,
                    content_hash=content_hash,
                    hashed_at=hashed_at,
                    source_id=source_id_for_key(filepath),
                )
                existing_paths.add(filepath)
                scanned += 1
                root_count += 1

        logger.info("  → %d file(s) found in %s", root_count, root)

        # An empty directory while the queue still holds rows under it is treated as
        # unmounted: a mount point that failed to mount is an empty directory too.
        # The check is read-only.
        if root_count == 0 and state_db.count_with_prefix(os.path.join(root, "")) > 0:
            logger.warning(
                "NFS root %s yielded 0 files but the queue has rows under it; treating as unmounted",
                root,
            )
            skipped_roots.append(root)

    return {
        "scanned": scanned,
        "existing_paths": existing_paths,
        "skipped_roots": skipped_roots,
        "project_markers": project_markers,
        "new_hashes": new_hashes,
    }


def claude_dot_claude_dir(path: Path) -> Path:
    """
    Return the `.claude` directory that holds a Claude Code transcript.

    Anchors on the `projects` directory instead of counting parent levels, so the same
    answer comes back for a main session, a direct subagent transcript, and a
    workflow-depth subagent transcript (`.../subagents/workflows/wf_X/agent-*.jsonl`).
    The nearest `projects` component is the one directly under `.claude`: mangled
    project directory names (e.g. `-mnt-nfs-...-projects-RAGNART`) never equal `projects`.
    """
    parts = path.parts
    projects_indexes = [index for index, part in enumerate(parts) if part == "projects"]
    if not projects_indexes:
        raise ValueError(f"not a Claude Code transcript path (no 'projects' directory): {path}")
    return Path(*parts[: projects_indexes[-1]])


def claude_jsonl_key(path: Path) -> str | None:
    """
    Return the ingestion-queue key for a Claude Code transcript, or None to exclude it.

    The key depends on the session the file belongs to, not on where the file sits:

      main session        .../<uuid>.jsonl                                       -> claude-session://<uuid>
      direct subagent     .../<uuid>/subagents/agent-<id>.jsonl                  -> claude-session://<uuid>/agent-<id>
      workflow agent      .../<uuid>/subagents/workflows/wf_<w>/agent-<id>.jsonl -> claude-session://<uuid>/agent-<id>
      workflow journal    .../<uuid>/subagents/workflows/wf_<w>/journal.jsonl    -> None

    A workflow journal is an orchestration log, not a conversation, so it is never
    indexed. None means the scanner must not discover the file at all.
    """
    # Workflow depth: .../<uuid>/subagents/workflows/wf_<w>/<file>.jsonl
    if path.parent.parent.name == "workflows" and path.parent.parent.parent.name == "subagents":
        if path.name == "journal.jsonl":
            return None
        session_uuid = path.parent.parent.parent.parent.name
        return f"claude-session://{session_uuid}/{path.stem}"

    # Direct subagent: .../<uuid>/subagents/<file>.jsonl
    if path.parent.name == "subagents":
        session_uuid = path.parent.parent.name
        return f"claude-session://{session_uuid}/{path.stem}"

    # Main session: .../<uuid>.jsonl
    return f"claude-session://{path.stem}"


def scan_claude_chats(
    state_db: StateDB,
    config: dict,
    user_filter: str | None = None,
) -> dict:
    """
    Walk configured claude_chats roots and update the ingestion queue with .jsonl session files.

    Reads config["claude_chats"]["roots"] (list of {path, owner} dicts).
    Skips gracefully if the key is absent.

    Each transcript is queued under its claude_jsonl_key() (a claude-session:// key),
    with origin_path set to the real .jsonl path that ingestion reads. Workflow
    journals have no key and are not queued.

    One session can exist as several transcript files (the same uuid in two project
    directories, from copies made when a session moves). Within this scan, all copies
    of one key compete: the newest copy (largest mtime) wins, and a tie goes to the
    lexicographically greatest path. Each key is written once, from its winning copy,
    so origin_path always follows the winner. The other copies are not queued.

    Returns:
        {
          'scanned': N,                  # session keys admitted to the queue this scan
          'existing_paths': set[str],    # every admitted key, for find_deleted()
          'skipped_roots': list[str],    # roots that could not be trusted this scan
        }

        A root is skipped when it is not a directory, or when it is a directory that
        yielded zero transcripts while the queue still holds rows whose origin_path lies
        under it. Callers must not delete rows for a skipped root, because its transcripts
        were not seen.
    """
    roots_cfg = config.get("claude_chats", {}).get("roots", [])
    if not roots_cfg:
        logger.info("No claude_chats roots configured — skipping chat scan")
        return {"scanned": 0, "existing_paths": set(), "skipped_roots": []}

    skipped_roots: list[str] = []
    visited_real_files: set[str] = set()

    # Pass 1: walk every root and keep the best copy of each key. Nothing is written
    # yet, because two copies of one key may sit under different roots.
    # best_by_key maps key -> (mtime, filepath, owner) of the current winner.
    best_by_key: dict[str, tuple[float, str, str]] = {}
    # (root, transcripts seen under it) for the empty-root check in pass 2.
    walked_roots: list[tuple[str, int]] = []

    for entry in roots_cfg:
        root = entry.get("path", "")
        owner = entry.get("owner", "")
        if user_filter and owner != user_filter:
            continue
        if not root or not os.path.isdir(root):
            logger.warning("claude_chats root not accessible, skipping: %s", root)
            skipped_roots.append(root)
            continue

        root_count = 0
        logger.info("Scanning claude chats: %s  (owner=%s)", root, owner)

        for dirpath, _dirnames, filenames in os.walk(root, topdown=True, followlinks=True):
            for filename in filenames:
                if not filename.endswith(".jsonl"):
                    continue
                filepath = os.path.join(dirpath, filename)
                # The queue key is the session identity. None means the file is excluded
                # (a workflow journal): skip it before the dedup, so it never reaches
                # existing_paths and the next sync removes any row it had.
                key = claude_jsonl_key(Path(filepath))
                if key is None:
                    continue
                real_filepath = os.path.realpath(filepath)
                if real_filepath in visited_real_files:
                    continue
                visited_real_files.add(real_filepath)
                try:
                    mtime = os.path.getmtime(filepath)
                except OSError:
                    continue
                root_count += 1

                # Newest mtime wins. Comparing (mtime, path) tuples breaks an exact tie by
                # the greatest path, so the choice is deterministic.
                best = best_by_key.get(key)
                if best is None or (mtime, filepath) > (best[0], best[1]):
                    best_by_key[key] = (mtime, filepath, owner)

        logger.info("  → %d transcript(s) found in %s", root_count, root)
        walked_roots.append((root, root_count))

    # Pass 2: one write per key, from its winning copy.
    existing_paths: set[str] = set()
    for key, (mtime, filepath, owner) in best_by_key.items():
        state_db.discover_or_update(
            key,
            owner,
            mtime,
            origin_path=filepath,
            source_type="claude_session",
            file_type="claude_chat",
            source_id=source_id_for_key(key),
        )
        existing_paths.add(key)

    # Pass 3: the empty-root check. It runs after the writes, so it sees the queue as this
    # scan leaves it. A root that yielded no transcripts while the queue still holds rows
    # whose origin_path lies under it is treated as unreachable, as scan_nfs_roots does
    # for NFS roots. Its rows must not be deleted. The check is read-only.
    for root, root_count in walked_roots:
        if root_count == 0 and state_db.count_origin_with_prefix(os.path.join(root, "")) > 0:
            logger.warning(
                "claude_chats root %s yielded 0 transcripts but the queue has rows under it; "
                "treating as unreachable",
                root,
            )
            skipped_roots.append(root)

    return {
        "scanned": len(best_by_key),
        "existing_paths": existing_paths,
        "skipped_roots": skipped_roots,
    }


def scan_opencode_sessions(
    state_db: StateDB,
    config: dict,
    user_filter: str | None = None,
) -> dict:
    """
    Discover opencode sessions via the opencode HTTP API and update the
    ingestion queue.

    Walks GET /api/session (v2) with cursor pagination until exhausted. This
    endpoint returns every session opencode knows about (project-bound or
    loose, archived or active) — no directory filter, no /project enumeration.

    Reads config["opencode"]:
      - api_url: base URL of the opencode HTTP API
      - owner:   constant owner for all opencode sessions (single-user per host)

    Returns gracefully if the config key is absent. On any HTTP failure mid-
    walk, returns existing_paths=None to preserve already-indexed Qdrant points
    (callers must skip this source in find_deleted()).

    Synthesises file_path as "opencode://{session_id}". Mtime is derived from
    session["time"]["updated"] / 1000.0 (UNIX milliseconds).

    user_filter, if set, narrows the scan to that owner; if the configured
    opencode.owner doesn't match user_filter, the scan is skipped.

    Returns:
        {'scanned': N, 'existing_paths': set[str] | None}
    """
    opencode_cfg = config.get("opencode", {})
    if not opencode_cfg:
        logger.info("No opencode config — skipping opencode scan")
        return {"scanned": 0, "existing_paths": set()}

    api_url = opencode_cfg.get("api_url", "http://localhost:4096")
    owner = opencode_cfg.get("owner", "")
    if not owner:
        logger.info("No opencode owner configured — skipping opencode scan")
        return {"scanned": 0, "existing_paths": set()}

    if user_filter and user_filter != owner:
        logger.info(
            "Opencode owner %r doesn't match --user %r — skipping opencode scan",
            owner, user_filter,
        )
        return {"scanned": 0, "existing_paths": set()}

    # OpenCode 2.x requires Basic auth. A missing password is a configuration
    # error: log it loudly and treat the source as unreachable, so existing
    # opencode points are preserved (see utils/rag_sync_nfs.py).
    try:
        password = load_password(opencode_cfg.get("password_env_file"))
    except RuntimeError as exc:
        logger.error("OpenCode scan skipped: %s", exc)
        return {"scanned": 0, "existing_paths": None}

    scanned = 0
    existing_paths: set[str] = set()
    cursor: str | None = None
    page_limit = 100

    # Walk /api/session with cursor pagination. Cursor is opaque; walk until next is empty.
    # Cap pages to avoid an unbounded loop if the API returns a self-referential cursor.
    for _page_index in range(1000):
        params: dict[str, str] = {"limit": str(page_limit)}
        if cursor:
            params["cursor"] = cursor

        try:
            payload = get_json(api_url, "/api/session", password, params, timeout=10)
        except urllib.error.HTTPError as exc:
            # 401/403 means the password is wrong — a configuration error, not
            # a transient outage, so it is logged as ERROR.
            level = logging.ERROR if exc.code in (401, 403) else logging.WARNING
            logger.log(
                level,
                "opencode /api/session at %s answered HTTP %d (%s) — check "
                "opencode.password_env_file; skipping opencode scan, existing "
                "opencode Qdrant points are preserved",
                api_url, exc.code, exc.reason,
            )
            return {"scanned": scanned, "existing_paths": None}
        except Exception as exc:
            logger.warning(
                "opencode /api/session unreachable at %s: %s — skipping opencode scan, "
                "existing opencode Qdrant points are preserved",
                api_url, exc,
            )
            return {"scanned": scanned, "existing_paths": None}

        items = payload.get("data", []) if isinstance(payload, dict) else []
        for session in items:
            session_id = session.get("id", "")
            if not session_id:
                continue

            path = f"opencode://{session_id}"
            time_info = session.get("time", {})
            updated_ms = time_info.get("updated", 0)
            mtime = updated_ms / 1000.0

            state_db.discover_or_update(
                path,
                owner,
                mtime,
                source_type="opencode_session",
                file_type="opencode",
                source_id=source_id_for_key(path),
            )
            existing_paths.add(path)
            scanned += 1

        cursor_info = payload.get("cursor", {}) if isinstance(payload, dict) else {}
        next_cursor = cursor_info.get("next") if isinstance(cursor_info, dict) else None
        if not next_cursor or not items:
            break
        cursor = next_cursor
    else:
        logger.warning(
            "opencode /api/session pagination exceeded 1000 pages — stopping; "
            "this likely indicates a self-referential cursor from the API"
        )

    logger.info("  → %d opencode session(s) discovered", scanned)

    return {"scanned": scanned, "existing_paths": existing_paths}
