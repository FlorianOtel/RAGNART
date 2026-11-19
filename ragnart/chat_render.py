"""
Rendering of Claude Code and OpenCode sessions into searchable text.

Moved here verbatim from ragnart/ingest.py (v0.3 session B1) so that the ingestion side
can render chat sessions without importing docling, qdrant_client or the embedder.
ragnart/ingest.py imports the two parsers back, so existing callers keep working.

Each parser returns (text, metadata, launch_dir). Nothing in this module writes to
SQLite or Qdrant. Its imports are limited to the light modules: opencode_client
(HTTP and password reads), scanner (the .claude directory lookup) and schema (field names).
"""

from __future__ import annotations

import functools
import json
import logging
import re
from datetime import datetime
from pathlib import Path

from .opencode_client import get_json, load_password
from .scanner import claude_dot_claude_dir
from .schema import FIELD_PROJECT, FIELD_SESSION_ID, FIELD_SESSION_TITLE

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Claude Code session titles
# ---------------------------------------------------------------------------

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


@functools.lru_cache(maxsize=8)
def _build_title_map(dot_claude_dir: Path) -> dict[str, str]:
    """Walk ~/.claude/chats/ symlinks → {realpath: human_title}.
    Excludes UUID-named symlinks (auto-created by Claude Code, not user-chosen).
    Returns {} if chats dir absent or inaccessible.
    """
    chats_dir = dot_claude_dir / "chats"
    if not chats_dir.is_dir():
        return {}
    title_map: dict[str, str] = {}
    try:
        for category in chats_dir.iterdir():
            if not category.is_dir():
                continue
            for f in category.iterdir():
                if f.suffix != ".jsonl" or not f.is_symlink():
                    continue
                if _UUID_RE.match(f.stem):
                    continue
                try:
                    title_map[str(f.resolve())] = f.stem
                except OSError:
                    continue
    except OSError as exc:
        logger.warning("_build_title_map: cannot scan %s: %s", chats_dir, exc)
    return title_map


def _synthesize_title(path: Path, max_chars: int = 60) -> str:
    """Derive a title from the first non-empty, non-command user message.
    Falls back to path.stem (UUID) on any failure.
    """
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return path.stem
    for raw in lines:
        try:
            entry = json.loads(raw.strip())
        except (json.JSONDecodeError, ValueError):
            continue
        if entry.get("type") != "user" or entry.get("isMeta"):
            continue
        content = (entry.get("message") or {}).get("content", "")
        if isinstance(content, str):
            text = content.strip()
        elif isinstance(content, list):
            text = " ".join(
                b.get("text", "").strip()
                for b in content
                if isinstance(b, dict) and b.get("type") == "text"
            )
        else:
            text = ""
        if not text or text.startswith("<"):
            continue
        title = text.replace("\n", " ").strip()[:max_chars]
        if len(text) > max_chars:
            title += "..."
        return title
    return path.stem


# ---------------------------------------------------------------------------
# Claude Code session transcripts
# ---------------------------------------------------------------------------


def _parse_claude_chat(path: Path) -> tuple[str, dict, str | None]:
    """
    Extract user/assistant text turns from a Claude Code session JSONL.

    Returns (text, metadata, launch_dir):
      - metadata contains session_id, project and session_title for storage as
        Qdrant payload fields.
      - launch_dir is the first non-empty `cwd` in the transcript, verbatim (never
        normalised), or None. It goes to the SQLite `launch_dir` column only, never
        into the Qdrant payload.

    Skips: meta entries, tool_use blocks, tool_result blocks, thinking blocks,
    XML-tagged command strings, and file-history-snapshot entries.
    """
    import json as _json

    session_id = ""
    cwd = ""
    timestamp = ""
    model = ""
    git_branch = ""
    version = ""

    turns: list[str] = []

    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return "", {}, None

    for raw_line in lines:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            entry = _json.loads(raw_line)
        except _json.JSONDecodeError:
            continue

        entry_type = entry.get("type", "")

        # Collect session metadata from any entry
        if not session_id:
            session_id = entry.get("sessionId", "")
        if not cwd:
            cwd = entry.get("cwd", "")
        if not git_branch:
            git_branch = entry.get("gitBranch", "")
        if not version:
            version = entry.get("version", "")

        if entry_type == "user":
            if entry.get("isMeta"):
                continue
            msg = entry.get("message", {})
            content = msg.get("content", "")
            text = ""
            if isinstance(content, str):
                c = content.strip()
                if c and not c.startswith("<"):
                    text = c
            elif isinstance(content, list):
                parts = []
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        t = block.get("text", "").strip()
                        if t:
                            parts.append(t)
                text = "\n".join(parts)
            if text:
                if not timestamp:
                    timestamp = entry.get("timestamp", "")
                turns.append(f"**User**: {text}")

        elif entry_type == "assistant":
            msg = entry.get("message", {})
            if not model:
                model = msg.get("model", "")
            content = msg.get("content", [])
            if isinstance(content, list):
                parts = []
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        t = block.get("text", "").strip()
                        if t:
                            parts.append(t)
                text = "\n".join(parts)
                if text:
                    turns.append(f"**Claude**: {text}")

    if not turns:
        return "", {}, None

    project = Path(cwd).name if cwd else "unknown"

    header = (
        f"# Claude Code Session\n\n"
        f"Session: {session_id}\n"
        f"Project: {project}  ({cwd})\n"
        f"Date: {timestamp}\n"
        f"Model: {model}\n"
        f"Branch: {git_branch}\n"
        f"Version: {version}\n\n"
        f"---\n\n"
    )

    body = "\n\n".join(turns)
    text = header + body

    # --- session_title derivation ---
    if "projects" not in path.parts:
        # Not in the Claude Code layout (e.g. a plain notes.jsonl on the NAS). There is no
        # `.claude` directory or parent session to look up, so the synthesized title is the
        # only title. This is what such a file got before the Claude Code re-key.
        session_title = _synthesize_title(path)
    else:
        # The .claude directory is found by anchoring on the `projects` component, so the
        # same code handles main sessions, direct subagents and workflow-depth subagents.
        dot_claude_dir = claude_dot_claude_dir(path)
        if "subagents" in path.parts:
            # Subagent path: .../projects/<mangled>/<uuid>/subagents/[workflows/wf_X/]agent-xxx.jsonl
            # The parent session is <mangled>/<uuid>.jsonl, whatever the depth below <uuid>.
            idx = path.parts.index("subagents")
            session_uuid = path.parts[idx - 1]
            parent_jsonl = Path(*path.parts[: idx - 1]) / f"{session_uuid}.jsonl"
            title_map = _build_title_map(dot_claude_dir)
            parent_title = title_map.get(str(parent_jsonl.resolve())) or _synthesize_title(parent_jsonl)
            session_title = f"{parent_title} [subagent]"
        else:
            # Main session path: .../projects/<mangled>/<uuid>.jsonl
            title_map = _build_title_map(dot_claude_dir)
            session_title = title_map.get(str(path.resolve())) or _synthesize_title(path)

    metadata = {
        FIELD_SESSION_ID:    session_id,
        FIELD_PROJECT:       project,
        FIELD_SESSION_TITLE: session_title,
    }
    return text, metadata, cwd or None


# ---------------------------------------------------------------------------
# OpenCode sessions (fetched over the OpenCode HTTP API)
# ---------------------------------------------------------------------------


def _parse_opencode_session(
    path: str, oc_api_url: str, password_env_file: str | None = None
) -> tuple[str, dict, str | None]:
    """
    Fetch an OpenCode 2.x session via its HTTP API and extract user/assistant text turns.

    path is the synthetic key "opencode://{session_id}".
    oc_api_url is the opencode API base URL (e.g. "http://192.168.1.95:4096").
    password_env_file names the file holding OPENCODE_SERVER_PASSWORD
    (opencode.password_env_file; see ragnart/opencode_client.py).

    Routes (OpenCode 2.x, Basic auth):
      GET /api/session/{id}          -> {"data": {title, time, location.directory, ...}}
      GET /api/session/{id}/message  -> {"data": [...], "cursor": {"next", ...}}
        newest-first by default, so we ask for order=asc; cursor.next is set while
        a page is non-empty, so the walk stops at the first empty page.

    Returns (text, metadata, launch_dir) with session_id and session_title. No `project`
    field — opencode sessions have no path-derived project (the launch
    directory is cwd-at-launch metadata, not topic classification). The
    directory is preserved inside the searchable header body as forensic context.
    launch_dir is location.directory verbatim, or None when the session has none.
    It goes to the SQLite `launch_dir` column only, never into the Qdrant payload.

    HTTP, auth and non-JSON errors are RAISED, so the item is marked failed and
    retried; only a session with no user/assistant text returns ("", {}, None).
    """
    # Parse session_id from synthetic path
    if not path.startswith("opencode://"):
        return "", {}, None
    session_id = path[len("opencode://"):]
    if not session_id:
        return "", {}, None

    password = load_password(password_env_file)

    # Session metadata (title, created timestamp, launch directory)
    session = get_json(oc_api_url, f"/api/session/{session_id}", password).get("data", {})
    session_title = (session.get("title") or "").strip()
    launched_from = (session.get("location") or {}).get("directory", "") or ""
    created_ms = (session.get("time") or {}).get("created", 0)
    created_timestamp = (
        datetime.fromtimestamp(created_ms / 1000.0).isoformat() if created_ms else ""
    )

    # All messages, oldest first, walked page by page. The page cap only guards
    # against a self-referential cursor (200 x 200 = 40,000 messages).
    messages: list[dict] = []
    params: dict[str, str] = {"order": "asc", "limit": "200"}
    for _page_index in range(200):
        page = get_json(oc_api_url, f"/api/session/{session_id}/message", password, params)
        items = page.get("data", [])
        if not items:
            break
        messages.extend(items)
        next_cursor = (page.get("cursor") or {}).get("next")
        if not next_cursor:
            break
        # The cursor carries the order; the server rejects cursor + order together.
        params = {"cursor": next_cursor, "limit": "200"}

    turns: list[str] = []

    # Extract text turns. 2.x messages are typed by a top-level "type":
    #   user      -> "text" (the prompt as typed)
    #   assistant -> "content": [{type: text|reasoning|tool, ...}]; keep only "text"
    # Everything else (synthetic, system, skill, shell, compaction, idle,
    # agent/model/location switches) is harness bookkeeping, not conversation.
    first_user_text = ""
    for message in messages:
        message_type = message.get("type", "")

        if message_type == "user":
            combined_text = (message.get("text") or "").strip()
            if combined_text and not combined_text.startswith("/"):  # Skip slash commands
                turns.append(f"**User**: {combined_text}")
                if not first_user_text:
                    first_user_text = combined_text

        elif message_type == "assistant":
            text_parts = [
                (part.get("text") or "").strip()
                for part in message.get("content") or []
                if part.get("type") == "text"
            ]
            combined_text = "\n".join(t for t in text_parts if t)
            if combined_text:
                turns.append(f"**Assistant**: {combined_text}")

    if not turns:
        return "", {}, None

    # Synthesise session_title if absent or empty
    if not session_title:
        if first_user_text:
            # Use first non-empty user text (up to 60 chars), stripping leading <
            text = first_user_text.lstrip("<").replace("\n", " ").strip()[:60]
            session_title = text + "..." if len(first_user_text) > 60 else text
        else:
            session_title = session_id  # Fallback to session_id

    # Build header block. "Launched from" is forensic information about cwd
    # at session start — NOT a project classification (opencode sessions have
    # no genuine path-derived project; see schema.py FIELD_PROJECT comment).
    header = (
        f"# Opencode Session\n\n"
        f"Session: {session_id}\n"
        f"Launched from: {launched_from}\n"
        f"Date: {created_timestamp}\n"
        f"Title: {session_title}\n\n"
        f"---\n\n"
    )

    body = "\n\n".join(turns)
    text = header + body

    # No FIELD_PROJECT — opencode points intentionally omit the project field.
    metadata = {
        FIELD_SESSION_ID:    session_id,
        FIELD_SESSION_TITLE: session_title,
    }
    return text, metadata, launched_from or None
