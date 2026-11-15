"""
Tests for the Claude Code re-key in ragnart/scanner.py.

Covers the key derivation (claude_jsonl_key), the `.claude` directory lookup
(claude_dot_claude_dir), and scan_claude_chats over a real tmp_path tree with a real
StateDB. No Qdrant, OpenCode or live NFS is contacted.
"""

from __future__ import annotations

from pathlib import Path

from ragnart.scanner import claude_dot_claude_dir, claude_jsonl_key, scan_claude_chats
from ragnart.state import StateDB

OWNER = "tester"
UUID = "47cfe354-39f7-45a0-993e-1df37c4fb88e"


# ----------------------------------------------------------------------------
# claude_jsonl_key
# ----------------------------------------------------------------------------


def test_claude_jsonl_key_main_session() -> None:
    path = Path(f"/h/.claude/projects/-mnt-x/{UUID}.jsonl")
    assert claude_jsonl_key(path) == f"claude-session://{UUID}"


def test_claude_jsonl_key_direct_subagent() -> None:
    path = Path(f"/h/.claude/projects/-mnt-x/{UUID}/subagents/agent-A1.jsonl")
    assert claude_jsonl_key(path) == f"claude-session://{UUID}/agent-A1"


def test_claude_jsonl_key_workflow_agent() -> None:
    path = Path(f"/h/.claude/projects/-mnt-x/{UUID}/subagents/workflows/wf_X/agent-B2.jsonl")
    assert claude_jsonl_key(path) == f"claude-session://{UUID}/agent-B2"


def test_claude_jsonl_key_workflow_journal_excluded() -> None:
    path = Path(f"/h/.claude/projects/-mnt-x/{UUID}/subagents/workflows/wf_X/journal.jsonl")
    assert claude_jsonl_key(path) is None


# ----------------------------------------------------------------------------
# claude_dot_claude_dir
# ----------------------------------------------------------------------------


def test_claude_dot_claude_dir_main_and_workflow_depth() -> None:
    main = Path(f"/h/.claude/projects/-mnt-x/{UUID}.jsonl")
    workflow = Path(f"/h/.claude/projects/-mnt-x/{UUID}/subagents/workflows/wf_X/agent-B2.jsonl")

    assert claude_dot_claude_dir(main) == Path("/h/.claude")
    assert claude_dot_claude_dir(workflow) == Path("/h/.claude")


def test_claude_dot_claude_dir_ignores_earlier_projects_components() -> None:
    """A `projects` directory above `.claude` must not be mistaken for the anchor."""
    path = Path(f"/home/projects/work/.claude/projects/-mnt-x/{UUID}.jsonl")

    assert claude_dot_claude_dir(path) == Path("/home/projects/work/.claude")


# ----------------------------------------------------------------------------
# scan_claude_chats (integration, real tmp_path tree and StateDB)
# ----------------------------------------------------------------------------


def _write(path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"type": "user"}\n', encoding="utf-8")
    return str(path)


def test_scan_claude_chats_excludes_journal_and_rekeys(tmp_path) -> None:
    root = tmp_path / "projects"
    mangled = root / "-mnt-x"
    main = _write(mangled / f"{UUID}.jsonl")
    direct = _write(mangled / UUID / "subagents" / "agent-A.jsonl")
    _write(mangled / UUID / "subagents" / "workflows" / "wf_1" / "journal.jsonl")
    workflow = _write(mangled / UUID / "subagents" / "workflows" / "wf_1" / "agent-B.jsonl")

    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        config = {"claude_chats": {"roots": [{"path": str(root), "owner": OWNER}]}}
        result = scan_claude_chats(state_db, config)

        assert result["existing_paths"] == {
            f"claude-session://{UUID}",
            f"claude-session://{UUID}/agent-A",
            f"claude-session://{UUID}/agent-B",
        }
        assert result["skipped_roots"] == []

        rows = {
            row["file_path"]: row["origin_path"]
            for row in state_db._conn.execute("SELECT file_path, origin_path FROM ingestion_queue").fetchall()
        }
        assert rows == {
            f"claude-session://{UUID}": main,
            f"claude-session://{UUID}/agent-A": direct,
            f"claude-session://{UUID}/agent-B": workflow,
        }
    finally:
        state_db.close()


def test_scan_claude_chats_reports_skipped_root(tmp_path) -> None:
    missing = tmp_path / "never-existed"

    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        config = {"claude_chats": {"roots": [{"path": str(missing), "owner": OWNER}]}}
        result = scan_claude_chats(state_db, config)

        assert result["skipped_roots"] == [str(missing)]
        assert result["scanned"] == 0
        assert result["existing_paths"] == set()
    finally:
        state_db.close()


# ----------------------------------------------------------------------------
# Empty claude roots (skipped_roots guard)
# ----------------------------------------------------------------------------


def test_empty_claude_root_with_prior_rows_is_skipped(tmp_path) -> None:
    """A root that yielded transcripts last scan but none now is treated as unreachable."""
    root = tmp_path / "projects"
    transcript = _write(root / "-mnt-x" / f"{UUID}.jsonl")

    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        config = {"claude_chats": {"roots": [{"path": str(root), "owner": OWNER}]}}
        first = scan_claude_chats(state_db, config)
        assert first["skipped_roots"] == []
        assert state_db.count_origin_with_prefix(str(root) + "/") == 1

        # The root still exists but no transcript is left under it.
        Path(transcript).unlink()
        second = scan_claude_chats(state_db, config)

        assert second["scanned"] == 0
        assert second["skipped_roots"] == [str(root)]
    finally:
        state_db.close()


def test_empty_claude_root_with_no_rows_is_not_skipped(tmp_path) -> None:
    """An empty directory the queue knows nothing about is a legitimate empty source."""
    root = tmp_path / "projects"
    root.mkdir()

    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        config = {"claude_chats": {"roots": [{"path": str(root), "owner": OWNER}]}}
        result = scan_claude_chats(state_db, config)

        assert result["skipped_roots"] == []
    finally:
        state_db.close()
