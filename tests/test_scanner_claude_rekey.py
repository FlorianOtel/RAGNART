"""
Tests for the Claude Code re-key in ragnart/scanner.py.

Covers the key derivation (claude_jsonl_key), the `.claude` directory lookup
(claude_dot_claude_dir), and scan_claude_chats over a real tmp_path tree with a real
StateDB. No Qdrant, OpenCode or live NFS is contacted.
"""

from __future__ import annotations

import os
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


# ----------------------------------------------------------------------------
# scan_claude_chats: several copies of one session (duplicate key within a scan)
# ----------------------------------------------------------------------------


def _write_with_mtime(path: Path, mtime: float) -> str:
    """Write a transcript and pin its mtime, so the copy's age is known."""
    written = _write(path)
    os.utime(written, (mtime, mtime))
    return written


def test_scan_claude_chats_newest_copy_wins_and_origin_follows(tmp_path) -> None:
    root = tmp_path / "projects"
    older = _write_with_mtime(root / "-mnt-a" / f"{UUID}.jsonl", 1000.0)
    newer = _write_with_mtime(root / "-mnt-b" / f"{UUID}.jsonl", 2000.0)

    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        config = {"claude_chats": {"roots": [{"path": str(root), "owner": OWNER}]}}
        result = scan_claude_chats(state_db, config)

        # One key, one admitted row, from the newer copy.
        assert result["scanned"] == 1
        assert result["existing_paths"] == {f"claude-session://{UUID}"}
        row = state_db._conn.execute(
            "SELECT origin_path, last_modified FROM ingestion_queue WHERE file_path = ?",
            (f"claude-session://{UUID}",),
        ).fetchone()
        assert row["origin_path"] == newer
        assert row["last_modified"] == 2000.0
        assert row["origin_path"] != older
    finally:
        state_db.close()


def test_scan_claude_chats_newest_copy_wins_across_two_roots(tmp_path) -> None:
    """The copies sit under different configured roots. The newer one still wins."""
    root_a = tmp_path / "home-a" / "projects"
    root_b = tmp_path / "home-b" / "projects"
    _write_with_mtime(root_a / "-mnt-a" / f"{UUID}.jsonl", 1000.0)
    newer = _write_with_mtime(root_b / "-mnt-b" / f"{UUID}.jsonl", 5000.0)

    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        config = {"claude_chats": {"roots": [
            {"path": str(root_a), "owner": OWNER},
            {"path": str(root_b), "owner": OWNER},
        ]}}
        result = scan_claude_chats(state_db, config)

        assert result["scanned"] == 1
        assert result["skipped_roots"] == []
        assert state_db.count_origin_with_prefix(str(root_b) + "/") == 1
        assert state_db.count_origin_with_prefix(str(root_a) + "/") == 0
        row = state_db._conn.execute(
            "SELECT origin_path FROM ingestion_queue WHERE file_path = ?",
            (f"claude-session://{UUID}",),
        ).fetchone()
        assert row["origin_path"] == newer
    finally:
        state_db.close()


def test_scan_claude_chats_equal_mtime_goes_to_greatest_path(tmp_path) -> None:
    root = tmp_path / "projects"
    _write_with_mtime(root / "-mnt-a" / f"{UUID}.jsonl", 3000.0)
    greater = _write_with_mtime(root / "-mnt-z" / f"{UUID}.jsonl", 3000.0)

    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        config = {"claude_chats": {"roots": [{"path": str(root), "owner": OWNER}]}}
        scan_claude_chats(state_db, config)

        row = state_db._conn.execute(
            "SELECT origin_path FROM ingestion_queue WHERE file_path = ?",
            (f"claude-session://{UUID}",),
        ).fetchone()
        assert row["origin_path"] == greater
    finally:
        state_db.close()


def test_rescan_after_newer_copy_appears_requeues_and_follows_it(tmp_path) -> None:
    """A completed row is re-queued when a different copy of its key becomes the newest."""
    root = tmp_path / "projects"
    first = _write_with_mtime(root / "-mnt-a" / f"{UUID}.jsonl", 1000.0)
    key = f"claude-session://{UUID}"

    state_db = StateDB(str(tmp_path / "state" / "rag_state.db"))
    try:
        config = {"claude_chats": {"roots": [{"path": str(root), "owner": OWNER}]}}
        scan_claude_chats(state_db, config)
        state_db.mark_completed(key, chunk_count=1, completed_at="2026-01-01T00:00:00+00:00")

        # A newer copy appears in another project directory.
        second = _write_with_mtime(root / "-mnt-b" / f"{UUID}.jsonl", 2000.0)
        scan_claude_chats(state_db, config)

        row = state_db._conn.execute(
            "SELECT status, origin_path, last_modified FROM ingestion_queue WHERE file_path = ?",
            (key,),
        ).fetchone()
        assert row["origin_path"] == second
        assert row["origin_path"] != first
        assert row["status"] == "pending"
        assert row["last_modified"] == 2000.0
    finally:
        state_db.close()
