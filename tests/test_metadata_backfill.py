"""
Tests for utils/rag_metadata_backfill.py.

Everything runs on a temporary SQLite file (a real StateDB) and real small files under
tmp_path. The Qdrant client is a MagicMock and the OpenCode API is a patched function, so
no Qdrant server or OpenCode server is contacted and the production queue is never opened.
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from qdrant_client.models import FilterSelector

from ragnart.state import StateDB

REPO_ROOT = Path(__file__).resolve().parent.parent

OWNER = "florian"
COMPLETED_AT = "2026-01-02T03:04:05+00:00"
LAST_MODIFIED = 1_700_000_000.0
MODIFIED_AT = datetime.fromtimestamp(LAST_MODIFIED, tz=timezone.utc).isoformat()
NOW_ISO = "2026-11-15T12:00:00+00:00"

SESSION_UUID = "47cfe354-0000-4000-8000-000000000001"
OC_KEY = "opencode://ses_abc123"


@pytest.fixture
def backfill(monkeypatch):
    """Load utils/rag_metadata_backfill.py as a module. It is a script, not a package."""
    name = "rag_metadata_backfill"
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "utils" / "rag_metadata_backfill.py")
    module = importlib.util.module_from_spec(spec)
    # Register before executing, as the other utils script tests do.
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def state_db(tmp_path):
    db = StateDB(str(tmp_path / "rag_state.db"))
    yield db
    db.close()


@pytest.fixture
def claude_root(tmp_path):
    """A claude_chats root: <tmp>/.claude/projects, holding one mangled project directory."""
    root = tmp_path / ".claude" / "projects"
    (root / "-mnt-x").mkdir(parents=True)
    return root


@pytest.fixture
def config(claude_root):
    return {
        "claude_chats": {"roots": [{"path": str(claude_root), "owner": OWNER}]},
        "opencode": {"api_url": "http://oc.invalid:4096", "owner": OWNER},
    }


@pytest.fixture
def qdrant():
    client = MagicMock()
    client.count.return_value = SimpleNamespace(count=3)
    return client


@pytest.fixture
def no_opencode_api(backfill, monkeypatch):
    """Fail the test if anything calls the OpenCode API."""
    def _forbidden(*args, **kwargs):
        raise AssertionError("OpenCode API must not be called")
    monkeypatch.setattr(backfill, "get_json", _forbidden)


def add_completed(state_db: StateDB, key: str) -> None:
    """Queue one row and mark it completed, leaving pipeline_version NULL (not migrated)."""
    state_db.discover_or_update(key, OWNER, LAST_MODIFIED)
    state_db.mark_completed(key, chunk_count=2, completed_at=COMPLETED_AT)


def row_of(state_db: StateDB, key: str) -> dict:
    row = state_db._conn.execute("SELECT * FROM ingestion_queue WHERE file_path = ?", (key,)).fetchone()
    return dict(row) if row is not None else None


def write_claude_transcript(claude_root: Path) -> Path:
    """A main-session transcript whose first line has no cwd and whose second line has one."""
    path = claude_root / "-mnt-x" / f"{SESSION_UUID}.jsonl"
    path.write_text(
        '{"type": "summary", "summary": "no cwd here"}\n'
        '{"type": "user", "cwd": "/home/florian/Gin-AI/projects/RAGNART", "message": "hi"}\n'
        '{"type": "assistant", "cwd": "/somewhere/else"}\n',
        encoding="utf-8",
    )
    return path


def journal_path(claude_root: Path) -> Path:
    """Path shape of a workflow orchestration journal. The file need not exist: classify() does not read it."""
    return (claude_root / "-mnt-x" / SESSION_UUID / "subagents" / "workflows"
            / "wf_08ea07fa-b92" / "journal.jsonl")


# ---------------------------------------------------------------------------
# classify
# ---------------------------------------------------------------------------

def test_classify_distinguishes_all_four_kinds(backfill, claude_root):
    roots = [str(claude_root)]
    main_session = str(claude_root / "-mnt-x" / f"{SESSION_UUID}.jsonl")

    assert backfill.classify(OC_KEY, roots) == "opencode"
    assert backfill.classify(main_session, roots) == "claude"
    assert backfill.classify(str(journal_path(claude_root)), roots) == "claude_journal_skip"
    assert backfill.classify("/mnt/nfs/Florian/docs/notes.md", roots) == "nas"


def test_classify_does_not_match_a_sibling_root_with_a_shared_prefix(backfill, claude_root):
    # A path under "<root>2" is NOT under the claude root "<root>", so it is a NAS file.
    sibling = str(claude_root) + "2/-mnt-x/" + f"{SESSION_UUID}.jsonl"
    assert backfill.classify(sibling, [str(claude_root)]) == "nas"


# ---------------------------------------------------------------------------
# candidates
# ---------------------------------------------------------------------------

def test_prefix_filters_candidates(backfill, state_db):
    add_completed(state_db, "/pilot/a.md")
    add_completed(state_db, "/other/b.md")

    rows = backfill.candidates(state_db, prefix="/pilot/")

    assert [row["file_path"] for row in rows] == ["/pilot/a.md"]


def test_candidates_skip_rows_already_migrated_or_not_completed(backfill, state_db):
    add_completed(state_db, "/data/done.md")
    state_db.discover_or_update("/data/pending.md", OWNER, LAST_MODIFIED)
    state_db._conn.execute("UPDATE ingestion_queue SET pipeline_version = 'p1' WHERE file_path = '/data/done.md'")
    state_db._conn.commit()

    assert backfill.candidates(state_db) == []


# ---------------------------------------------------------------------------
# dry-run
# ---------------------------------------------------------------------------

def test_dry_run_reports_counts_and_makes_no_writes(backfill, state_db, qdrant, config, claude_root, no_opencode_api):
    nas_file = claude_root.parent.parent / "notes.md"
    nas_file.write_text("some notes", encoding="utf-8")
    transcript = write_claude_transcript(claude_root)
    add_completed(state_db, str(nas_file))
    add_completed(state_db, str(transcript))
    add_completed(state_db, str(journal_path(claude_root)))
    add_completed(state_db, OC_KEY)
    before = {key: row_of(state_db, key) for key in (str(nas_file), str(transcript), str(journal_path(claude_root)), OC_KEY)}

    summary = backfill.run_backfill(config, state_db, qdrant, None, True, 50)

    assert summary["kinds"]["nas"] == 1
    assert summary["kinds"]["claude"] == 1
    assert summary["kinds"]["claude_journal_skip"] == 1
    assert summary["kinds"]["opencode"] == 1
    assert summary["bytes"] == nas_file.stat().st_size + transcript.stat().st_size
    qdrant.set_payload.assert_not_called()
    qdrant.count.assert_not_called()
    for key, row in before.items():
        assert row_of(state_db, key) == row
        assert row["pipeline_version"] is None


def test_dry_run_lists_a_vanished_nas_file_as_unreadable(backfill, state_db, config):
    add_completed(state_db, "/gone/vanished.md")

    summary = backfill.run_backfill(config, state_db, None, None, True, 50)

    assert [path for path, _ in summary["unreadable"]] == ["/gone/vanished.md"]


# ---------------------------------------------------------------------------
# apply: per row kind
# ---------------------------------------------------------------------------

def test_nas_row_is_hashed_and_payload_set_in_place(backfill, state_db, qdrant, config, tmp_path):
    nas_file = tmp_path / "report.md"
    nas_file.write_text("hello backfill\n", encoding="utf-8")
    key = str(nas_file)
    add_completed(state_db, key)

    summary = backfill.run_backfill(config, state_db, qdrant, None, False, 50)

    assert summary["processed"] == 1
    assert summary["errors"] == []
    qdrant.set_payload.assert_called_once()
    kwargs = qdrant.set_payload.call_args.kwargs
    # The filter targets the unchanged source_path, because a NAS file keeps its key.
    assert kwargs["points"].must[0].match.value == key
    assert kwargs["payload"]["project_id"] == "global"
    assert kwargs["payload"]["source_type"] == "file"
    assert kwargs["payload"]["file_type"] == "md"
    assert kwargs["payload"]["modified_at"] == MODIFIED_AT
    assert kwargs["payload"]["ingested_at"] == COMPLETED_AT
    expected_md5 = hashlib.md5(nas_file.read_bytes()).hexdigest()
    assert kwargs["payload"]["content_hash"] == expected_md5

    row = row_of(state_db, key)
    assert row["pipeline_version"] == "p1"
    assert row["content_hash"] == expected_md5
    assert row["size_bytes"] == nas_file.stat().st_size
    assert row["origin_path"] == key
    assert row["chunk_count"] == 3
    assert row["launch_dir"] is None


def test_claude_row_is_renamed_in_both_systems(backfill, state_db, qdrant, config, claude_root):
    transcript = write_claude_transcript(claude_root)
    old_key = str(transcript)
    new_key = f"claude-session://{SESSION_UUID}"
    add_completed(state_db, old_key)

    summary = backfill.run_backfill(config, state_db, qdrant, None, False, 50)

    assert summary["errors"] == []
    # Qdrant: the filter used the OLD key, and the payload carries the NEW key.
    set_kwargs = qdrant.set_payload.call_args.kwargs
    assert set_kwargs["points"].must[0].match.value == old_key
    assert set_kwargs["payload"]["source_path"] == new_key
    assert set_kwargs["payload"]["source_type"] == "claude_session"
    assert set_kwargs["payload"]["file_type"] == "claude_chat"
    # The count was taken under the NEW key.
    count_kwargs = qdrant.count.call_args.kwargs
    assert count_kwargs["count_filter"].must[0].match.value == new_key

    # SQLite: the row is found under the new key, and nothing is left under the old one.
    assert row_of(state_db, old_key) is None
    row = row_of(state_db, new_key)
    assert row["pipeline_version"] == "p1"
    assert row["origin_path"] == old_key
    assert row["launch_dir"] == "/home/florian/Gin-AI/projects/RAGNART"
    assert row["content_hash"] == hashlib.md5(transcript.read_bytes()).hexdigest()
    assert row["file_type"] == "claude_chat"


def test_claude_row_without_any_cwd_gets_null_launch_dir(backfill, state_db, qdrant, config, claude_root):
    transcript = claude_root / "-mnt-x" / f"{SESSION_UUID}.jsonl"
    transcript.write_text('{"type": "summary"}\n', encoding="utf-8")
    add_completed(state_db, str(transcript))

    backfill.run_backfill(config, state_db, qdrant, None, False, 50)

    assert row_of(state_db, f"claude-session://{SESSION_UUID}")["launch_dir"] is None


def test_workflow_journal_row_is_left_untouched(backfill, state_db, qdrant, config, claude_root):
    key = str(journal_path(claude_root))
    add_completed(state_db, key)
    before = row_of(state_db, key)

    summary = backfill.run_backfill(config, state_db, qdrant, None, False, 50)

    assert summary["skipped_journal"] == 1
    assert summary["processed"] == 0
    qdrant.set_payload.assert_not_called()
    assert row_of(state_db, key) == before
    assert before["pipeline_version"] is None


def test_opencode_row_gets_launch_dir_verbatim(backfill, state_db, qdrant, config, monkeypatch):
    add_completed(state_db, OC_KEY)
    verbatim_dir = "/Weird//Dir/ with spaces/"
    calls = []

    def fake_get_json(api_url, path, password, params=None, timeout=30):
        calls.append((api_url, path, password))
        return {"data": {"location": {"directory": verbatim_dir}}}

    monkeypatch.setattr(backfill, "get_json", fake_get_json)

    summary = backfill.run_backfill(config, state_db, qdrant, None, False, 50, oc_password="pw")

    assert summary["errors"] == []
    assert calls == [("http://oc.invalid:4096", "/api/session/ses_abc123", "pw")]
    set_kwargs = qdrant.set_payload.call_args.kwargs
    assert set_kwargs["points"].must[0].match.value == OC_KEY
    assert set_kwargs["payload"]["source_type"] == "opencode_session"
    assert set_kwargs["payload"]["file_type"] == "opencode"
    assert "content_hash" not in set_kwargs["payload"]
    row = row_of(state_db, OC_KEY)
    assert row["launch_dir"] == verbatim_dir
    assert row["pipeline_version"] == "p1"
    assert row["content_hash"] is None
    assert row["origin_path"] is None


# ---------------------------------------------------------------------------
# apply: failures, recovery and idempotency
# ---------------------------------------------------------------------------

def test_vanished_nas_file_is_counted_and_the_run_goes_on(backfill, state_db, qdrant, config, tmp_path):
    good = tmp_path / "good.md"
    good.write_text("still here", encoding="utf-8")
    vanished = str(tmp_path / "vanished.md")  # never created
    add_completed(state_db, str(good))
    add_completed(state_db, vanished)

    summary = backfill.run_backfill(config, state_db, qdrant, None, False, 50)

    assert summary["processed"] == 1
    assert len(summary["errors"]) == 1
    assert summary["errors"][0][0] == vanished
    assert "FileNotFoundError" in summary["errors"][0][1]
    # The vanished row stays NULL, so a re-run retries it. Nothing was written for it.
    assert row_of(state_db, vanished)["pipeline_version"] is None
    assert row_of(state_db, str(good))["pipeline_version"] == "p1"
    assert qdrant.set_payload.call_count == 1


def test_crash_between_qdrant_write_and_sqlite_rename_is_recoverable(
    backfill, state_db, qdrant, config, claude_root, monkeypatch
):
    transcript = write_claude_transcript(claude_root)
    old_key = str(transcript)
    new_key = f"claude-session://{SESSION_UUID}"
    add_completed(state_db, old_key)
    row = row_of(state_db, old_key)

    real_commit = backfill._commit_row
    failures = {"left": 1}

    def flaky_commit(state_db_arg, sql, params):
        # Fail the first SQLite step once, after Qdrant has already been written.
        if failures["left"]:
            failures["left"] -= 1
            raise RuntimeError("simulated crash before SQLite commit")
        return real_commit(state_db_arg, sql, params)

    monkeypatch.setattr(backfill, "_commit_row", flaky_commit)

    with pytest.raises(RuntimeError):
        backfill.process_one(row, "claude", qdrant, state_db, NOW_ISO)

    # Qdrant was written, but SQLite still has the row under the old key.
    assert qdrant.set_payload.call_count == 1
    assert row_of(state_db, old_key)["pipeline_version"] is None
    assert row_of(state_db, new_key) is None

    # Re-running the same row completes the migration.
    backfill.process_one(row, "claude", qdrant, state_db, NOW_ISO)

    assert qdrant.set_payload.call_count == 2
    assert row_of(state_db, old_key) is None
    assert row_of(state_db, new_key)["pipeline_version"] == "p1"


def test_rerun_after_full_apply_is_a_no_op(backfill, state_db, qdrant, config, claude_root, tmp_path, monkeypatch):
    monkeypatch.setattr(backfill, "get_json", lambda *a, **k: {"data": {"location": {"directory": "/d"}}})
    nas_file = tmp_path / "doc.md"
    nas_file.write_text("doc", encoding="utf-8")
    add_completed(state_db, str(nas_file))
    add_completed(state_db, str(write_claude_transcript(claude_root)))
    add_completed(state_db, OC_KEY)

    first = backfill.run_backfill(config, state_db, qdrant, None, False, 50, oc_password="pw")
    calls_after_first = qdrant.set_payload.call_count

    assert first["errors"] == []
    assert backfill.candidates(state_db) == []

    second = backfill.run_backfill(config, state_db, qdrant, None, False, 50, oc_password="pw")

    assert second["total"] == 0
    assert qdrant.set_payload.call_count == calls_after_first


# ---------------------------------------------------------------------------
# helpers and checkpoint
# ---------------------------------------------------------------------------

def test_first_cwd_skips_empty_and_non_json_lines(backfill, tmp_path):
    path = tmp_path / "transcript.jsonl"
    path.write_bytes(
        b'{"cwd": ""}\n'
        b"this is not json at all \"cwd\"\n"
        b'{"cwd": "/first/real/dir"}\n'
        b'{"cwd": "/second/dir"}\n'
    )

    size_bytes, md5, first_cwd = backfill._hash_and_first_cwd(str(path))

    assert first_cwd == "/first/real/dir"
    assert size_bytes == path.stat().st_size
    assert md5 == hashlib.md5(path.read_bytes()).hexdigest()


def test_checkpoint_line_written_every_hundred_rows(backfill, state_db, config, tmp_path, claude_root):
    # Journal rows are counted as processed without touching Qdrant, so 100 of them are cheap.
    for index in range(100):
        add_completed(state_db, str(claude_root / "-mnt-x" / SESSION_UUID / "subagents" / "workflows"
                                    / f"wf_{index}" / "journal.jsonl"))
    checkpoint = tmp_path / "checkpoint.txt"

    summary = backfill.run_backfill(
        config, state_db, MagicMock(), None, False, 50, checkpoint_file=str(checkpoint)
    )

    assert summary["skipped_journal"] == 100
    lines = checkpoint.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert lines[0].split("\t")[1] == "100/100"


# ---------------------------------------------------------------------------
# Collisions: two copies of one session uuid, one new-key row already present
# ---------------------------------------------------------------------------

NEW_KEY = f"claude-session://{SESSION_UUID}"


def add_new_key_row(state_db: StateDB, origin: str, mtime: float) -> None:
    """A row already under the new key, as an earlier copy of the session left it: completed and migrated."""
    state_db.discover_or_update(
        NEW_KEY, OWNER, mtime, origin_path=origin, source_type="claude_session", file_type="claude_chat",
    )
    state_db.mark_completed(NEW_KEY, chunk_count=2, completed_at=COMPLETED_AT, pipeline_version="p1")


def write_copy(claude_root: Path, project_dir: str, mtime: float, text: str = '{"type": "user"}\n') -> str:
    """Write a second copy of the session under another project directory, with a fixed mtime."""
    path = claude_root / project_dir / f"{SESSION_UUID}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    os.utime(path, (mtime, mtime))
    return str(path)


def deleted_keys(qdrant) -> list[str]:
    """The source_path value of each delete call, in call order. Also checks wait=True on each."""
    keys = []
    for call in qdrant.delete.call_args_list:
        assert call.kwargs["wait"] is True
        assert isinstance(call.kwargs["points_selector"], FilterSelector)
        keys.append(call.kwargs["points_selector"].filter.must[0].match.value)
    return keys


def test_collision_newer_old_copy_wins(backfill, state_db, qdrant, config, claude_root):
    old = write_claude_transcript(claude_root)
    os.utime(old, (2000.0, 2000.0))
    other = write_copy(claude_root, "-mnt-y", 1000.0)
    add_completed(state_db, str(old))
    add_new_key_row(state_db, other, 1000.0)

    summary = backfill.run_backfill(config, state_db, qdrant, None, False, 50)

    assert summary["errors"] == []
    assert summary["collisions"] == 1
    # Points under new_key (both copies' points) and any under old_key are deleted, before the SQLite commit.
    assert deleted_keys(qdrant) == [NEW_KEY, str(old)]
    qdrant.set_payload.assert_not_called()
    # The old row is gone; the new-key row is pending with the newer file as origin.
    assert row_of(state_db, str(old)) is None
    row = row_of(state_db, NEW_KEY)
    assert row["status"] == "pending"
    assert row["origin_path"] == str(old)
    assert row["last_modified"] == 2000.0
    assert row["pipeline_version"] is None
    assert row["chunk_count"] is None
    assert row["completed_at"] is None
    assert row["retry_count"] == 0


def test_collision_newer_existing_copy_wins(backfill, state_db, qdrant, config, claude_root):
    old = write_claude_transcript(claude_root)
    os.utime(old, (1000.0, 1000.0))
    other = write_copy(claude_root, "-mnt-y", 3000.0)
    add_completed(state_db, str(old))
    add_new_key_row(state_db, other, 3000.0)

    summary = backfill.run_backfill(config, state_db, qdrant, None, False, 50)

    assert summary["collisions"] == 1
    row = row_of(state_db, NEW_KEY)
    assert row["status"] == "pending"
    assert row["origin_path"] == other
    assert row["last_modified"] == 3000.0


def test_collision_identical_copies_with_equal_mtime_pick_greatest_path(backfill, state_db, qdrant, config, claude_root):
    # Same bytes, same mtime: the tie rule is the greatest path, the same rule the scanner uses.
    old = write_claude_transcript(claude_root)
    os.utime(old, (1500.0, 1500.0))
    other = write_copy(claude_root, "-mnt-y", 1500.0, text=Path(old).read_text(encoding="utf-8"))
    add_completed(state_db, str(old))
    add_new_key_row(state_db, other, 1500.0)

    summary = backfill.run_backfill(config, state_db, qdrant, None, False, 50)

    assert summary["errors"] == []
    assert summary["collisions"] == 1
    assert row_of(state_db, NEW_KEY)["origin_path"] == max(str(old), other)
    assert row_of(state_db, str(old)) is None


def test_collision_with_missing_existing_origin_old_copy_wins(backfill, state_db, qdrant, config, claude_root):
    old = write_claude_transcript(claude_root)
    add_completed(state_db, str(old))
    add_new_key_row(state_db, str(claude_root / "-mnt-y" / "gone.jsonl"), 9999.0)

    backfill.run_backfill(config, state_db, qdrant, None, False, 50)

    assert row_of(state_db, NEW_KEY)["origin_path"] == str(old)


def test_collision_with_missing_old_copy_existing_wins(backfill, state_db, qdrant, config, claude_root):
    # The old transcript has vanished, so nothing is read from it. The existing copy wins.
    old_missing = str(claude_root / "-mnt-x" / f"{SESSION_UUID}.jsonl")
    other = write_copy(claude_root, "-mnt-y", 1000.0)
    add_completed(state_db, old_missing)
    add_new_key_row(state_db, other, 1000.0)

    summary = backfill.run_backfill(config, state_db, qdrant, None, False, 50)

    assert summary["errors"] == []
    assert summary["collisions"] == 1
    assert row_of(state_db, old_missing) is None
    assert row_of(state_db, NEW_KEY)["origin_path"] == other


def test_collision_with_both_copies_missing_fails_the_row_and_writes_nothing(
    backfill, state_db, qdrant, config, claude_root
):
    old_missing = str(claude_root / "-mnt-x" / f"{SESSION_UUID}.jsonl")
    add_completed(state_db, old_missing)
    add_new_key_row(state_db, str(claude_root / "-mnt-y" / "gone.jsonl"), 1000.0)
    before = row_of(state_db, NEW_KEY)

    summary = backfill.run_backfill(config, state_db, qdrant, None, False, 50)

    assert summary["collisions"] == 0
    assert len(summary["errors"]) == 1
    qdrant.delete.assert_not_called()
    assert row_of(state_db, old_missing)["pipeline_version"] is None
    assert row_of(state_db, NEW_KEY) == before


def test_collision_rerun_is_a_no_op(backfill, state_db, qdrant, config, claude_root):
    old = write_claude_transcript(claude_root)
    os.utime(old, (2000.0, 2000.0))
    add_completed(state_db, str(old))
    add_new_key_row(state_db, write_copy(claude_root, "-mnt-y", 1000.0), 1000.0)
    backfill.run_backfill(config, state_db, qdrant, None, False, 50)
    after_first = row_of(state_db, NEW_KEY)
    deletes_after_first = qdrant.delete.call_count

    second = backfill.run_backfill(config, state_db, qdrant, None, False, 50)

    assert second["total"] == 0
    assert second["collisions"] == 0
    assert row_of(state_db, NEW_KEY) == after_first
    assert qdrant.delete.call_count == deletes_after_first


def test_collision_repeated_from_the_old_row_repeats_harmlessly(backfill, state_db, qdrant, config, claude_root):
    # A crash before the SQLite commit leaves the old row in place. Repeating the same
    # row must land on the same final state, because the deletes then match nothing.
    old = write_claude_transcript(claude_root)
    os.utime(old, (2000.0, 2000.0))
    add_completed(state_db, str(old))
    add_new_key_row(state_db, write_copy(claude_root, "-mnt-y", 1000.0), 1000.0)
    old_row = row_of(state_db, str(old))

    assert backfill.process_one(old_row, "claude", qdrant, state_db, NOW_ISO) is True
    after_first = row_of(state_db, NEW_KEY)
    assert backfill.process_one(old_row, "claude", qdrant, state_db, NOW_ISO) is True

    assert row_of(state_db, NEW_KEY) == after_first
    assert row_of(state_db, str(old)) is None
