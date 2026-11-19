"""
Tests for ragnart/sync_submit.py (session B2, step A.8): the submit half of the NAS ingestion side.

Each test builds a SyncPlan (or a queue state) by hand and runs submit_plan() against:
  - a real StateDB and a real RegistryDB, in tmp files;
  - one mocked HubClient per source id. Every call is appended to one shared log, so the order of
    requests across the sources is asserted, not inferred.

No live database, hub, OpenCode server or Qdrant is touched. The Claude transcript renderer runs for real on
a tiny tmp .jsonl (one test); the OpenCode renderer is replaced by a stub that records its arguments.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import urllib.error
from collections import Counter
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest

import ragnart.sync_submit as sync_submit
from ragnart.endpoint_client import HubClient, HubError
from ragnart.registry import RegistryDB
from ragnart.schema import (
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
)
from ragnart.state import StateDB
from ragnart.sync_plan_core import SyncPlan
from ragnart.sync_submit import submit_plan

NOW_ISO = "2026-11-19T10:00:00+00:00"
MTIME = 1_700_000_000.0  # 2023-11-14T22:13:20+00:00
MTIME_ISO = "2023-11-14T22:13:20+00:00"
CLAUDE_UUID = "11111111-2222-3333-4444-555555555555"
OPENCODE_KEY = "opencode://ses_abc"

CONFIG = {
    "rag": {"opencode_api_url": "http://oc.test:4096"},
    "opencode": {"password_env_file": "/secrets/opencode.env"},
}


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def state(tmp_path) -> StateDB:
    db = StateDB(str(tmp_path / "rag_state.db"))
    yield db
    db.close()


@pytest.fixture
def registry(tmp_path) -> RegistryDB:
    db = RegistryDB(str(tmp_path / "hub_registry.db"))
    yield db
    db.close()


@pytest.fixture(autouse=True)
def _hash_real_files_only(monkeypatch):
    """
    submit_plan() hashes an uploaded file before the upload (ragnart/sync_submit.py _send_item). Most tests in
    this file name NAS paths that are not on disk and mock the upload, so the file is never opened there.
    For a path that does not exist, this stub returns the empty-file size and MD5 instead of raising. A real
    file is hashed for real, which the fingerprint tests rely on.
    """
    real_hash_file = sync_submit.hash_file

    def hash_or_stub(path: str) -> tuple[int, str]:
        if os.path.exists(path):
            return real_hash_file(path)
        return 0, hashlib.md5(b"").hexdigest()

    monkeypatch.setattr(sync_submit, "hash_file", hash_or_stub)


@pytest.fixture
def calls() -> list:
    """The shared request log: (source label, method name, positional args, keyword args), in send order."""
    return []


def _answer(method: str, counter: list) -> dict:
    """The answer a fake hub gives for one method. Upserts and changes get a request id; a project gets its own answer."""
    if method == "publish_project":
        return {"published": True}
    if method == "remove_project":
        return {"removed": True}
    counter.append(1)
    return {"request_id": f"req-{len(counter)}", "status": "received"}


def _fake_client(label: str, calls: list, request_ids: list, registry: RegistryDB | None = None) -> MagicMock:
    """
    A HubClient stand-in. Each method records its call in `calls` and answers like the hub would.

    publish_project writes the registry row when `registry` is given, as the hub does synchronously, so the
    upserts that follow see the new project in the registry.
    """
    client = MagicMock(spec=HubClient)

    def record(method: str):
        def side_effect(*args, **kwargs):
            calls.append((label, method, args, kwargs))
            if method == "publish_project" and registry is not None:
                registry.upsert_project(args[0], kwargs.get("path"), args[1], kwargs.get("owner"), SOURCE_ID_NAS)
                registry.replace_project_tags(args[0], kwargs.get("tags") or {})
            return _answer(method, request_ids)
        return side_effect

    for method in ("upsert_bytes", "upsert_text", "delete", "change_key", "change_metadata",
                   "publish_project", "remove_project"):
        getattr(client, method).side_effect = record(method)
    return client


@pytest.fixture
def clients(calls, registry) -> dict:
    """One fake client per source id, all writing to the one shared call log."""
    request_ids: list = []
    return {
        SOURCE_ID_NAS: _fake_client("nas", calls, request_ids, registry),
        SOURCE_ID_CLAUDE_LINUX_SHARED_NFS: _fake_client("claude", calls, request_ids),
        SOURCE_ID_OPENCODE_K11: _fake_client("opencode", calls, request_ids),
    }


def _queue_file(state: StateDB, path: str, owner: str = "florian", mtime: float = MTIME, **kwargs) -> None:
    """Queue one NAS file as the scan does: a pending row with its source id."""
    state.discover_or_update(path, owner, mtime, origin_path=path, source_type="file",
                             file_type=Path(path).suffix.lstrip(".").lower(), source_id=SOURCE_ID_NAS, **kwargs)


def _row(state: StateDB, path: str) -> dict:
    """One queue row, read through a separate connection, or None when it does not exist."""
    conn = sqlite3.connect(state._db_path)
    conn.row_factory = sqlite3.Row
    try:
        found = conn.execute("SELECT * FROM ingestion_queue WHERE file_path = ?", (path,)).fetchone()
        return dict(found) if found is not None else None
    finally:
        conn.close()


def _calls_of(calls: list, method: str) -> list:
    return [call for call in calls if call[1] == method]


def _run(plan: SyncPlan, state, registry, clients, config=CONFIG) -> Counter:
    return submit_plan(plan, state, registry, clients, config, NOW_ISO)


# ---------------------------------------------------------------------------
# Upserts: owner, tag and project_id
# ---------------------------------------------------------------------------

def test_nas_file_is_uploaded_as_bytes_with_owner_and_project(state, registry, clients, calls):
    """A pending NAS file under a registered project is uploaded with the row's owner and the project's id."""
    registry.upsert_project("proj-1", "/mnt/nfs/Florian/proj", "Proj", "florian", SOURCE_ID_NAS)
    path = "/mnt/nfs/Florian/proj/notes.MD"
    _queue_file(state, path)

    counts = _run(SyncPlan(), state, registry, clients)

    upload = _calls_of(calls, "upsert_bytes")
    assert len(upload) == 1
    label, _, args, kwargs = upload[0]
    assert label == "nas"
    # The file type is the key's suffix, lowercased. No tag is sent: this row has none.
    assert args == (path, path, "md")
    assert kwargs == {"modified_at": MTIME_ISO, "owner": "florian", "project_id": "proj-1"}
    row = _row(state, path)
    assert row["status"] == "processing"
    assert row["project_id"] == "proj-1"
    assert counts["submitted"] == 1


def test_nas_file_outside_every_project_is_sent_to_global(state, registry, clients, calls):
    """A file no registered project encloses attaches to 'global'."""
    path = "/mnt/nfs/La-Familia/menu.md"
    _queue_file(state, path, owner="la-familia")

    _run(SyncPlan(), state, registry, clients)

    kwargs = _calls_of(calls, "upsert_bytes")[0][3]
    assert kwargs["project_id"] == "global"
    assert kwargs["owner"] == "la-familia"


def test_claude_session_is_rendered_and_sent_as_text_with_its_chat_metadata(state, registry, clients, calls, tmp_path):
    """
    A pending Claude session is rendered by the real parser and sent as text with its chat metadata.

    Its project comes from its launch directory (the transcript's cwd), not from its key.
    """
    registry.upsert_project("proj-cwd", "/home/x/demo", "Demo", "florian", SOURCE_ID_NAS)
    transcript = tmp_path / "projects" / f"{CLAUDE_UUID}.jsonl"
    transcript.parent.mkdir(parents=True)
    entries = [
        {"type": "user", "sessionId": CLAUDE_UUID, "cwd": "/home/x/demo/sub", "timestamp": "2026-11-18T09:00:00Z",
         "message": {"role": "user", "content": "How do I rotate the backup key?"}},
        {"type": "assistant", "sessionId": CLAUDE_UUID, "cwd": "/home/x/demo/sub",
         "message": {"role": "assistant", "model": "m", "content": [{"type": "text", "text": "Run the rotate command."}]}},
    ]
    transcript.write_text("\n".join(json.dumps(e) for e in entries) + "\n", encoding="utf-8")
    key = f"claude-session://{CLAUDE_UUID}"
    state.discover_or_update(key, "florian", MTIME, origin_path=str(transcript), source_type="claude_session",
                             source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS)

    _run(SyncPlan(), state, registry, clients)

    sent = _calls_of(calls, "upsert_text")
    assert len(sent) == 1
    label, _, args, kwargs = sent[0]
    assert label == "claude"
    assert args[0] == key
    assert "How do I rotate the backup key?" in args[1]
    assert kwargs["metadata"]["session_id"] == CLAUDE_UUID
    assert kwargs["modified_at"] == MTIME_ISO
    assert kwargs["owner"] == "florian"
    assert kwargs["project_id"] == "proj-cwd"
    row = _row(state, key)
    assert row["status"] == "processing"
    assert row["launch_dir"] == "/home/x/demo/sub"
    assert row["project_id"] == "proj-cwd"


def test_claude_session_with_no_launch_directory_attaches_to_global(state, registry, clients, calls, tmp_path):
    """
    A session whose transcript records no cwd has no launch directory, so it attaches to 'global' and sends no launch_dir.

    The registry holds a project, and the session is not under it: attachment must not match anything when there is no
    directory to match on.
    """
    registry.upsert_project("proj-cwd", "/home/x/demo", "Demo", "florian", SOURCE_ID_NAS)
    transcript = tmp_path / "projects" / f"{CLAUDE_UUID}.jsonl"
    transcript.parent.mkdir(parents=True)
    entries = [
        {"type": "user", "sessionId": CLAUDE_UUID, "cwd": "", "timestamp": "2026-11-18T09:00:00Z",
         "message": {"role": "user", "content": "How do I rotate the backup key?"}},
        {"type": "assistant", "sessionId": CLAUDE_UUID, "cwd": "",
         "message": {"role": "assistant", "model": "m", "content": [{"type": "text", "text": "Run the rotate command."}]}},
    ]
    transcript.write_text("\n".join(json.dumps(e) for e in entries) + "\n", encoding="utf-8")
    key = f"claude-session://{CLAUDE_UUID}"
    state.discover_or_update(key, "florian", MTIME, origin_path=str(transcript), source_type="claude_session",
                             source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS)

    _run(SyncPlan(), state, registry, clients)

    sent = _calls_of(calls, "upsert_text")
    assert len(sent) == 1
    assert sent[0][3]["project_id"] == "global"
    row = _row(state, key)
    assert row["launch_dir"] is None
    assert row["project_id"] == "global"


def test_opencode_session_is_rendered_with_the_configured_server_and_password_file(state, registry, clients, calls, monkeypatch):
    """The OpenCode renderer gets the configured server address and password file, and its text goes as text."""
    seen = {}

    def fake_render(path, api_url, password_env_file):
        seen.update(path=path, api_url=api_url, password_env_file=password_env_file)
        return "the session", {"session_id": "ses_abc", "session_title": "Title"}, None

    monkeypatch.setattr(sync_submit, "_parse_opencode_session", fake_render)
    state.discover_or_update(OPENCODE_KEY, "florian", MTIME, source_type="opencode_session",
                             source_id=SOURCE_ID_OPENCODE_K11)

    counts = _run(SyncPlan(), state, registry, clients)

    assert seen == {"path": OPENCODE_KEY, "api_url": "http://oc.test:4096", "password_env_file": "/secrets/opencode.env"}
    sent = _calls_of(calls, "upsert_text")
    assert len(sent) == 1
    assert sent[0][0] == "opencode"
    assert sent[0][3] == {
        "metadata": {"session_id": "ses_abc", "session_title": "Title"},
        "modified_at": MTIME_ISO, "owner": "florian", "project_id": "global",
    }
    assert _row(state, OPENCODE_KEY)["status"] == "processing"
    assert counts["submitted"] == 1


def test_accepted_upsert_records_the_request_id_on_the_row(state, registry, clients, calls):
    """The request id the hub returned is stored on the processing row, where the poll reads it."""
    path = "/mnt/nfs/Florian/a.md"
    _queue_file(state, path)

    _run(SyncPlan(), state, registry, clients)

    row = _row(state, path)
    assert row["status"] == "processing"
    assert row["request_id"] == "req-1"


def test_uploaded_file_records_size_md5_and_hash_time_when_the_hub_accepts_it(state, registry, clients, calls, tmp_path):
    """
    The size and MD5 of the bytes sent are stored on the row at acceptance, with the hash time. They are
    the same values the scanner would store for a new file (ragnart/hashing.py hash_file).
    """
    content = b"hello hub\n"
    path = str(tmp_path / "notes.md")
    with open(path, "wb") as handle:
        handle.write(content)
    _queue_file(state, path)

    _run(SyncPlan(), state, registry, clients)

    assert _calls_of(calls, "upsert_bytes")[0][2][0] == path
    row = _row(state, path)
    assert row["status"] == "processing"
    assert row["size_bytes"] == len(content)
    assert row["content_hash"] == hashlib.md5(content).hexdigest()
    assert row["hashed_at"] is not None


def test_a_refused_upload_does_not_record_a_fingerprint(state, registry, clients, tmp_path):
    """A row the hub refuses (here 413) has no accepted bytes, so its fingerprint columns are left as they were."""
    path = str(tmp_path / "big.md")
    with open(path, "wb") as handle:
        handle.write(b"x" * 10)
    _queue_file(state, path)
    clients[SOURCE_ID_NAS].upsert_bytes.side_effect = HubError(413, "too large")

    _run(SyncPlan(), state, registry, clients)

    row = _row(state, path)
    assert row["status"] == "ignored"
    assert row["size_bytes"] is None
    assert row["content_hash"] is None


# ---------------------------------------------------------------------------
# Upsert outcomes: door refusal, transport, other hub errors, item errors
# ---------------------------------------------------------------------------

def test_413_too_large_deletes_the_old_points_and_marks_the_row_ignored(state, registry, clients, calls):
    """A 413 (an admission refusal) deletes the item's old points, then records the hub's reason as ignored."""
    path = "/mnt/nfs/Florian/big.md"
    _queue_file(state, path)
    clients[SOURCE_ID_NAS].upsert_bytes.side_effect = HubError(413, "too large for the cap")

    counts = _run(SyncPlan(), state, registry, clients)

    assert _calls_of(calls, "delete") == [("nas", "delete", (path,), {})]
    row = _row(state, path)
    assert row["status"] == "ignored"
    assert row["skip_reason"] == "too large for the cap"
    assert counts["refused"] == 1


def test_422_request_error_leaves_the_row_pending_with_nothing_deleted(state, registry, clients, calls, caplog):
    """
    A 422 is a request error, not a judgement on the item. Nothing is deleted, the row is not ignored,
    an ERROR with the hub's detail is logged, and the run goes on to the next row.
    """
    refused, other = "/mnt/nfs/Florian/refused.md", "/mnt/nfs/Florian/other.md"
    _queue_file(state, refused)
    _queue_file(state, other)
    clients[SOURCE_ID_NAS].upsert_bytes.side_effect = [
        HubError(422, "owner 'florian' may not be named"),
        {"request_id": "req-other", "status": "received"},
    ]

    with caplog.at_level(logging.ERROR, logger="ragnart.sync_submit"):
        counts = _run(SyncPlan(), state, registry, clients)

    assert _calls_of(calls, "delete") == []
    row = _row(state, refused)
    assert row["status"] == "pending"
    assert row["skip_reason"] is None
    assert row["retry_count"] == 0
    assert counts["request_error"] == 1
    assert counts["refused"] == 0
    assert "owner 'florian' may not be named" in caplog.text
    assert any(record.levelno == logging.ERROR and refused in record.getMessage() for record in caplog.records)
    # The run goes on: the next row is still submitted.
    assert _row(state, other)["status"] == "processing"
    assert counts["submitted"] == 1


def test_refusal_whose_delete_fails_leaves_the_row_pending(state, registry, clients):
    """If the old points cannot be deleted, the row stays pending and the next run tries again."""
    path = "/mnt/nfs/Florian/big.md"
    _queue_file(state, path)
    clients[SOURCE_ID_NAS].upsert_bytes.side_effect = HubError(413, "too large")
    clients[SOURCE_ID_NAS].delete.side_effect = httpx.ConnectError("hub down")

    counts = _run(SyncPlan(), state, registry, clients)

    assert _row(state, path)["status"] == "pending"
    assert counts["refused_delete_failed"] == 1


@pytest.mark.parametrize("source_id", [SOURCE_ID_NAS, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_OPENCODE_K11])
def test_a_nas_side_source_with_a_fixed_owner_refuses_to_run_before_any_request(state, registry, clients, calls, source_id):
    """
    The preflight refuses the whole run when any NAS-side source has a registry owner, because the
    per-item owner would be refused for it. Nothing is sent, and the message names the source.
    """
    registry._conn.execute("UPDATE sources SET owner = 'florian' WHERE id = ?", (source_id,))
    registry._conn.commit()
    _queue_file(state, "/mnt/nfs/Florian/a.md")

    with pytest.raises(RuntimeError, match=source_id):
        _run(SyncPlan(), state, registry, clients)

    assert calls == []
    assert _row(state, "/mnt/nfs/Florian/a.md")["status"] == "pending"


def test_the_preflight_passes_when_no_nas_side_source_has_a_fixed_owner(state, registry, clients, calls):
    """The three sources are seeded with a NULL owner, so the preflight lets the run through."""
    _queue_file(state, "/mnt/nfs/Florian/a.md")

    counts = _run(SyncPlan(), state, registry, clients)

    assert counts["submitted"] == 1


def test_transport_error_leaves_the_row_pending_with_no_attempt_counted(state, registry, clients):
    """A transport error means the hub never saw the request: the row stays pending, retry_count is unchanged, the run goes on."""
    first, second = "/mnt/nfs/Florian/a.md", "/mnt/nfs/Florian/b.md"
    _queue_file(state, first)
    _queue_file(state, second)
    clients[SOURCE_ID_NAS].upsert_bytes.side_effect = [httpx.ConnectError("hub down"), {"request_id": "req-b", "status": "received"}]

    counts = _run(SyncPlan(), state, registry, clients)

    assert _row(state, first)["status"] == "pending"
    assert _row(state, first)["retry_count"] == 0
    assert _row(state, second)["status"] == "processing"
    assert counts["transport"] == 1
    assert counts["submitted"] == 1


def test_other_hub_error_leaves_the_row_pending(state, registry, clients):
    """A hub answer that is not a door refusal (here 401) leaves the row pending, for the next run."""
    path = "/mnt/nfs/Florian/a.md"
    _queue_file(state, path)
    clients[SOURCE_ID_NAS].upsert_bytes.side_effect = HubError(401, "missing or invalid source secret")

    counts = _run(SyncPlan(), state, registry, clients)

    assert _row(state, path)["status"] == "pending"
    assert counts["hub_error"] == 1


def test_file_that_cannot_be_read_is_marked_failed_with_the_retry_rule(state, registry, clients):
    """A file that vanished before upload is failed. mark_failed puts it back to pending with one retry counted."""
    path = "/mnt/nfs/Florian/gone.md"
    _queue_file(state, path)
    clients[SOURCE_ID_NAS].upsert_bytes.side_effect = FileNotFoundError("gone.md")

    counts = _run(SyncPlan(), state, registry, clients)

    row = _row(state, path)
    assert row["status"] == "pending"
    assert row["retry_count"] == 1
    assert "gone.md" in row["error_msg"]
    assert counts["render_failed"] == 1


def test_real_file_deleted_from_disk_before_submit_is_marked_failed_without_the_hash_stub(state, registry, clients, monkeypatch, tmp_path):
    """
    A file that was on disk when the scan queued it, and is gone when the submit runs, is failed with the same outcome as
    an unreadable file. Unlike the tests above, the autouse hash stub is undone here, so the real hash_file runs on the
    missing path and raises, as it would on the NFS.
    """
    import ragnart.hashing as hashing

    monkeypatch.setattr(sync_submit, "hash_file", hashing.hash_file)  # opt out of the autouse stub for this test
    path = tmp_path / "vanished.md"
    path.write_text("was here when queued", encoding="utf-8")
    _queue_file(state, str(path))
    path.unlink()

    counts = _run(SyncPlan(), state, registry, clients)

    row = _row(state, str(path))
    assert row["status"] == "pending"
    assert row["retry_count"] == 1
    assert "vanished.md" in row["error_msg"]
    assert counts["render_failed"] == 1
    clients[SOURCE_ID_NAS].upsert_bytes.assert_not_called()


def test_claude_session_that_does_not_render_is_marked_failed(state, registry, clients, monkeypatch):
    """A session the renderer rejects follows the file rule: failed, with the retry counted."""
    def broken(path):
        raise ValueError("not a transcript")

    monkeypatch.setattr(sync_submit, "_parse_claude_chat", broken)
    key = f"claude-session://{CLAUDE_UUID}"
    state.discover_or_update(key, "florian", MTIME, origin_path="/x/bad.jsonl", source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS)

    _run(SyncPlan(), state, registry, clients)

    row = _row(state, key)
    assert row["retry_count"] == 1
    assert row["error_msg"] == "not a transcript"


def test_opencode_session_that_cannot_be_reached_stays_pending(state, registry, clients, monkeypatch):
    """OpenCode unreachable is the server's fault, not the item's: the row stays pending with no retry counted."""
    def unreachable(path, api_url, password_env_file):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(sync_submit, "_parse_opencode_session", unreachable)
    state.discover_or_update(OPENCODE_KEY, "florian", MTIME, source_id=SOURCE_ID_OPENCODE_K11)

    counts = _run(SyncPlan(), state, registry, clients)

    row = _row(state, OPENCODE_KEY)
    assert row["status"] == "pending"
    assert row["retry_count"] == 0
    assert counts["opencode_unavailable"] == 1


def test_claude_session_without_an_origin_path_is_marked_failed(state, registry, clients):
    """A session row with no origin_path cannot be read, so it fails rather than being guessed."""
    key = f"claude-session://{CLAUDE_UUID}"
    state.discover_or_update(key, "florian", MTIME, source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS)

    _run(SyncPlan(), state, registry, clients)

    assert _row(state, key)["retry_count"] == 1


# ---------------------------------------------------------------------------
# Moves: change_key first, rename only after acceptance
# ---------------------------------------------------------------------------

def _completed_with_hash(state: StateDB, path: str, content_hash: str = "h-1") -> None:
    """A completed NAS row with a content hash, as a previous run left it."""
    state.discover_or_update(path, "florian", MTIME, origin_path=path, source_type="file",
                             file_type="md", source_id=SOURCE_ID_NAS, content_hash=content_hash)
    state.mark_completed(path, chunk_count=2)


def test_move_sends_change_key_and_renames_the_row_only_after_acceptance(state, registry, clients, calls):
    """
    The hub is asked to change the key first. At the moment of that request the local row is still at the
    old key, so the rename cannot have happened early.
    """
    old, new = "/mnt/nfs/Florian/old.md", "/mnt/nfs/Florian/new.md"
    _completed_with_hash(state, old)
    _queue_file(state, new)
    seen_at_request = {}

    def change_key(key, new_key):
        seen_at_request["old_row_present"] = _row(state, old) is not None
        seen_at_request["new_row_present"] = _row(state, new) is not None
        return {"request_id": "req-move", "status": "received"}

    clients[SOURCE_ID_NAS].change_key.side_effect = change_key
    plan = SyncPlan(moves={old: new})

    counts = _run(plan, state, registry, clients)

    assert seen_at_request == {"old_row_present": True, "new_row_present": True}
    assert _row(state, old) is None
    moved = _row(state, new)
    assert moved["status"] == "completed"
    assert moved["chunk_count"] == 2
    assert counts["moved"] == 1
    # The pending row that the scan queued at the new path is gone, so it is not uploaded again.
    assert not any(call[2] == (new, new, "md") for call in _calls_of(calls, "upsert_bytes"))


def test_move_refused_by_the_hub_changes_nothing_locally(state, registry, clients, calls):
    """
    A refused change_key leaves the old row as it was. The file at the new path keeps its pending row, so the
    upsert step uploads it as an ordinary new item. The old key is then vanished, tombstoned and purged later.
    """
    old, new = "/mnt/nfs/Florian/old.md", "/mnt/nfs/Florian/new.md"
    _completed_with_hash(state, old)
    _queue_file(state, new)
    clients[SOURCE_ID_NAS].change_key.side_effect = HubError(422, "refused")

    counts = _run(SyncPlan(moves={old: new}), state, registry, clients)

    assert _row(state, old)["status"] == "completed"
    assert _row(state, new)["status"] == "processing"
    assert [call[2] for call in _calls_of(calls, "upsert_bytes")] == [(new, new, "md")]
    assert counts["move_failed"] == 1


def test_move_without_a_pending_target_refuses_before_any_request(state, registry, clients, calls):
    """A move with no pending row at its target is refused before anything is sent."""
    old, new = "/mnt/nfs/Florian/old.md", "/mnt/nfs/Florian/new.md"
    _completed_with_hash(state, old)

    with pytest.raises(RuntimeError, match="no pending row"):
        _run(SyncPlan(moves={old: new}), state, registry, clients)

    assert _calls_of(calls, "change_key") == []


# ---------------------------------------------------------------------------
# Purge, tombstones, stale rows, attachment
# ---------------------------------------------------------------------------

def _tombstoned(state: StateDB, path: str, missing_since: str) -> None:
    """A completed row that vanished on an earlier run, tombstoned at missing_since."""
    _completed_with_hash(state, path)
    state.mark_missing(path, missing_since)


def test_purge_sends_delete_and_removes_the_row_only_after_acceptance(state, registry, clients, calls):
    """A tombstone past its grace period is deleted on the hub. Its row is removed after the hub accepts."""
    path = "/mnt/nfs/Florian/gone.md"
    _tombstoned(state, path, "2026-11-10T00:00:00+00:00")
    seen = {}

    def delete(key):
        seen["row_present"] = _row(state, path) is not None
        return {"request_id": "req-del", "status": "received"}

    clients[SOURCE_ID_NAS].delete.side_effect = delete

    counts = _run(SyncPlan(purge_ready=[path]), state, registry, clients)

    assert seen == {"row_present": True}
    assert _row(state, path) is None
    assert counts["purged"] == 1


def test_purge_whose_delete_fails_keeps_the_row_and_its_tombstone(state, registry, clients):
    """A purge the hub did not accept keeps the row, so the next run purges it again."""
    path = "/mnt/nfs/Florian/gone.md"
    _tombstoned(state, path, "2026-11-10T00:00:00+00:00")
    clients[SOURCE_ID_NAS].delete.side_effect = httpx.ConnectError("hub down")

    counts = _run(SyncPlan(purge_ready=[path]), state, registry, clients)

    assert _row(state, path)["missing_since"] == "2026-11-10T00:00:00+00:00"
    assert counts["purge_failed"] == 1


def test_first_missing_since_is_kept_when_the_row_is_still_missing_on_the_next_sync(state, registry, clients, calls):
    """
    Two syncs, both reporting the same row as newly missing. The first run's timestamp is the one that stays.

    The grace period is measured from the first sighting, so a second sync must not restart it. This is the
    submit half of the state rule in tests/test_state_projects.py (test_second_mark_missing_keeps_the_first_timestamp).
    """
    path = "/mnt/nfs/Florian/vanished.md"
    _completed_with_hash(state, path)
    first_sync = "2026-11-18T10:00:00+00:00"
    second_sync = NOW_ISO  # 2026-11-19T10:00:00+00:00, one day later

    submit_plan(SyncPlan(newly_missing=[path]), state, registry, clients, CONFIG, first_sync)
    submit_plan(SyncPlan(newly_missing=[path]), state, registry, clients, CONFIG, second_sync)

    assert _row(state, path)["missing_since"] == first_sync
    assert _row(state, path)["status"] == "completed"
    assert calls == []


def test_purge_logs_one_line_with_key_chunks_reason_and_missing_since(state, registry, clients, caplog):
    """The purge log line keeps its exact form: key, chunks, reason and missing_since."""
    path = "/mnt/nfs/Florian/gone.md"
    _tombstoned(state, path, "2026-11-10T00:00:00+00:00")
    state._conn.execute("UPDATE ingestion_queue SET chunk_count = 5 WHERE file_path = ?", (path,))
    state._conn.commit()

    with caplog.at_level(logging.INFO, logger="ragnart.sync_submit"):
        _run(SyncPlan(purge_ready=[path]), state, registry, clients)

    purge_lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("purged ")]
    assert purge_lines == [
        f"purged {path} chunks=5 reason=tombstone grace period expired missing_since=2026-11-10T00:00:00+00:00"
    ]


def test_claude_session_purge_and_attachment_go_to_the_claude_source_client(state, registry, clients, calls):
    """
    Ported from tests/test_sync_plan.py (point filters name the source of the key). A session's delete and its
    change_metadata are sent through the claude-linux-shared-nfs client, never through the NAS client.
    """
    session = f"claude-session://{CLAUDE_UUID}"
    state.discover_or_update(session, "florian", MTIME, origin_path="/home/x/s.jsonl",
                             source_type="claude_session", source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS)
    state.mark_completed(session, chunk_count=2)
    state.mark_missing(session, "2026-11-10T00:00:00+00:00")
    attached = f"claude-session://{CLAUDE_UUID[:-1]}0"
    state.discover_or_update(attached, "florian", MTIME, origin_path="/home/x/t.jsonl",
                             source_type="claude_session", source_id=SOURCE_ID_CLAUDE_LINUX_SHARED_NFS)
    state.mark_completed(attached, chunk_count=2)

    _run(SyncPlan(purge_ready=[session], attachment_changes={attached: "pid-p"}), state, registry, clients)

    assert [c[0] for c in _calls_of(calls, "delete")] == ["claude"]
    assert [c[0] for c in _calls_of(calls, "change_metadata")] == ["claude"]
    assert _calls_of(calls, "change_metadata")[0][2] == (attached,)


def test_newly_missing_reappeared_and_stale_rows_are_local_only(state, registry, clients, calls):
    """Tombstoning, clearing a tombstone and removing stale rows send no request."""
    tombstoned_then_back = "/mnt/nfs/Florian/back.md"
    vanished = "/mnt/nfs/Florian/vanished.md"
    stale = "/mnt/nfs/Florian/stale.md"
    _tombstoned(state, tombstoned_then_back, "2026-11-18T00:00:00+00:00")
    _completed_with_hash(state, vanished)
    state.discover_or_update(stale, "florian", MTIME, source_id=SOURCE_ID_NAS)

    counts = _run(SyncPlan(reappeared=[tombstoned_then_back], newly_missing=[vanished], stale_rows=[stale]),
                  state, registry, clients)

    assert calls == []
    assert _row(state, tombstoned_then_back)["missing_since"] is None
    assert _row(state, vanished)["missing_since"] == NOW_ISO
    assert _row(state, vanished)["status"] == "completed"
    assert _row(state, stale) is None
    assert (counts["reappeared"], counts["newly_missing"], counts["stale_removed"]) == (1, 1, 1)


def test_attachment_sends_change_metadata_and_sets_the_project_after_acceptance(state, registry, clients, calls):
    """A re-attachment is sent as change_metadata(project_id). The local project_id changes only once it is accepted."""
    path = "/mnt/nfs/Florian/proj/a.md"
    registry.upsert_project("proj-1", "/mnt/nfs/Florian/proj", "Proj", "florian", SOURCE_ID_NAS)
    _completed_with_hash(state, path)

    counts = _run(SyncPlan(attachment_changes={path: "proj-1"}), state, registry, clients)

    assert _calls_of(calls, "change_metadata") == [("nas", "change_metadata", (path,), {"project_id": "proj-1"})]
    assert _row(state, path)["project_id"] == "proj-1"
    assert counts["attached"] == 1


def test_attachment_refused_by_the_hub_leaves_the_project_unchanged(state, registry, clients):
    """A 422 from change_metadata (the project is not in the registry) leaves the row as it was."""
    path = "/mnt/nfs/Florian/proj/a.md"
    _completed_with_hash(state, path)
    clients[SOURCE_ID_NAS].change_metadata.side_effect = HubError(422, "project 'x' does not exist")

    counts = _run(SyncPlan(attachment_changes={path: "x"}), state, registry, clients)

    assert _row(state, path)["project_id"] == "global"
    assert counts["attach_failed"] == 1


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------

def _marker(project_id: str, path: str, owner: str = "florian") -> dict:
    return {"id": project_id, "path": path, "name": project_id.title(), "owner": owner, "tags": {"kind": "repo"},
            "inherit_access": True, "mode": None}


def test_projects_are_published_before_the_uploads_that_name_them(state, registry, clients, calls):
    """
    A new marker is published (and written to the registry by the hub) before the upload of a file inside it.
    The upload therefore carries that project's id.
    """
    _queue_file(state, "/mnt/nfs/Florian/new-proj/x.md")
    plan = SyncPlan(project_upserts=[_marker("new-proj", "/mnt/nfs/Florian/new-proj")])

    _run(plan, state, registry, clients)

    order = [method for _, method, _, _ in calls]
    assert order.index("publish_project") < order.index("upsert_bytes")
    publish = _calls_of(calls, "publish_project")[0]
    assert publish[2] == ("new-proj", "New-Proj")
    assert publish[3] == {"path": "/mnt/nfs/Florian/new-proj", "owner": "florian", "tags": {"kind": "repo"}}
    assert _calls_of(calls, "upsert_bytes")[0][3]["project_id"] == "new-proj"


def test_a_project_the_hub_refuses_is_not_used_for_attachment(state, registry, clients, calls):
    """A refused publish leaves no registry row, so the upload attaches to 'global', not to the refused project."""
    _queue_file(state, "/mnt/nfs/Florian/homed-elsewhere/x.md")
    clients[SOURCE_ID_NAS].publish_project.side_effect = None
    clients[SOURCE_ID_NAS].publish_project.return_value = {"refused": True, "reason": "homed on MBP"}
    plan = SyncPlan(project_upserts=[_marker("homed-elsewhere", "/mnt/nfs/Florian/homed-elsewhere")])

    counts = _run(plan, state, registry, clients)

    assert _calls_of(calls, "upsert_bytes")[0][3]["project_id"] == "global"
    assert counts["project_refused"] == 1


def test_project_removal_is_sent_and_a_404_counts_as_already_removed(state, registry, clients):
    """A removal goes to the hub. A 404 means the hub has no such project: the removal is already done."""
    registry.upsert_project("old-proj", "/mnt/nfs/Florian/old", "Old", "florian", SOURCE_ID_NAS)
    clients[SOURCE_ID_NAS].remove_project.side_effect = HubError(404, "no project")

    counts = _run(SyncPlan(project_removals=["old-proj"]), state, registry, clients)

    clients[SOURCE_ID_NAS].remove_project.assert_called_once_with("old-proj")
    assert counts["project_removed"] == 1


def test_project_removal_refused_with_409_is_counted_as_refused(state, registry, clients):
    """A removal of a project homed on another source is refused by the hub (409) and is only logged."""
    clients[SOURCE_ID_NAS].remove_project.side_effect = HubError(409, "homed on MBP")

    counts = _run(SyncPlan(project_removals=["mbp-proj"]), state, registry, clients)

    assert counts["project_refused"] == 1


def test_a_project_transport_failure_is_skipped_and_the_run_goes_on(state, registry, clients):
    """A publish the hub could not receive is counted, and the uploads after it still go."""
    path = "/mnt/nfs/Florian/a.md"
    _queue_file(state, path)
    clients[SOURCE_ID_NAS].publish_project.side_effect = httpx.ConnectError("hub down")
    plan = SyncPlan(project_upserts=[_marker("p", "/mnt/nfs/Florian/p")])

    counts = _run(plan, state, registry, clients)

    assert counts["project_failed"] == 1
    assert _row(state, path)["status"] == "processing"


# ---------------------------------------------------------------------------
# The whole plan: order, and the client check
# ---------------------------------------------------------------------------

def test_the_whole_plan_runs_in_the_documented_order(state, registry, clients, calls):
    """
    Projects, then the move, then the purge, then the attachment, then the uploads. Each step is before the next.
    """
    old, moved_to = "/mnt/nfs/Florian/old.md", "/mnt/nfs/Florian/moved.md"
    purged = "/mnt/nfs/Florian/purged.md"
    attached = "/mnt/nfs/Florian/attached.md"
    upload = "/mnt/nfs/Florian/proj-new/fresh.md"
    _completed_with_hash(state, old)
    _queue_file(state, moved_to)
    _tombstoned(state, purged, "2026-11-10T00:00:00+00:00")
    _completed_with_hash(state, attached)
    _queue_file(state, upload)
    plan = SyncPlan(
        project_upserts=[_marker("proj-new", "/mnt/nfs/Florian/proj-new")],
        moves={old: moved_to},
        purge_ready=[purged],
        attachment_changes={attached: "proj-new"},
    )

    _run(plan, state, registry, clients)

    order = [method for _, method, _, _ in calls]
    assert order == ["publish_project", "change_key", "delete", "change_metadata", "upsert_bytes"]


def test_a_source_without_a_client_refuses_before_anything_is_sent(state, registry, calls):
    """Every NAS-side source needs a client. A missing one is refused up front, with nothing sent."""
    clients = {SOURCE_ID_NAS: _fake_client("nas", calls, [])}

    with pytest.raises(ValueError, match="no HubClient"):
        _run(SyncPlan(), state, registry, clients)

    assert calls == []
