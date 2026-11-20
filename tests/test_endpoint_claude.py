"""
End-to-end tests for the claude_code endpoint runner (ragnart/endpoint.py, scan_claude_code) against the real hub API.

The hub side is the real ragnart.ingest_api app, driven through starlette's TestClient (an httpx.Client). The
endpoint side is ragnart.endpoint.run(), with its HubClient pointed at the same TestClient, so every request the
runner makes goes through the API's auth, validation and intake writes. The intake table is read back to see
what the hub recorded.

The hub's worker is not run. Its outcome is simulated by settle(): each received upsert is completed through the
registry, and the Qdrant listing (a MagicMock) shows each item whose latest intake row is an upsert.

Nothing live is touched: tmp dirs, a tmp registry and a tmp spool only.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

import ragnart.cli as cli
import ragnart.endpoint as endpoint
import ragnart.ingest_api as ingest_api
from ragnart.cli import registry_db_path
from ragnart.endpoint_client import HubClient
from ragnart.chat_render import _parse_claude_chat
from ragnart.endpoint_state import EndpointState
from ragnart.hashing import hash_text
from ragnart.registry import RegistryDB

CLAUDE_SOURCE = "claude-MBP"
CLAUDE_SECRET = "claude-secret-for-claude-code-tests"
BASE_URL = "http://hub.test:8101"

# A fixed clock. The grace period is measured from here, so the tests never depend on the real time.
NOW = datetime(2026, 11, 19, 10, 0, tzinfo=timezone.utc)

LOGGER_NAME = "ragnart.endpoint"


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


@pytest.fixture(autouse=True)
def _reset_endpoint_logger():
    """main() attaches handlers to the endpoint logger. Nothing here calls main(), but clear them anyway."""
    yield
    logger = logging.getLogger(LOGGER_NAME)
    for handler in list(logger.handlers):
        handler.close()
    logger.handlers.clear()


# ---------------------------------------------------------------------------
# Hub fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def hub_config(tmp_path, monkeypatch) -> dict:
    """A tmp ragnart.yaml: the admission caps and include list the API reads. The cap is 1000 bytes for bytes uploads."""
    cfg = {
        "db_base_path": str(tmp_path / "hub-db"),
        "rag": {
            "qdrant_url": "http://qdrant.invalid:6333",
            "admission": {
                "max_text_file_size_bytes": 1000,
                "max_chunks_per_item": 5000,
                "fffd_ratio_threshold": 0.005,
            },
            "scanner": {"include_extensions": [".md", ".jsonl"]},
        },
    }
    path = tmp_path / "ragnart.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    monkeypatch.setattr(cli, "CONFIG_PATH", path)
    return cfg


@pytest.fixture
def qdrant(monkeypatch) -> MagicMock:
    """A mocked Qdrant client. GET /v1/items reads its scroll result; by default the hub lists nothing."""
    client = MagicMock(name="qdrant_client")
    client.scroll.return_value = ([], None)
    monkeypatch.setattr(ingest_api, "get_client", lambda url, timeout=60: client)
    return client


@pytest.fixture
def hub(hub_config, qdrant):
    """The ingestion API with the claude_code source registered. The lifespan has run."""
    registry = RegistryDB(registry_db_path(hub_config))
    registry.register_source(CLAUDE_SOURCE, "claude_code", "endpoint", "florian", _sha256(CLAUDE_SECRET))
    registry.close()
    with TestClient(ingest_api.app) as client:
        yield client


@pytest.fixture
def sent(hub) -> list:
    """Every request the endpoint sends to the hub, in order."""
    requests: list[httpx.Request] = []
    hub.event_hooks["request"].append(requests.append)
    return requests


# ---------------------------------------------------------------------------
# Endpoint fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def ws(tmp_path, hub, monkeypatch) -> SimpleNamespace:
    """
    A tmp endpoint host: a projects root shaped like ~/.claude/projects, a state database, a secret and the config.

    Paths are real paths (no symlink in the tmp prefix), because the runner keys files by real path.
    """
    base = Path(os.path.realpath(tmp_path))
    root = base / "projects"
    root.mkdir()
    secret = base / "claude.secret"
    secret.write_text(CLAUDE_SECRET + "\n", encoding="utf-8")

    real_client = HubClient
    monkeypatch.setattr(
        endpoint, "HubClient",
        lambda base_url, secret_file, **kwargs: real_client(base_url, secret_file, http_client=hub, **kwargs),
    )
    workspace = SimpleNamespace(
        base=base,
        root=root,
        secret=secret,
        state_db=base / "state" / "endpoint.db",
        log=base / "logs" / "endpoint.log",
        config=base / "endpoint.yaml",
    )
    write_config(workspace)
    return workspace


def write_config(ws: SimpleNamespace, *, root: Path | None = None, grace_hours: float = 24) -> None:
    """Write endpoint.yaml for the claude_code source. The root defaults to the workspace's projects tree."""
    cfg = {
        "hub_url": BASE_URL,
        "state_db": str(ws.state_db),
        "log_file": str(ws.log),
        "grace_hours": grace_hours,
        "sources": [{
            "id": CLAUDE_SOURCE,
            "connector": "claude_code",
            "root": str(root or ws.root),
            "secret_file": str(ws.secret),
        }],
    }
    ws.config.write_text(yaml.safe_dump(cfg), encoding="utf-8")


def run(ws: SimpleNamespace, **kwargs) -> int:
    """Run the endpoint once against the workspace's config. kwargs go to endpoint.run()."""
    return endpoint.run(endpoint.load_config(str(ws.config)), **kwargs)


# ---------------------------------------------------------------------------
# Transcripts in the real JSONL shape
# ---------------------------------------------------------------------------

def write_transcript(
    path: Path,
    session_id: str,
    question: str,
    answer: str = "Run the rotate command.",
    cwd: str = "/Users/fotel/Gin-AI/projects/demo",
) -> Path:
    """
    Write a minimal Claude Code transcript: one user turn and one assistant text turn.

    The parser reads sessionId and cwd from any entry, and the chat title is the first user message, since no
    chats directory exists under the tmp projects tree.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    entries = [
        {"type": "user", "sessionId": session_id, "cwd": cwd, "timestamp": "2026-11-18T09:00:00Z",
         "message": {"role": "user", "content": question}},
        {"type": "assistant", "sessionId": session_id, "cwd": cwd,
         "message": {"role": "assistant", "model": "test-model",
                     "content": [{"type": "text", "text": answer}]}},
    ]
    path.write_text("\n".join(json.dumps(entry) for entry in entries) + "\n", encoding="utf-8")
    return path


def append_turn(path: Path, session_id: str, text: str, mtime: float) -> None:
    """Append one more assistant turn and set the file's mtime, so the change is seen on any filesystem."""
    entry = {"type": "assistant", "sessionId": session_id, "cwd": "/Users/fotel/Gin-AI/projects/demo",
             "message": {"role": "assistant", "model": "test-model", "content": [{"type": "text", "text": text}]}}
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry) + "\n")
    os.utime(path, (mtime, mtime))


# ---------------------------------------------------------------------------
# Reads and simulated worker steps
# ---------------------------------------------------------------------------

def intake_rows(hub_config: dict) -> list[dict]:
    """Every intake row of the claude_code source, in insertion order. Read-only."""
    conn = sqlite3.connect(f"file:{registry_db_path(hub_config)}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute(
            "SELECT * FROM intake WHERE source_id = ? ORDER BY rowid", (CLAUDE_SOURCE,))]
    finally:
        conn.close()


def upserts_for(hub_config: dict, key: str) -> list[dict]:
    """The upsert rows the hub recorded for one key, oldest first."""
    return [row for row in intake_rows(hub_config) if row["kind"] == "upsert" and row["key"] == key]


def upserted_keys(hub_config: dict) -> set[str]:
    """The keys of every upsert the hub recorded."""
    return {row["key"] for row in intake_rows(hub_config) if row["kind"] == "upsert"}


def state_rows(ws: SimpleNamespace) -> dict[str, dict]:
    """The endpoint's own rows for the claude_code source, read read-only."""
    state = EndpointState(str(ws.state_db), read_only=True)
    try:
        return state.rows(CLAUDE_SOURCE)
    finally:
        state.close()


def settle(hub_config: dict, qdrant: MagicMock) -> None:
    """
    Simulate the worker: complete every received upsert with two chunks, and list in Qdrant every key whose latest
    intake row is an upsert (a key whose latest row is a delete is not listed).
    """
    registry = RegistryDB(registry_db_path(hub_config))
    try:
        for row in intake_rows(hub_config):
            if row["kind"] == "upsert" and row["status"] == "received":
                registry.update_intake(row["id"], status="completed", chunk_count=2)
    finally:
        registry.close()
    latest_kind: dict[str, str] = {}
    for row in intake_rows(hub_config):
        latest_kind[row["key"]] = row["kind"]
    points = [
        SimpleNamespace(payload={"source_path": key, "content_hash": "h", "pipeline_version": "p1"})
        for key, kind in latest_kind.items() if kind == "upsert"
    ]
    qdrant.scroll.return_value = (points, None)


def key_of(session_id: str, agent: str | None = None) -> str:
    """The claude-session:// key of a main session, or of one of its subagent transcripts."""
    return f"claude-session://{session_id}" + (f"/{agent}" if agent else "")


# ---------------------------------------------------------------------------
# Submit: keys, text, metadata
# ---------------------------------------------------------------------------

def test_session_is_submitted_as_text_with_its_chat_metadata(ws, hub_config, sent):
    """A main session is a text upsert under claude-session://<uuid>, with the three chat fields and the file's mtime."""
    uuid = "11111111-aaaa-4bbb-8ccc-000000000001"
    path = write_transcript(ws.root / "-Users-fotel-demo" / f"{uuid}.jsonl", uuid, "How do I rotate the backup key?")

    assert run(ws, now=NOW) == 0

    rows = upserts_for(hub_config, key_of(uuid))
    assert len(rows) == 1
    row = rows[0]
    assert row["source_id"] == CLAUDE_SOURCE
    metadata = json.loads(row["metadata"])
    assert metadata == {
        "session_id": uuid,
        "project": "demo",
        "session_title": "How do I rotate the backup key?",
    }
    assert "**User**: How do I rotate the backup key?" in row["text"]
    assert "**Claude**: Run the rotate command." in row["text"]
    assert row["modified_at"] == datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
    state = state_rows(ws)[key_of(uuid)]
    assert state["status"] == "pending"
    assert state["content_hash"] == _md5(row["text"]), "the stored hash is the hash of the submitted text"
    assert [request.url.path for request in sent if request.method == "POST"] == ["/v1/upsert/text"]


def test_subagent_transcripts_get_their_agent_keys(ws, hub_config):
    """A direct subagent and a workflow agent are submitted under the parent session's uuid and their own agent id."""
    uuid = "22222222-aaaa-4bbb-8ccc-000000000002"
    write_transcript(ws.root / "-Users-fotel-demo" / f"{uuid}.jsonl", uuid, "parent question")
    write_transcript(ws.root / "-Users-fotel-demo" / uuid / "subagents" / "agent-a1bbf3e4e190989ea.jsonl",
                     uuid, "direct subagent question")
    write_transcript(ws.root / "-Users-fotel-demo" / uuid / "subagents" / "workflows" / "wf_08ea07fa-b92"
                     / "agent-b2c3d4.jsonl", uuid, "workflow agent question")

    assert run(ws, now=NOW) == 0

    assert upserted_keys(hub_config) == {
        key_of(uuid),
        key_of(uuid, "agent-a1bbf3e4e190989ea"),
        key_of(uuid, "agent-b2c3d4"),
    }


def test_workflow_journal_is_never_submitted(ws, hub_config):
    """A workflow journal is an orchestration log: it has no key, and the runner never submits it."""
    uuid = "33333333-aaaa-4bbb-8ccc-000000000003"
    write_transcript(ws.root / "-Users-fotel-demo" / f"{uuid}.jsonl", uuid, "a real conversation")
    write_transcript(ws.root / "-Users-fotel-demo" / uuid / "subagents" / "workflows" / "wf_1" / "journal.jsonl",
                     uuid, "orchestration log")

    assert run(ws, now=NOW) == 0

    assert upserted_keys(hub_config) == {key_of(uuid)}


def test_duplicate_uuid_submits_only_the_newest_copy(ws, hub_config):
    """Two project folders hold one session uuid: only the copy with the newer mtime is submitted, and its metadata is its own."""
    uuid = "44444444-aaaa-4bbb-8ccc-000000000004"
    older = write_transcript(ws.root / "projA" / f"{uuid}.jsonl", uuid, "older copy",
                             cwd="/Users/fotel/older-proj")
    newer = write_transcript(ws.root / "projB" / f"{uuid}.jsonl", uuid, "newer copy",
                             cwd="/Users/fotel/newer-proj")
    os.utime(older, (1_700_000_000, 1_700_000_000))
    os.utime(newer, (1_700_000_500, 1_700_000_500))

    assert run(ws, now=NOW) == 0

    rows = upserts_for(hub_config, key_of(uuid))
    assert len(rows) == 1, "one item for the uuid, not one per copy"
    assert json.loads(rows[0]["metadata"])["project"] == "newer-proj"
    assert "newer copy" in rows[0]["text"]


def test_duplicate_uuid_with_equal_mtime_goes_to_the_greatest_path(ws, hub_config):
    """An exact mtime tie goes to the greatest path, as the scanner's rule does (docs/RAGNART-design.md §4.2)."""
    uuid = "55555555-aaaa-4bbb-8ccc-000000000005"
    first = write_transcript(ws.root / "projA" / f"{uuid}.jsonl", uuid, "copy in projA", cwd="/x/from-a")
    second = write_transcript(ws.root / "projB" / f"{uuid}.jsonl", uuid, "copy in projB", cwd="/x/from-b")
    os.utime(first, (1_700_000_000, 1_700_000_000))
    os.utime(second, (1_700_000_000, 1_700_000_000))

    assert run(ws, now=NOW) == 0

    rows = upserts_for(hub_config, key_of(uuid))
    assert len(rows) == 1
    assert json.loads(rows[0]["metadata"])["project"] == "from-b"


def test_symlinked_transcript_is_never_followed(ws, hub_config):
    """A transcript symlink, even one pointing at a real transcript outside the root, contributes nothing."""
    outside = ws.base / "outside"
    uuid = "66666666-aaaa-4bbb-8ccc-000000000006"
    write_transcript(outside / f"{uuid}.jsonl", uuid, "outside the root")
    (ws.root / "proj").mkdir()
    os.symlink(outside / f"{uuid}.jsonl", ws.root / "proj" / f"{uuid}.jsonl")
    os.symlink(outside, ws.root / "linkdir")

    assert run(ws, now=NOW) == 0

    assert upserted_keys(hub_config) == set()


# ---------------------------------------------------------------------------
# Change detection, vanish, moves
# ---------------------------------------------------------------------------

def test_edited_session_is_upserted_again(ws, hub_config, qdrant):
    """An appended turn changes the rendered text and the hash, so the session is submitted again."""
    uuid = "77777777-aaaa-4bbb-8ccc-000000000007"
    path = write_transcript(ws.root / "proj" / f"{uuid}.jsonl", uuid, "first question")
    assert run(ws, now=NOW) == 0
    settle(hub_config, qdrant)
    assert run(ws, now=NOW) == 0
    assert state_rows(ws)[key_of(uuid)]["status"] == "completed"

    append_turn(path, uuid, "a new answer", mtime=path.stat().st_mtime + 60)
    assert run(ws, now=NOW) == 0

    rows = upserts_for(hub_config, key_of(uuid))
    assert len(rows) == 2
    assert "a new answer" in rows[-1]["text"]
    assert state_rows(ws)[key_of(uuid)]["content_hash"] == _md5(rows[-1]["text"])


def test_unchanged_session_is_not_rendered_or_submitted_again(ws, hub_config, qdrant, monkeypatch):
    """With the same size and mtime as its row, a session keeps its stored hash and is not rendered again."""
    uuid = "88888888-aaaa-4bbb-8ccc-000000000008"
    write_transcript(ws.root / "proj" / f"{uuid}.jsonl", uuid, "a question")
    assert run(ws, now=NOW) == 0
    settle(hub_config, qdrant)
    assert run(ws, now=NOW) == 0

    rendered = []
    real_parse = endpoint._parse_claude_chat
    monkeypatch.setattr(endpoint, "_parse_claude_chat", lambda path: rendered.append(path) or real_parse(path))

    assert run(ws, now=NOW) == 0

    assert rendered == [], "the hash is reused while size and mtime are unchanged"
    assert len(upserts_for(hub_config, key_of(uuid))) == 1


def test_vanished_session_is_marked_missing_then_deleted_after_grace(ws, hub_config, qdrant):
    """A vanished session is marked missing, then deleted by a run after the grace period, like a file."""
    uuid = "99999999-aaaa-4bbb-8ccc-000000000009"
    keep = "99999999-aaaa-4bbb-8ccc-000000000010"
    gone_path = write_transcript(ws.root / "proj" / f"{uuid}.jsonl", uuid, "to be removed")
    write_transcript(ws.root / "proj" / f"{keep}.jsonl", keep, "stays")
    assert run(ws, now=NOW) == 0
    settle(hub_config, qdrant)
    assert run(ws, now=NOW) == 0

    gone_path.unlink()
    assert run(ws, now=NOW) == 0
    assert state_rows(ws)[key_of(uuid)]["missing_since"] == NOW.isoformat(timespec="seconds")
    assert not any(row["kind"] == "delete" for row in intake_rows(hub_config)), "still inside the grace period"

    assert run(ws, now=NOW + timedelta(hours=25)) == 0
    deletes = [row["key"] for row in intake_rows(hub_config) if row["kind"] == "delete"]
    assert deletes == [key_of(uuid)]
    assert key_of(uuid) not in state_rows(ws)


def test_sessions_never_move_and_no_change_key_is_sent(ws, hub_config, qdrant):
    """
    A session is keyed by its uuid, so it never moves. A file renamed to another folder keeps its key and is not
    resubmitted. A new uuid with the same content is a new item, and the old one is missing, not a change_key.
    """
    uuid = "aaaaaaaa-aaaa-4bbb-8ccc-00000000000a"
    original = write_transcript(ws.root / "old-folder" / f"{uuid}.jsonl", uuid, "same bytes everywhere")
    write_transcript(ws.root / "keep" / "bbbbbbbb-aaaa-4bbb-8ccc-00000000000b.jsonl",
                     "bbbbbbbb-aaaa-4bbb-8ccc-00000000000b", "stays")
    assert run(ws, now=NOW) == 0
    settle(hub_config, qdrant)
    assert run(ws, now=NOW) == 0

    (ws.root / "new-folder").mkdir()
    os.rename(original, ws.root / "new-folder" / f"{uuid}.jsonl")
    assert run(ws, now=NOW) == 0
    assert len(upserts_for(hub_config, key_of(uuid))) == 1, "the renamed session is not resubmitted"

    # Same content, new uuid, old file gone: the new key is a new item; the old one goes missing.
    copy_uuid = "cccccccc-aaaa-4bbb-8ccc-00000000000c"
    write_transcript(ws.root / "new-folder" / f"{copy_uuid}.jsonl", copy_uuid, "same bytes everywhere")
    os.remove(ws.root / "new-folder" / f"{uuid}.jsonl")
    assert run(ws, now=NOW) == 0

    assert len(upserts_for(hub_config, key_of(copy_uuid))) == 1
    assert state_rows(ws)[key_of(uuid)]["missing_since"] == NOW.isoformat(timespec="seconds")
    assert not any(row["kind"] == "change_key" for row in intake_rows(hub_config)), "a session never moves"


def test_hub_listing_of_an_unknown_session_gets_a_delete(ws, hub_config, qdrant):
    """
    A key the hub lists with no local row is deleted, as for a file (the reconcile step).

    The first run gives the state rows, since with no local rows at all the orphan rule is not applied.
    """
    uuid = "dddddddd-aaaa-4bbb-8ccc-00000000000d"
    write_transcript(ws.root / "proj" / f"{uuid}.jsonl", uuid, "local")
    assert run(ws, now=NOW) == 0
    settle(hub_config, qdrant)
    ghost = key_of("eeeeeeee-aaaa-4bbb-8ccc-00000000000e")
    qdrant.scroll.return_value = (
        [SimpleNamespace(payload={"source_path": ghost, "content_hash": "h", "pipeline_version": "p1"})], None,
    )

    assert run(ws, now=NOW) == 0

    deletes = [row["key"] for row in intake_rows(hub_config) if row["kind"] == "delete"]
    assert deletes == [ghost]


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def test_dry_run_writes_nothing(ws, hub_config, sent, caplog):
    """A dry run makes no hub write and no state write: no intake row, no state file, and only GET requests."""
    uuid = "f0f0f0f0-aaaa-4bbb-8ccc-0000000000f0"
    write_transcript(ws.root / "proj" / f"{uuid}.jsonl", uuid, "would be submitted")
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)

    assert run(ws, now=NOW, dry_run=True) == 0

    assert intake_rows(hub_config) == []
    assert not ws.state_db.exists()
    assert {request.method for request in sent} == {"GET"}, "a dry run only reads"
    assert f"would upsert {key_of(uuid)}" in caplog.text


# ---------------------------------------------------------------------------
# Session B2 (A.18): --rebuild-from-hub for sessions
# ---------------------------------------------------------------------------

def _rendered_hash(path: Path) -> str:
    """The md5 of a transcript's rendered text, the hash the hub keeps for a session it ingested."""
    text, _metadata, _launch_dir = _parse_claude_chat(path)
    return hash_text(text)


def _listed(key: str, content_hash: str) -> list[SimpleNamespace]:
    """Two Qdrant points of one session, as the hub lists it."""
    payload = {"source_path": key, "content_hash": content_hash, "pipeline_version": "p1"}
    return [SimpleNamespace(payload=payload), SimpleNamespace(payload=payload)]


def _rebuild(ws: SimpleNamespace):
    return endpoint.rebuild_from_hub(endpoint.load_config(str(ws.config)))


def test_rebuild_restores_a_session_whose_rendered_text_matches_and_sends_nothing(ws, hub_config, qdrant, sent):
    """A session on disk whose rendered md5 equals the hub's hash is restored; the rebuild sends only the listing read."""
    uuid = "22222222-aaaa-4bbb-8ccc-000000000021"
    path = write_transcript(ws.root / "proj" / f"{uuid}.jsonl", uuid, "rebuild this")
    key = key_of(uuid)
    qdrant.scroll.return_value = (_listed(key, _rendered_hash(path)), None)
    before = len(sent)

    counts = _rebuild(ws)

    assert counts["restored"] == 1
    assert [request.method for request in sent[before:]] == ["GET"]
    row = state_rows(ws)[key]
    assert (row["status"], row["chunk_count"]) == ("completed", 2)

    # The next run re-renders the session (its placeholder stat never matches), finds the same hash, and sends nothing.
    assert run(ws, now=NOW) == 0
    assert intake_rows(hub_config) == []


def test_rebuild_leaves_a_session_whose_text_changed_without_a_row_so_it_is_submitted_again(ws, hub_config, qdrant):
    """A session whose rendered text no longer matches the hub's hash gets no row, and the next run submits it as new."""
    uuid = "22222222-aaaa-4bbb-8ccc-000000000022"
    write_transcript(ws.root / "proj" / f"{uuid}.jsonl", uuid, "changed since the hub's ingest")
    key = key_of(uuid)
    qdrant.scroll.return_value = (_listed(key, "hash-of-an-older-text"), None)

    counts = _rebuild(ws)

    assert counts["session_resubmit"] == 1
    assert state_rows(ws) == {}
    assert run(ws, now=NOW) == 0
    assert len(upserts_for(hub_config, key)) == 1


def test_rebuild_restores_a_session_not_on_disk_so_its_grace_period_runs(ws, hub_config, qdrant):
    """A session with no transcript is restored completed, and the next run marks it missing (grace starts then)."""
    keep_uuid = "22222222-aaaa-4bbb-8ccc-000000000023"
    gone_uuid = "22222222-aaaa-4bbb-8ccc-000000000024"
    keep_path = write_transcript(ws.root / "proj" / f"{keep_uuid}.jsonl", keep_uuid, "stays")
    keep_key, gone_key = key_of(keep_uuid), key_of(gone_uuid)
    # One listing holds both sessions. The gone one is listed, but has no transcript on disk.
    qdrant.scroll.return_value = (
        _listed(keep_key, _rendered_hash(keep_path)) + _listed(gone_key, "any"), None,
    )
    counts = _rebuild(ws)
    assert (counts["restored"], counts["session_gone"]) == (2, 1)

    assert run(ws, now=NOW) == 0

    assert state_rows(ws)[gone_key]["missing_since"] == NOW.isoformat(timespec="seconds")

