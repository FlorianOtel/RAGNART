"""
Payload parity for ingest_content() against the payloads ingest_file() wrote before B2 (session B2, A.5, A.13).

ingest_file() was the reference for NAS items: the daemon wrote every NAS point with it before B2. It was
deleted in A.13, after this comparison passed. Its output for six fixtures was captured then, and the
captured points are stored in tests/fixtures/payload_parity_expected.json. In that file the temporary root
of each run is written as <ROOT>, and ingested_at (the write time) is left out.

For each case the item goes through ingest_content() with the arguments ragnart/sync_submit.py sends:
  - for a file: the bytes, as a spool copy with no suffix (as the API spools them), and the declared file type;
  - for a session: the text rendered by ragnart.chat_render (the same parser the NAS side calls) and the chat
    metadata;
  - for every item: the key, owner, tag, project_id and modified_at (the ISO-8601 UTC form the API stores).

The embedder is an AsyncMock with deterministic vectors, Qdrant is a MagicMock (its upserted points are
captured), the OpenCode HTTP API is a fake urlopen, and sparse readiness is off. No live service is used.
No docling conversion runs, so there are no pdf or docx cases: those would load the converter models.

Every payload key is compared except ingested_at, which is the time of the write. The vectors and the chunk
count are compared too. A difference must be listed in EXPECTED_DIFFERENCES, or the test fails.

To regenerate the stored file, the previous ingest_file() code is needed (git history, before session B2 A.13).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import urllib.parse
import uuid
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ragnart import ingest
from ragnart.chat_render import _parse_claude_chat, _parse_opencode_session
from ragnart.ingest import IngestResult, ingest_content
from ragnart.projects import project_id_for_path
from ragnart.schema import (
    GLOBAL_PROJECT_ID,
    SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
    SOURCE_ID_NAS,
    SOURCE_ID_OPENCODE_K11,
)

OWNER = "florian"
TAG_CERTIFICATIONS = "certifications"

# The modification time of every fixture, in UNIX seconds (what the queue row holds). The same instant, as
# the ISO-8601 UTC string the API stores, is MODIFIED_AT. The stored points were written with it.
LAST_MODIFIED = 1_700_000_000.0
MODIFIED_AT = "2023-11-14T22:13:20+00:00"

OPENCODE_URL = "http://opencode.invalid:4096"
OPENCODE_SESSION_ID = "ses_abc"
CLAUDE_SESSION_ID = "6f1c2a4e-1111-4222-8333-944455556666"

# The placeholder that stands for the temporary root in the stored fixture.
ROOT_PLACEHOLDER = "<ROOT>"
FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "payload_parity_expected.json"

RAG_CFG = {
    "embedding_model": "bge-m3",
    "opencode_api_url": OPENCODE_URL,
    "admission": {
        "max_text_file_size_bytes": 5_000_000,
        "fffd_ratio_threshold": 0.005,
        "max_chunks_per_item": 5000,
    },
}

# The payload keys every point carries, besides the chat fields of a session.
FILE_PAYLOAD_KEYS = frozenset({
    "text", "parent_text", "owner", "source_path", "file_name", "file_type", "page", "chunk_index",
    "tag", "source_id", "pipeline_version", "project_id", "source_type", "modified_at", "ingested_at",
    "content_hash",
})

# Each case's known difference from the stored payloads, by payload key. An empty set means the payloads
# must be identical (apart from ingested_at). A case whose difference disappears fails the test too, so
# this list cannot go stale silently.
#
# claude_session, content_hash: the stored point carries the .jsonl file's MD5 (what ingest_file hashed).
# ingest_content hashes the rendered text, because the hub stores the hash of what it received (operator
# decision, 2026-11-19). Session points stored before B2 keep the file's MD5 until they are re-submitted.
EXPECTED_DIFFERENCES: dict[str, frozenset[str]] = {
    "plain_md": frozenset(),
    "plain_txt": frozenset(),
    "claude_session": frozenset({"content_hash"}),
    "opencode_session": frozenset(),
    "project_attached": frozenset(),
    "tagged": frozenset(),
}

CASES = tuple(EXPECTED_DIFFERENCES)


# ---------------------------------------------------------------------------
# The stored expected payloads
# ---------------------------------------------------------------------------

def _load_stored() -> dict:
    """The stored ingest_file() output, keyed by case name. Read once per test run."""
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


STORED = _load_stored()


def _restore_root(value, tmp_path: Path):
    """Replace the <ROOT> placeholder by this run's temporary root, in every string, recursively."""
    if isinstance(value, str):
        return value.replace(ROOT_PLACEHOLDER, str(tmp_path))
    if isinstance(value, list):
        return [_restore_root(item, tmp_path) for item in value]
    if isinstance(value, dict):
        return {key: _restore_root(item, tmp_path) for key, item in value.items()}
    return value


def _stored_case(name: str, tmp_path: Path) -> dict:
    """The stored points, vectors and chunk count of one case, with this run's root restored."""
    stored = STORED[name]
    return {
        "points": _restore_root(stored["points"], tmp_path),
        "vectors": stored["vectors"],
        "chunk_count": stored["chunk_count"],
    }


# ---------------------------------------------------------------------------
# Doubles: the embedder, the OpenCode HTTP API
# ---------------------------------------------------------------------------

def _fake_embed(texts, **_kwargs):
    """A deterministic vector per text, so two runs over the same text give the same vector."""
    return [[float(len(text)), 1.0, 0.0, 0.0] for text in texts]


class _FakeResponse:
    """A minimal stand-in for the response urllib returns: a JSON body and its Content-Type."""

    def __init__(self, payload: dict) -> None:
        self._body = json.dumps(payload).encode("utf-8")
        self.headers = {"Content-Type": "application/json"}

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> bool:
        return False


OPENCODE_SESSION = {
    "data": {
        "title": "Certificate renewal",
        "location": {"directory": "/mnt/nfs/Florian/proj"},
        "time": {"created": 1_700_000_000_000},
    },
}

OPENCODE_MESSAGES = [
    {"type": "user", "text": "How do I renew the certificate for the printer?"},
    {"type": "assistant", "content": [
        {"type": "reasoning", "text": "thinking, not part of the transcript"},
        {"type": "text", "text": "Renew it through the portal, then copy the new file to the printer."},
    ]},
    {"type": "user", "text": "/help"},  # A slash command: skipped by the renderer.
    {"type": "user", "text": "Thanks, and where is the portal?"},
]


def _fake_opencode_urlopen(request, timeout=None):
    """
    Answer the two OpenCode routes the renderer calls, as the 2.x server does.

    The message route returns one page with no next cursor, so the renderer's walk stops after it.
    Any other route fails the test, so a renderer that asks for something new is noticed.
    """
    parts = urllib.parse.urlsplit(request.full_url)
    if parts.path == f"/api/session/{OPENCODE_SESSION_ID}":
        return _FakeResponse(OPENCODE_SESSION)
    if parts.path == f"/api/session/{OPENCODE_SESSION_ID}/message":
        if "cursor=" in parts.query:
            return _FakeResponse({"data": [], "cursor": {"next": None}})
        return _FakeResponse({"data": OPENCODE_MESSAGES, "cursor": {"next": None}})
    raise AssertionError(f"unexpected OpenCode route in the parity test: {parts.path}")


def _doubles() -> ExitStack:
    """
    The doubles every case runs under: no embedder, no Qdrant, no OpenCode server, no sparse vectors.

    It is entered before a case builds its arguments, because a session's text is rendered (over the fake
    OpenCode API) while the arguments are being built, not only when ingest_content runs.
    """
    stack = ExitStack()
    stack.enter_context(patch.object(ingest, "ensure_collection", MagicMock()))
    stack.enter_context(patch.object(ingest, "_is_sparse_ready", lambda _client: False))
    stack.enter_context(patch.object(ingest, "embed_batch", AsyncMock(side_effect=_fake_embed)))
    stack.enter_context(patch("ragnart.chat_render.load_password", return_value="test-password"))
    stack.enter_context(patch("urllib.request.urlopen", side_effect=_fake_opencode_urlopen))
    return stack


# ---------------------------------------------------------------------------
# The content arguments of each case, as the NAS submitter sends them
# ---------------------------------------------------------------------------

def _plain_file(root: Path, rel: str, body: bytes, tag: str = "", projects: list[dict] | None = None) -> dict:
    """
    The ingest_content arguments for one NAS file at root/rel.

    The file's key is its absolute path. The bytes go in a spool copy with no suffix, as the API spools an
    upload. Its project is attached as the NAS side attaches a file: by its path, against the projects.
    """
    key = str(root / rel)
    spool_dir = root / "spool"
    spool_dir.mkdir(parents=True, exist_ok=True)
    spool = spool_dir / uuid.uuid4().hex
    spool.write_bytes(body)

    projects = projects or []
    project_id = project_id_for_path(key, [(p["id"], p["path"]) for p in projects], GLOBAL_PROJECT_ID)
    return {
        "source_id": SOURCE_ID_NAS,
        "connector": "filesystem",
        "key": key,
        "owner": OWNER,
        "spool_path": str(spool),
        "file_type": Path(rel).suffix.lstrip(".").lower(),
        "tag": tag,
        "project_id": project_id,
        "modified_at": MODIFIED_AT,
    }


def _claude_session(root: Path) -> Path:
    """
    Write a Claude Code transcript in the layout the scanner reads: .../.claude/projects/<mangled>/<uuid>.jsonl.

    The .claude directory is found by anchoring on the 'projects' component, so it is under root.
    """
    project_dir = root / "home" / ".claude" / "projects" / "-mnt-nfs-Florian-proj"
    project_dir.mkdir(parents=True, exist_ok=True)
    path = project_dir / f"{CLAUDE_SESSION_ID}.jsonl"
    lines = [
        {"type": "user", "sessionId": CLAUDE_SESSION_ID, "cwd": "/mnt/nfs/Florian/proj",
         "timestamp": "2026-11-18T10:00:00Z", "message": {"role": "user", "content": "Summarise the renewal steps."}},
        {"type": "assistant", "sessionId": CLAUDE_SESSION_ID, "cwd": "/mnt/nfs/Florian/proj",
         "message": {"model": "claude-test", "content": [{"type": "text", "text": "Here are the renewal steps."}]}},
        {"type": "user", "sessionId": CLAUDE_SESSION_ID, "message": {"role": "user", "content": "Thanks."}},
    ]
    path.write_text("\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8")
    os.utime(path, (LAST_MODIFIED, LAST_MODIFIED))
    return path


def _claude_case(root: Path) -> dict:
    """The arguments for the Claude session: its rendered text and chat metadata. No project is registered, so 'global'."""
    path = _claude_session(root)
    key = f"claude-session://{CLAUDE_SESSION_ID}"
    # The sender renders the session with the same parser the NAS side calls, and sends its text and metadata.
    text, chat_meta, _launch_dir = _parse_claude_chat(path)
    return {
        "source_id": SOURCE_ID_CLAUDE_LINUX_SHARED_NFS,
        "connector": "claude_code",
        "key": key,
        "owner": OWNER,
        "text": text,
        "chat_meta": chat_meta,
        "tag": "",
        "project_id": GLOBAL_PROJECT_ID,
        "modified_at": MODIFIED_AT,
    }


def _opencode_case() -> dict:
    """The arguments for the OpenCode session: its rendered text and chat metadata. No project is registered, so 'global'."""
    key = f"opencode://{OPENCODE_SESSION_ID}"
    # The sender renders the session over the same OpenCode API the NAS side reads.
    text, chat_meta, _launch_dir = _parse_opencode_session(key, OPENCODE_URL, None)
    return {
        "source_id": SOURCE_ID_OPENCODE_K11,
        "connector": "opencode",
        "key": key,
        "owner": OWNER,
        "text": text,
        "chat_meta": chat_meta,
        "tag": "",
        "project_id": GLOBAL_PROJECT_ID,
        "modified_at": MODIFIED_AT,
    }


def _build_content_args(name: str, root: Path) -> dict:
    """The ingest_content arguments of one named case. Must run while _doubles() is in place."""
    if name == "plain_md":
        return _plain_file(root, "notes/todo.md", b"# Todo\n\nRenew the printer certificate before March.\n")
    if name == "plain_txt":
        return _plain_file(root, "notes/minutes.txt", b"Minutes of the household meeting.\nThe boiler is due a service.\n")
    if name == "project_attached":
        # The project's path encloses the file, so the sender attaches it and sends the project's id.
        projects = [{"id": "proj-1", "path": str(root / "proj")}]
        return _plain_file(root, "proj/plan.md", b"# Plan\n\nThe project plan for the printer.\n", projects=projects)
    if name == "tagged":
        return _plain_file(root, "notes/course.md", b"# Course\n\nA note about a certificate course.\n", tag=TAG_CERTIFICATIONS)
    if name == "claude_session":
        return _claude_case(root)
    if name == "opencode_session":
        return _opencode_case()
    raise ValueError(f"no such parity case: {name}")


# ---------------------------------------------------------------------------
# The run and the comparison
# ---------------------------------------------------------------------------

def _run_content(content_args: dict) -> tuple[list[dict], list[list[float]], IngestResult]:
    """Run ingest_content() with a mock Qdrant client. Returns the payloads and vectors it upserted, and its result."""
    client = MagicMock()
    result = asyncio.run(ingest_content(qdrant_client=client, rag_cfg=RAG_CFG, **content_args))
    points = [point for call in client.upsert.call_args_list for point in call.kwargs["points"]]
    return [point.payload for point in points], [list(point.vector) for point in points], result


def _compare_with_stored(name: str, tmp_path: Path, points: list[dict], vectors: list[list[float]],
                         result: IngestResult) -> set[str]:
    """
    Compare ingest_content's points with the stored ingest_file points, key by key.

    Asserts the points line up one to one (same count, same order), the chunk count matches, every payload has
    the same keys, and every vector is equal. Returns the set of payload keys that differ, which must equal the
    case's expected difference exactly.
    """
    stored = _stored_case(name, tmp_path)
    assert result.status == "completed", result
    assert len(points) == len(stored["points"]) > 0, name
    assert result.chunk_count == len(points) == stored["chunk_count"], name

    observed: set[str] = set()
    for index, (stored_payload, payload) in enumerate(zip(stored["points"], points)):
        # ingested_at is the time of the write. The stored points have it removed, and the live point must still carry it.
        assert "ingested_at" in payload, name
        stored_compare = dict(stored_payload)
        compare = {k: v for k, v in payload.items() if k != "ingested_at"}
        assert set(stored_compare) == set(compare), (
            f"{name} point {index}: keys differ: "
            f"only in the stored payload {sorted(set(stored_compare) - set(compare))}, "
            f"only in ingest_content {sorted(set(compare) - set(stored_compare))}"
        )
        for key in stored_compare:
            if stored_compare[key] != compare[key]:
                observed.add(key)
        assert vectors[index] == stored["vectors"][index], f"{name} point {index}: vectors differ"
    return observed


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", CASES)
def test_ingest_content_writes_the_stored_ingest_file_payload(name, tmp_path):
    """The same item: every payload key except ingested_at equal to the stored ingest_file point, and only the expected differences."""
    root = tmp_path / name
    root.mkdir()
    with _doubles():
        content_args = _build_content_args(name, root)
        points, vectors, result = _run_content(content_args)

    observed = _compare_with_stored(name, tmp_path, points, vectors, result)

    assert observed == set(EXPECTED_DIFFERENCES[name]), (
        f"{name}: payload keys that differ are {sorted(observed)}, expected {sorted(EXPECTED_DIFFERENCES[name])}"
    )


@pytest.mark.parametrize("name", CASES)
def test_payload_key_set_is_the_documented_one(name, tmp_path):
    """
    Each point carries the base field set (FILE_PAYLOAD_KEYS), plus the chat fields of a session.

    A session adds its metadata (session_id, session_title, and project for Claude Code). A file adds nothing.
    A new key fails this test, so the set is always known.
    """
    root = tmp_path / name
    root.mkdir()
    with _doubles():
        points, _vectors, _result = _run_content(_build_content_args(name, root))

    keys = set(points[0])
    session_keys = keys - FILE_PAYLOAD_KEYS
    if name == "claude_session":
        assert session_keys == {"session_id", "project", "session_title"}
    elif name == "opencode_session":
        assert session_keys == {"session_id", "session_title"}
    else:
        assert session_keys == set()
    assert FILE_PAYLOAD_KEYS <= keys


def test_claude_session_content_hash_is_the_recorded_exception(tmp_path):
    """
    The one known difference, stated on its own.

    The stored point carries the MD5 of the session's .jsonl file, as ingest_file wrote it. ingest_content carries the
    MD5 of the rendered text, and the result says the same. Both values are asserted, so the reason stays visible.
    """
    root = tmp_path / "claude_session"
    root.mkdir()
    with _doubles():
        content_args = _build_content_args("claude_session", root)
        points, _vectors, result = _run_content(content_args)

    session_file = root / "home" / ".claude" / "projects" / "-mnt-nfs-Florian-proj" / f"{CLAUDE_SESSION_ID}.jsonl"
    stored_hash = STORED["claude_session"]["points"][0]["content_hash"]
    text_hash = hashlib.md5(content_args["text"].encode("utf-8")).hexdigest()

    assert stored_hash == hashlib.md5(session_file.read_bytes()).hexdigest()
    assert points[0]["content_hash"] == text_hash
    assert result.content_hash == text_hash
    assert stored_hash != text_hash
