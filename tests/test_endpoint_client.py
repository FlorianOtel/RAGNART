"""
Tests for ragnart/endpoint_client.py: the HubClient that an endpoint host uses to talk to the ingestion API.

The client is driven against the real ragnart.ingest_api app, through starlette's TestClient, which is
an httpx.Client. So each call goes through the API's auth gate, its validation and its registry writes.
A request hook on that client records every request as it is sent, so the method, path, Authorization
header and body of each call are asserted on the wire, not inferred.

Everything runs against tmp files and a mocked Qdrant client. No live hub, Qdrant or service is touched.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

import ragnart.cli as cli
import ragnart.ingest_api as ingest_api
from ragnart.cli import registry_db_path
from ragnart.endpoint_client import HubClient, HubError
from ragnart.registry import RegistryDB

# The secrets of the seeded endpoint sources. The registry stores only their SHA-256 hashes.
MBP_SECRET = "mbp-secret-for-client-tests"
CLAUDE_SECRET = "claude-secret-for-client-tests"
WRONG_SECRET = "not-a-secret-the-hub-knows"

BASE_URL = "http://hub.test:8101"


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@pytest.fixture
def config(tmp_path, monkeypatch) -> dict:
    """A tmp ragnart.yaml with the include list and admission caps the API reads. cli reads it from here."""
    cfg = {
        "db_base_path": str(tmp_path / "db"),
        "rag": {
            "qdrant_url": "http://qdrant.invalid:6333",
            "admission": {
                "max_text_file_size_bytes": 1000,
                "max_chunks_per_item": 5000,
                "fffd_ratio_threshold": 0.005,
            },
            "scanner": {"include_extensions": [".md", ".pdf"]},
        },
    }
    path = tmp_path / "ragnart.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    monkeypatch.setattr(cli, "CONFIG_PATH", path)
    return cfg


@pytest.fixture
def seeded(config):
    """A hub registry with the two endpoint sources the tests submit as: MBP and claude-MBP."""
    registry = RegistryDB(registry_db_path(config))
    registry.register_source("MBP", "filesystem", "endpoint", "florian", _sha256(MBP_SECRET))
    registry.register_source("claude-MBP", "claude_code", "endpoint", "florian", _sha256(CLAUDE_SECRET))
    registry.close()
    return config


@pytest.fixture
def qdrant(monkeypatch) -> MagicMock:
    """A mocked Qdrant client. The API's get_client() returns it, so no server is reached."""
    client = MagicMock(name="qdrant_client")
    monkeypatch.setattr(ingest_api, "get_client", lambda url, timeout=60: client)
    return client


@pytest.fixture
def hub(seeded, qdrant):
    """The ingestion API as an httpx client: the app's lifespan has run, and every request goes through it."""
    with TestClient(ingest_api.app) as client:
        yield client


@pytest.fixture
def sent(hub) -> list:
    """Every request the hub client sends, in order, recorded by an httpx request hook."""
    requests: list[httpx.Request] = []
    hub.event_hooks["request"].append(requests.append)
    return requests


def _secret_file(tmp_path: Path, name: str, secret: str) -> str:
    """Write a secret file as an operator would: the secret and a trailing newline."""
    path = tmp_path / name
    path.write_text(secret + "\n", encoding="utf-8")
    return str(path)


@pytest.fixture
def mbp(hub, tmp_path) -> HubClient:
    """The client of the MBP (filesystem) source."""
    return HubClient(BASE_URL, _secret_file(tmp_path, "mbp.secret", MBP_SECRET), http_client=hub)


@pytest.fixture
def claude(hub, tmp_path) -> HubClient:
    """The client of the claude-MBP (claude_code) source."""
    return HubClient(BASE_URL, _secret_file(tmp_path, "claude.secret", CLAUDE_SECRET), http_client=hub)


def _row(config: dict, request_id: str) -> dict:
    """The intake row of one request, read from the registry file the API wrote."""
    registry = RegistryDB(registry_db_path(config), read_only=True)
    try:
        return registry.intake_by_id(request_id)
    finally:
        registry.close()


def _body(request: httpx.Request) -> dict:
    """The JSON body of a request that was sent as JSON."""
    return json.loads(request.content)


# ---------------------------------------------------------------------------
# One method per endpoint: method, path, auth header and body, as sent
# ---------------------------------------------------------------------------

def test_upsert_bytes_streams_the_file_with_the_bearer_secret(mbp, sent, config, tmp_path):
    """upsert_bytes posts multipart to /v1/upsert/bytes: the file's bytes, key, type and modified_at."""
    local = tmp_path / "notes.md"
    local.write_bytes(b"# Notes\n\nstreamed from the local disk\n")

    answer = mbp.upsert_bytes(str(local), "/Users/fotel/MBP/notes.md", "md", modified_at="2023-11-14T22:13:20+00:00")

    request = sent[-1]
    assert request.method == "POST"
    assert request.url.path == "/v1/upsert/bytes"
    assert request.headers["Authorization"] == f"Bearer {MBP_SECRET}"
    assert request.headers["content-type"].startswith("multipart/form-data")

    # The multipart body is streamed, so its fields are checked on what the hub recorded.
    assert answer["status"] == "received"
    row = _row(config, answer["request_id"])
    assert row["source_id"] == "MBP"
    assert row["kind"] == "upsert"
    assert row["key"] == "/Users/fotel/MBP/notes.md"
    assert row["file_type"] == "md"
    assert row["modified_at"] == "2023-11-14T22:13:20+00:00"
    assert Path(row["spool_path"]).read_bytes() == b"# Notes\n\nstreamed from the local disk\n"


def test_upsert_bytes_without_modified_at_stores_none(mbp, config, tmp_path):
    """Without modified_at the request is still accepted, and the row stores none."""
    local = tmp_path / "plain.md"
    local.write_bytes(b"plain")

    answer = mbp.upsert_bytes(str(local), "/Users/fotel/MBP/plain.md", "md")

    assert _row(config, answer["request_id"])["modified_at"] is None


def test_upsert_text_posts_key_text_metadata_and_modified_at(claude, sent, config):
    """upsert_text posts JSON to /v1/upsert/text; the chat metadata and modified_at land on the row."""
    answer = claude.upsert_text(
        "claude-session://abc-123", "the rendered session",
        metadata={"session_id": "abc-123", "project": "demo"},
        modified_at="2023-11-14T22:13:20+00:00",
    )

    request = sent[-1]
    assert request.method == "POST"
    assert request.url.path == "/v1/upsert/text"
    assert request.headers["Authorization"] == f"Bearer {CLAUDE_SECRET}"
    assert _body(request) == {
        "key": "claude-session://abc-123",
        "text": "the rendered session",
        "metadata": {"session_id": "abc-123", "project": "demo"},
        "modified_at": "2023-11-14T22:13:20+00:00",
    }
    row = _row(config, answer["request_id"])
    assert row["source_id"] == "claude-MBP"
    assert row["text"] == "the rendered session"
    assert row["metadata"] == {"session_id": "abc-123", "project": "demo"}
    assert row["modified_at"] == "2023-11-14T22:13:20+00:00"


def test_delete_posts_the_key(mbp, sent, config):
    """delete posts {key} to /v1/delete, and the row is a delete of that item."""
    answer = mbp.delete("/Users/fotel/MBP/gone.md")

    request = sent[-1]
    assert (request.method, request.url.path) == ("POST", "/v1/delete")
    assert request.headers["Authorization"] == f"Bearer {MBP_SECRET}"
    assert _body(request) == {"key": "/Users/fotel/MBP/gone.md"}
    row = _row(config, answer["request_id"])
    assert (row["kind"], row["key"]) == ("delete", "/Users/fotel/MBP/gone.md")


def test_change_key_posts_both_keys(mbp, sent, config):
    """change_key posts {key, new_key} to /v1/change-key."""
    answer = mbp.change_key("/Users/fotel/MBP/a.md", "/Users/fotel/MBP/sub/a.md")

    request = sent[-1]
    assert (request.method, request.url.path) == ("POST", "/v1/change-key")
    assert _body(request) == {"key": "/Users/fotel/MBP/a.md", "new_key": "/Users/fotel/MBP/sub/a.md"}
    row = _row(config, answer["request_id"])
    assert (row["kind"], row["new_key"]) == ("change_key", "/Users/fotel/MBP/sub/a.md")


def test_change_metadata_posts_the_owner_inside_metadata(mbp, sent, config):
    """change_metadata posts {key, metadata: {owner}} to /v1/change-metadata."""
    answer = mbp.change_metadata("/Users/fotel/MBP/a.md", "eva")

    request = sent[-1]
    assert (request.method, request.url.path) == ("POST", "/v1/change-metadata")
    assert _body(request) == {"key": "/Users/fotel/MBP/a.md", "metadata": {"owner": "eva"}}
    assert _row(config, answer["request_id"])["metadata"] == {"owner": "eva"}


def test_status_gets_the_request_and_refuses_another_sources_request(mbp, claude, sent):
    """status gets /v1/status/<id>; the owning source reads it, another source gets HubError 404."""
    request_id = mbp.delete("/Users/fotel/MBP/a.md")["request_id"]

    status = mbp.status(request_id)

    request = sent[-1]
    assert (request.method, request.url.path) == ("GET", f"/v1/status/{request_id}")
    assert request.headers["Authorization"] == f"Bearer {MBP_SECRET}"
    assert status["status"] == "received"
    assert status["kind"] == "delete"

    with pytest.raises(HubError) as exc_info:
        claude.status(request_id)
    assert exc_info.value.status_code == 404


def test_items_gets_the_list_and_returns_only_the_items(mbp, sent, qdrant):
    """items gets /v1/items and returns the list under 'items', derived from this source's points."""
    point = SimpleNamespace(payload={
        "source_path": "/Users/fotel/MBP/a.md", "content_hash": "h1", "pipeline_version": "p1",
    })
    qdrant.scroll.return_value = ([point], None)

    items = mbp.items()

    assert (sent[-1].method, sent[-1].url.path) == ("GET", "/v1/items")
    assert items == [{"key": "/Users/fotel/MBP/a.md", "chunk_count": 1, "content_hash": "h1", "pipeline_version": "p1"}]


def test_info_gets_the_pipeline_and_limits(mbp, sent):
    """info gets /v1/info and returns the pipeline version, the include list and the admission limits."""
    info = mbp.info()

    assert (sent[-1].method, sent[-1].url.path) == ("GET", "/v1/info")
    assert info["pipeline_version"] == "p1"
    assert info["accepted_file_types"] == [".md", ".pdf"]
    assert info["admission"]["max_text_file_size_bytes"] == 1000


def test_publish_project_returns_the_409_refusal_body(mbp, sent):
    """publish_project posts to /v1/projects; the hub's 409 refusal comes back as a dict, not as an error."""
    refusal = mbp.publish_project("proj-1", "My Project", path="/Users/fotel/MBP/proj")

    assert (sent[-1].method, sent[-1].url.path) == ("POST", "/v1/projects")
    assert _body(sent[-1]) == {"id": "proj-1", "name": "My Project", "path": "/Users/fotel/MBP/proj"}
    assert refusal["refused"] is True
    assert refusal["reason"].startswith("MBP cannot hold ragnart projects")


# ---------------------------------------------------------------------------
# Failures: HubError carries the status and the hub's detail
# ---------------------------------------------------------------------------

def test_wrong_secret_raises_hub_error_401(hub, tmp_path):
    """A client holding a secret the hub does not know is refused with 401."""
    client = HubClient(BASE_URL, _secret_file(tmp_path, "wrong.secret", WRONG_SECRET), http_client=hub)

    with pytest.raises(HubError) as exc_info:
        client.info()

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "missing or invalid source secret"


def test_other_non_2xx_raises_hub_error_with_the_hub_detail(mbp, tmp_path):
    """A 422 from the hub (a file type it does not ingest) raises HubError carrying the hub's detail."""
    local = tmp_path / "a.exe"
    local.write_bytes(b"x")

    with pytest.raises(HubError) as exc_info:
        mbp.upsert_bytes(str(local), "/Users/fotel/MBP/a.exe", "exe")

    assert exc_info.value.status_code == 422
    assert "not accepted" in exc_info.value.detail


# ---------------------------------------------------------------------------
# The secret: read once, stripped, never logged, never in repr or an error
# ---------------------------------------------------------------------------

def test_secret_is_read_once_from_the_file(hub, tmp_path):
    """The secret is read when the client is made. Deleting the file afterwards does not stop the client."""
    secret_path = Path(_secret_file(tmp_path, "once.secret", MBP_SECRET))
    client = HubClient(BASE_URL, secret_path, http_client=hub)

    secret_path.unlink()

    assert client.info()["pipeline_version"] == "p1"


def test_empty_secret_file_is_refused_without_naming_a_secret(hub, tmp_path):
    """An empty secret file is a ValueError at construction; the message names the file only."""
    empty = tmp_path / "empty.secret"
    empty.write_text("  \n", encoding="utf-8")

    with pytest.raises(ValueError, match="is empty"):
        HubClient(BASE_URL, empty, http_client=hub)


def test_secret_never_appears_in_repr_errors_or_logs(mbp, tmp_path, caplog):
    """The secret is absent from repr(client), from a HubError's text and repr, and from every log record."""
    caplog.set_level(logging.DEBUG)
    local = tmp_path / "a.exe"
    local.write_bytes(b"x")

    with pytest.raises(HubError) as exc_info:
        mbp.upsert_bytes(str(local), "/Users/fotel/MBP/a.exe", "exe")
    mbp.info()

    error = exc_info.value
    for text in (repr(mbp), str(error), repr(error), caplog.text):
        assert MBP_SECRET not in text
