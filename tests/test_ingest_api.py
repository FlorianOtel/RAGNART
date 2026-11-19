"""
Tests for ragnart/ingest_api.py: the auth gate, the writes (intake rows and spool files), the
key and connector rules, the status and items reads, info, and the refused project publish.

Everything runs against tmp files and a mocked Qdrant client. The hub registry is a real
RegistryDB in tmp_path; ragnart.yaml is a tmp file that the cli module reads through
ragnart.cli.CONFIG_PATH. No live database, no Qdrant server and no service is touched.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml
from fastapi.testclient import TestClient
from qdrant_client.models import FieldCondition

import ragnart.cli as cli
import ragnart.ingest_api as ingest_api
from ragnart.cli import registry_db_path
from ragnart.registry import RegistryDB

# Secrets used by the seeded sources. The registry stores only their hashes.
MBP_SECRET = "mbp-secret-for-tests"
CLAUDE_SECRET = "claude-secret-for-tests"
HUB_SECRET = "hub-secret-for-tests"

# A text-like size cap small enough to exceed in a test.
TEXT_CAP_BYTES = 1000


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _auth(secret: str) -> dict:
    return {"Authorization": f"Bearer {secret}"}


@pytest.fixture
def config(tmp_path, monkeypatch) -> dict:
    """Write a tmp ragnart.yaml and point the cli module at it. Returns the config dict."""
    cfg = {
        "db_base_path": str(tmp_path / "db"),
        "rag": {
            "qdrant_url": "http://qdrant.invalid:6333",
            "admission": {
                "max_text_file_size_bytes": TEXT_CAP_BYTES,
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
    """
    A hub registry with two endpoint sources and one hub source.

    MBP is a filesystem source, claude-MBP a claude_code source. 'hubby' is a hub source that
    has a secret hash: it must still never authenticate, because it is not an endpoint source.
    """
    registry = RegistryDB(registry_db_path(config))
    registry.register_source("MBP", "filesystem", "endpoint", "florian", _sha256(MBP_SECRET))
    registry.register_source("claude-MBP", "claude_code", "endpoint", "florian", _sha256(CLAUDE_SECRET))
    registry.register_source("hubby", "filesystem", "hub", None, _sha256(HUB_SECRET))
    registry.close()
    return config


@pytest.fixture
def qdrant(monkeypatch) -> MagicMock:
    """A mocked Qdrant client. get_client() in the API returns it, so no server is reached."""
    client = MagicMock(name="qdrant_client")
    monkeypatch.setattr(ingest_api, "get_client", lambda url, timeout=60: client)
    return client


@pytest.fixture
def api(seeded, qdrant):
    """A TestClient whose lifespan has run: the registry is open and the spool directory exists."""
    with TestClient(ingest_api.app) as client:
        yield client


def _registry(config) -> RegistryDB:
    """Open the same registry file read-only, to inspect what a request wrote."""
    return RegistryDB(registry_db_path(config), read_only=True)


def _spool_dir(config) -> Path:
    """The spool directory, as the API lays it out under db_base_path."""
    return Path(config["db_base_path"]) / "spool"


def _spool_files(config) -> list:
    spool = _spool_dir(config)
    return sorted(spool.iterdir()) if spool.exists() else []


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def test_bytes_upsert_is_accepted_and_spooled(api, config):
    """A valid bytes upsert returns 202, records a received row and spools the bytes."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("a.md", b"hello world", "text/markdown")},
        data={"key": "/Users/fotel/MBP/a.md", "file_type": "md"},
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "received"
    request_id = body["request_id"]

    spool_file = _spool_dir(config) / request_id
    assert spool_file.read_bytes() == b"hello world"

    registry = _registry(config)
    try:
        row = registry.intake_by_id(request_id)
    finally:
        registry.close()
    assert row["status"] == "received"
    assert row["kind"] == "upsert"
    assert row["source_id"] == "MBP"
    assert row["key"] == "/Users/fotel/MBP/a.md"
    assert row["file_type"] == "md"
    assert row["spool_path"] == str(spool_file)


def test_bytes_upsert_metadata_is_stored_when_allowed(api, config):
    """Upsert metadata with an allowed chat field is stored on the intake row."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "/Users/fotel/MBP/a.md", "file_type": "md", "metadata": json.dumps({"project": "p"})},
    )

    registry = _registry(config)
    try:
        row = registry.intake_by_id(response.json()["request_id"])
    finally:
        registry.close()
    assert row["metadata"] == {"project": "p"}


def test_text_upsert_is_stored_inline_with_metadata(api, config):
    """A claude_code text upsert stores the text and its metadata on the row. No spool file is made."""
    response = api.post(
        "/v1/upsert/text",
        headers=_auth(CLAUDE_SECRET),
        json={
            "key": "claude-session://abc-123",
            "text": "the session text",
            "metadata": {"session_id": "abc-123", "project": "demo"},
        },
    )

    assert response.status_code == 202
    registry = _registry(config)
    try:
        row = registry.intake_by_id(response.json()["request_id"])
    finally:
        registry.close()
    assert row["source_id"] == "claude-MBP"
    assert row["kind"] == "upsert"
    assert row["text"] == "the session text"
    assert row["metadata"] == {"session_id": "abc-123", "project": "demo"}
    assert row["spool_path"] is None
    assert _spool_files(config) == []


def test_delete_change_key_and_change_metadata_return_202(api, config):
    """The three other writes each record one row of their kind and return 202."""
    delete = api.post("/v1/delete", headers=_auth(MBP_SECRET), json={"key": "/Users/fotel/MBP/a.md"})
    rename = api.post(
        "/v1/change-key",
        headers=_auth(MBP_SECRET),
        json={"key": "/Users/fotel/MBP/a.md", "new_key": "/Users/fotel/MBP/b.md"},
    )
    owner = api.post(
        "/v1/change-metadata",
        headers=_auth(MBP_SECRET),
        json={"key": "/Users/fotel/MBP/b.md", "metadata": {"owner": "eva"}},
    )

    assert [delete.status_code, rename.status_code, owner.status_code] == [202, 202, 202]
    registry = _registry(config)
    try:
        kinds = [registry.intake_by_id(r.json()["request_id"])["kind"] for r in (delete, rename, owner)]
    finally:
        registry.close()
    assert kinds == ["delete", "change_key", "change_metadata"]


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("header", [None, {"Authorization": "Bearer wrong-secret"},
                                    {"Authorization": "Basic bWJwOnNlY3JldA=="},
                                    {"Authorization": "Bearer "}])
def test_wrong_or_absent_token_is_401(api, header):
    """A missing, wrong or malformed token is refused with 401 on every route."""
    headers = header or {}
    assert api.get("/v1/info", headers=headers).status_code == 401
    assert api.post("/v1/delete", headers=headers, json={"key": "/a"}).status_code == 401


def test_401_names_the_bearer_scheme(api):
    """The 401 carries WWW-Authenticate: Bearer, as HTTP asks."""
    response = api.get("/v1/info")
    assert response.headers["www-authenticate"] == "Bearer"


def test_hub_source_cannot_authenticate(api):
    """A hub source has a secret hash in the registry, and still cannot authenticate: it is not an endpoint."""
    assert api.get("/v1/info", headers=_auth(HUB_SECRET)).status_code == 401


def test_body_cannot_name_a_source(api, config):
    """A source id in the body is ignored: the request is recorded under the authenticated source."""
    response = api.post(
        "/v1/delete",
        headers=_auth(MBP_SECRET),
        json={"key": "/Users/fotel/MBP/a.md", "source_id": "claude-MBP"},
    )
    registry = _registry(config)
    try:
        row = registry.intake_by_id(response.json()["request_id"])
    finally:
        registry.close()
    assert row["source_id"] == "MBP"


# ---------------------------------------------------------------------------
# Admission pre-check, connector and key rules
# ---------------------------------------------------------------------------

def test_oversize_text_like_upload_is_413_and_not_spooled(api, config):
    """A text-like upload above the cap is refused with 413, and no spool file is left."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("big.md", b"a" * (TEXT_CAP_BYTES + 1), "text/markdown")},
        data={"key": "/Users/fotel/MBP/big.md", "file_type": "md"},
    )

    assert response.status_code == 413
    assert _spool_files(config) == []


def test_oversize_docling_upload_is_not_capped_here(api, config):
    """A PDF above the text cap is not text-like, so it is spooled: the worker checks it, not the API."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("big.pdf", b"%PDF" + b"a" * TEXT_CAP_BYTES, "application/pdf")},
        data={"key": "/Users/fotel/MBP/big.pdf", "file_type": "pdf"},
    )
    assert response.status_code == 202
    assert len(_spool_files(config)) == 1


@pytest.mark.parametrize("file_type, stored", [("md", "md"), (".MD", "md"), ("PDF", "pdf")])
def test_file_type_is_canonicalised_and_stored(api, config, file_type, stored):
    """F2: a type in the include list is accepted in any case, with or without a dot, and stored canonically."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("a.x", b"x", "application/octet-stream")},
        data={"key": "/Users/fotel/MBP/a.x", "file_type": file_type},
    )

    assert response.status_code == 202
    registry = _registry(config)
    try:
        row = registry.intake_by_id(response.json()["request_id"])
    finally:
        registry.close()
    assert row["file_type"] == stored


@pytest.mark.parametrize("file_type", ["exe", "txt", "", "md/../../x", "."])
def test_file_type_outside_include_extensions_is_422_and_not_spooled(api, config, file_type):
    """F2: a type the hub does not ingest is refused with 422 before anything is spooled."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("a.x", b"x", "application/octet-stream")},
        data={"key": "/Users/fotel/MBP/a.x", "file_type": file_type},
    )

    assert response.status_code == 422
    assert _spool_files(config) == []


def test_bytes_upsert_stores_modified_at_in_utc(api, config):
    """F1: a bytes upsert's modified_at is stored as an ISO-8601 UTC string, converted from its own offset."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "/Users/fotel/MBP/a.md", "file_type": "md", "modified_at": "2023-11-14T23:13:20+01:00"},
    )

    assert response.status_code == 202
    registry = _registry(config)
    try:
        row = registry.intake_by_id(response.json()["request_id"])
    finally:
        registry.close()
    assert row["modified_at"] == "2023-11-14T22:13:20+00:00"


def test_text_upsert_stores_modified_at(api, config):
    """F1: a text upsert carries modified_at in its JSON body, stored the same way."""
    response = api.post(
        "/v1/upsert/text",
        headers=_auth(CLAUDE_SECRET),
        json={"key": "claude-session://abc-123", "text": "t", "modified_at": "2023-11-14T22:13:20Z"},
    )

    assert response.status_code == 202
    registry = _registry(config)
    try:
        row = registry.intake_by_id(response.json()["request_id"])
    finally:
        registry.close()
    assert row["modified_at"] == "2023-11-14T22:13:20+00:00"


# An empty multipart form field is read as absent by Starlette, so "" is not listed for the bytes route.
@pytest.mark.parametrize("value", ["yesterday", "2023-11-14T22:13:20", "2023-13-45T00:00:00+00:00"])
def test_malformed_or_naive_modified_at_is_422_and_not_spooled(api, config, value):
    """F1: modified_at must be ISO-8601 with a UTC offset. Anything else is refused with 422, nothing spooled."""
    bytes_response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "/Users/fotel/MBP/a.md", "file_type": "md", "modified_at": value},
    )
    text_response = api.post(
        "/v1/upsert/text",
        headers=_auth(CLAUDE_SECRET),
        json={"key": "claude-session://abc-123", "text": "t", "modified_at": value},
    )

    assert bytes_response.status_code == 422
    assert text_response.status_code == 422
    assert _spool_files(config) == []


def test_change_key_from_claude_source_is_422_and_records_nothing(api, config):
    """F4: sessions are keyed by id and never move, so a claude_code change_key is refused."""
    response = api.post(
        "/v1/change-key",
        headers=_auth(CLAUDE_SECRET),
        json={"key": "claude-session://abc", "new_key": "claude-session://def"},
    )

    assert response.status_code == 422
    assert "never move" in response.json()["detail"]
    registry = _registry(config)
    try:
        count = registry._conn.execute("SELECT COUNT(*) FROM intake").fetchone()[0]
    finally:
        registry.close()
    assert count == 0


def test_text_upsert_from_filesystem_source_is_422(api):
    """A filesystem source sends bytes, so a text upsert from it is a connector mismatch."""
    response = api.post(
        "/v1/upsert/text",
        headers=_auth(MBP_SECRET),
        json={"key": "/Users/fotel/MBP/a.md", "text": "x"},
    )
    assert response.status_code == 422


def test_bytes_upsert_from_claude_source_is_422(api):
    """A claude_code source sends sessions as text, so a bytes upsert from it is a connector mismatch."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(CLAUDE_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "claude-session://abc", "file_type": "md"},
    )
    assert response.status_code == 422


@pytest.mark.parametrize("path, secret, body", [
    # A filesystem key must be absolute.
    ("/v1/delete", MBP_SECRET, {"key": "relative/a.md"}),
    # A claude_code key must carry the session prefix.
    ("/v1/upsert/text", CLAUDE_SECRET, {"key": "/abs/path.md", "text": "x"}),
    # change-key checks the new key as well as the old one.
    ("/v1/change-key", MBP_SECRET, {"key": "/Users/fotel/MBP/a.md", "new_key": "claude-session://x"}),
    # change-metadata checks its key too.
    ("/v1/change-metadata", MBP_SECRET, {"key": "x", "metadata": {"owner": "eva"}}),
])
def test_key_outside_source_namespace_is_422(api, path, secret, body):
    """A key outside the source's namespace is refused with 422."""
    assert api.post(path, headers=_auth(secret), json=body).status_code == 422


def test_bytes_upsert_with_relative_key_is_422(api, config):
    """A bytes upsert from a filesystem source needs an absolute key; nothing is spooled."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "a.md", "file_type": "md"},
    )
    assert response.status_code == 422
    assert _spool_files(config) == []


def test_change_metadata_with_non_owner_field_is_422(api):
    """Only owner may change in B1. Any other field, alone or beside owner, is refused."""
    url = "/v1/change-metadata"
    key = "/Users/fotel/MBP/a.md"
    assert api.post(url, headers=_auth(MBP_SECRET), json={"key": key, "metadata": {"owner": "eva"}}).status_code == 202
    assert api.post(url, headers=_auth(MBP_SECRET),
                    json={"key": key, "metadata": {"owner": "eva", "tag": "x"}}).status_code == 422
    assert api.post(url, headers=_auth(MBP_SECRET),
                    json={"key": key, "metadata": {"tag": "x"}}).status_code == 422


def test_upsert_metadata_outside_chat_fields_is_422(api):
    """Upsert metadata cannot set a field the hub owns, such as source_id, because the worker spreads it into points."""
    response = api.post(
        "/v1/upsert/text",
        headers=_auth(CLAUDE_SECRET),
        json={"key": "claude-session://abc", "text": "x", "metadata": {"source_id": "nas"}},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def test_status_of_own_request_is_200_and_another_sources_is_404(api):
    """A source reads its own request's status. Another source's request, or an unknown id, is 404."""
    posted = api.post("/v1/delete", headers=_auth(MBP_SECRET), json={"key": "/Users/fotel/MBP/a.md"})
    request_id = posted.json()["request_id"]

    own = api.get(f"/v1/status/{request_id}", headers=_auth(MBP_SECRET))
    assert own.status_code == 200
    assert set(own.json()) == set(ingest_api.STATUS_FIELDS)
    assert own.json()["status"] == "received"
    assert own.json()["kind"] == "delete"

    assert api.get(f"/v1/status/{request_id}", headers=_auth(CLAUDE_SECRET)).status_code == 404
    assert api.get("/v1/status/does-not-exist", headers=_auth(MBP_SECRET)).status_code == 404


# ---------------------------------------------------------------------------
# Items: derived live from Qdrant
# ---------------------------------------------------------------------------

def _point(source_path: str, content_hash: str, pipeline_version: str = "p1") -> SimpleNamespace:
    """A Qdrant point as scroll() returns it, with only the payload this endpoint reads."""
    return SimpleNamespace(payload={
        "source_path": source_path,
        "content_hash": content_hash,
        "pipeline_version": pipeline_version,
    })


def test_items_groups_only_this_sources_points(api, qdrant):
    """Points are grouped by source_path, and the scroll filter carries the authenticated source id."""
    qdrant.scroll.side_effect = [
        ([_point("/Users/fotel/MBP/a.md", "h1"), _point("/Users/fotel/MBP/a.md", "h1")], "page-2"),
        ([_point("/Users/fotel/MBP/b.md", "h2")], None),
    ]

    response = api.get("/v1/items", headers=_auth(MBP_SECRET))

    assert response.status_code == 200
    assert response.json() == {"items": [
        {"key": "/Users/fotel/MBP/a.md", "chunk_count": 2, "content_hash": "h1", "pipeline_version": "p1"},
        {"key": "/Users/fotel/MBP/b.md", "chunk_count": 1, "content_hash": "h2", "pipeline_version": "p1"},
    ]}

    # Every page is filtered on this source's id, and no vectors are read.
    assert qdrant.scroll.call_count == 2
    for call in qdrant.scroll.call_args_list:
        scroll_filter = call.kwargs["scroll_filter"]
        (condition,) = scroll_filter.must
        assert isinstance(condition, FieldCondition)
        assert condition.key == "source_id"
        assert condition.match.value == "MBP"
        assert call.kwargs["with_vectors"] is False


# ---------------------------------------------------------------------------
# Info and projects
# ---------------------------------------------------------------------------

def test_info_echoes_config(api):
    """Info reports the pipeline version, the include list and the admission limits from ragnart.yaml."""
    response = api.get("/v1/info", headers=_auth(MBP_SECRET))

    assert response.status_code == 200
    assert response.json() == {
        "pipeline_version": "p1",
        "accepted_file_types": [".md", ".pdf"],
        "admission": {
            "max_text_file_size_bytes": TEXT_CAP_BYTES,
            "max_chunks_per_item": 5000,
            "fffd_ratio_threshold": 0.005,
        },
    }


def test_project_publish_is_refused_and_logged(api, caplog):
    """A project publish from a non-nas source is 409 with a reason, and logged at WARNING with the source and the project."""
    with caplog.at_level("WARNING", logger="ragnart.ingest_api"):
        response = api.post(
            "/v1/projects",
            headers=_auth(MBP_SECRET),
            json={"id": "proj-1", "name": "My Project", "path": "/Users/fotel/MBP/proj"},
        )

    assert response.status_code == 409
    body = response.json()
    assert body["refused"] is True
    assert body["reason"].startswith("MBP cannot hold ragnart projects")
    assert "today nas" in body["reason"]
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert any("MBP" in r.getMessage() and "proj-1" in r.getMessage() and "My Project" in r.getMessage()
               for r in warnings)


# ---------------------------------------------------------------------------
# Text-like notion parity with the ingest pipeline
# ---------------------------------------------------------------------------

def test_text_like_rule_agrees_with_ingest():
    """
    The API's text-like rule matches ragnart/ingest.py is_text_like_path(), which the API cannot import.

    Importing ingest needs docling, which the test environment has. If the sets drift in ingest.py,
    this test fails until ingest_api.py is updated to match.
    """
    import ragnart.ingest as ingest

    assert ingest_api._DOCLING_FILE_TYPES == ingest._DOCLING_TYPES
    assert ingest_api._NON_TEXT_FILE_TYPES == {"ipynb", "jsonl"}
    for file_type in ["pdf", "docx", "pptx", "ipynb", "jsonl", "md", "txt", "yaml", "py", "sh", "html"]:
        assert ingest_api.is_text_like_file_type(file_type) == ingest.is_text_like_path(f"x.{file_type}"), file_type
