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
import sqlite3
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
from ragnart.schema import SOURCE_ID_NAS, SOURCE_ID_OPENCODE_K11

# Secrets used by the seeded sources. The registry stores only their hashes.
MBP_SECRET = "mbp-secret-for-tests"
CLAUDE_SECRET = "claude-secret-for-tests"
HUB_SECRET = "hub-secret-for-tests"
NAS_SECRET = "nas-secret-for-tests"
OPENCODE_SECRET = "opencode-secret-for-tests"
# A secret the hub does not know. It must be refused with 401.
WRONG = "not-a-secret-the-hub-knows"

# The owners configured in the tmp ragnart.yaml: one per-user owner and the shared owner.
CONFIGURED_OWNER = "florian"
SHARED_OWNER = "la-familia"

# A project that exists in the seeded registry.
PROJECT_ID = "proj-1"

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
        "users": {
            "florian.otel@gmail.com": {"owner": CONFIGURED_OWNER, "nfs_roots": ["/mnt/nfs/Florian"]},
        },
        "shared": {"owner": SHARED_OWNER, "nfs_roots": ["/mnt/nfs/La-Familia"]},
    }
    path = tmp_path / "ragnart.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    monkeypatch.setattr(cli, "CONFIG_PATH", path)
    return cfg


@pytest.fixture
def seeded(config):
    """
    A hub registry with the sources and the project the tests submit against.

    MBP is a filesystem source with a fixed owner, claude-MBP a claude_code source with a fixed owner.
    'hubby' is a hub-kind source that holds a secret hash: the kind no longer decides who may submit,
    so it authenticates like any other source that holds a secret.
    nas (filesystem, no fixed owner) and opencode-k11 (opencode, no fixed owner) are the seeded hub
    sources, given a secret hash the way a rotation would give it one.
    proj-1 is a project in the registry, so an upsert may attach to it.
    """
    registry = RegistryDB(registry_db_path(config))
    registry.register_source("MBP", "filesystem", "endpoint", "florian", _sha256(MBP_SECRET))
    registry.register_source("claude-MBP", "claude_code", "endpoint", "florian", _sha256(CLAUDE_SECRET))
    registry.register_source("hubby", "filesystem", "hub", None, _sha256(HUB_SECRET))
    registry.set_secret_hash(SOURCE_ID_NAS, _sha256(NAS_SECRET))
    registry.set_secret_hash(SOURCE_ID_OPENCODE_K11, _sha256(OPENCODE_SECRET))
    registry.upsert_project(PROJECT_ID, "/mnt/nfs/Florian/proj", "My Project", "florian", SOURCE_ID_NAS)
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


def _change_metadata_rows(config, source_id: str) -> list[dict]:
    """The change_metadata intake rows of one source. A refused request leaves none."""
    registry = _registry(config)
    try:
        return [row for row in registry.intake_for_source(source_id) if row["kind"] == "change_metadata"]
    finally:
        registry.close()


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
    # nas has no fixed owner, so it is the source that may name an owner in a change_metadata.
    owner = api.post(
        "/v1/change-metadata",
        headers=_auth(NAS_SECRET),
        json={"key": "/mnt/nfs/Florian/b.md", "metadata": {"owner": CONFIGURED_OWNER}},
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


def test_a_source_with_a_secret_authenticates_whatever_its_kind(api):
    """A hub-kind source that holds a secret hash authenticates: the kind no longer gates the auth gate."""
    assert api.get("/v1/info", headers=_auth(HUB_SECRET)).status_code == 200


def test_the_seeded_hub_sources_authenticate_once_they_hold_a_secret(api):
    """nas and opencode-k11 were given a secret hash, so their secrets authenticate; the wrong one does not."""
    assert api.get("/v1/info", headers=_auth(NAS_SECRET)).status_code == 200
    assert api.get("/v1/info", headers=_auth(OPENCODE_SECRET)).status_code == 200
    assert api.get("/v1/info", headers=_auth(MBP_SECRET + "x")).status_code == 401


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
    """Only owner and project_id may change. Any other field, alone or beside owner, is refused."""
    url = "/v1/change-metadata"
    key = "/mnt/nfs/Florian/a.md"
    assert api.post(url, headers=_auth(NAS_SECRET),
                    json={"key": key, "metadata": {"owner": CONFIGURED_OWNER}}).status_code == 202
    assert api.post(url, headers=_auth(NAS_SECRET),
                    json={"key": key, "metadata": {"owner": CONFIGURED_OWNER, "tag": "x"}}).status_code == 422
    key = "/Users/fotel/MBP/a.md"
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

def _point(source_path: str, content_hash: str, pipeline_version: str = "p1", **fields) -> SimpleNamespace:
    """
    A Qdrant point as scroll() returns it, with only the payload this endpoint reads.

    The item fields (owner, project_id, source_type, file_type) are optional keyword arguments, so a point
    can be given them or left without them, as an older point is.
    """
    payload = {
        "source_path": source_path,
        "content_hash": content_hash,
        "pipeline_version": pipeline_version,
    }
    payload.update(fields)
    return SimpleNamespace(payload=payload)


def test_items_groups_only_this_sources_points(api, qdrant):
    """Points are grouped by source_path, and the scroll filter carries the authenticated source id."""
    fields = {"owner": "florian", "project_id": "global", "source_type": "file", "file_type": "md"}
    qdrant.scroll.side_effect = [
        ([_point("/Users/fotel/MBP/a.md", "h1", **fields), _point("/Users/fotel/MBP/a.md", "h1", **fields)], "page-2"),
        ([_point("/Users/fotel/MBP/b.md", "h2", **fields)], None),
    ]

    response = api.get("/v1/items", headers=_auth(MBP_SECRET))

    assert response.status_code == 200
    assert response.json() == {"items": [
        {"key": "/Users/fotel/MBP/a.md", "chunk_count": 2, "content_hash": "h1", "pipeline_version": "p1",
         "owner": "florian", "project_id": "global", "source_type": "file", "file_type": "md", "inconsistent": []},
        {"key": "/Users/fotel/MBP/b.md", "chunk_count": 1, "content_hash": "h2", "pipeline_version": "p1",
         "owner": "florian", "project_id": "global", "source_type": "file", "file_type": "md", "inconsistent": []},
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


def test_items_read_the_item_fields_and_no_others(api, qdrant):
    """The scroll selects the item fields (owner, project, types) beside the three it always read, and no vectors."""
    qdrant.scroll.return_value = ([], None)

    api.get("/v1/items", headers=_auth(MBP_SECRET))

    selected = qdrant.scroll.call_args.kwargs["with_payload"]
    assert set(selected) == {
        "source_path", "content_hash", "pipeline_version", "owner", "project_id", "source_type", "file_type",
    }


def test_items_points_that_disagree_keep_the_first_value_and_name_the_field(api, qdrant):
    """When a later point of an item carries another owner, the first point's owner stays and 'owner' is flagged."""
    qdrant.scroll.return_value = ([
        _point("/Users/fotel/MBP/a.md", "h1", owner="florian", project_id="global"),
        _point("/Users/fotel/MBP/a.md", "h1", owner="eva", project_id="global"),
    ], None)

    items = api.get("/v1/items", headers=_auth(MBP_SECRET)).json()["items"]

    assert items == [{
        "key": "/Users/fotel/MBP/a.md", "chunk_count": 2, "content_hash": "h1", "pipeline_version": "p1",
        "owner": "florian", "project_id": "global", "source_type": None, "file_type": None,
        "inconsistent": ["owner"],
    }]


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
            json={"id": "fresh-proj", "name": "My Project", "path": "/Users/fotel/MBP/proj"},
        )

    assert response.status_code == 409
    body = response.json()
    assert body["refused"] is True
    assert body["reason"] == (
        "project records from MBP are not supported yet: "
        "project support is built for the NAS ingestion only (v0.4)"
    )
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert any("MBP" in r.getMessage() and "fresh-proj" in r.getMessage() and "My Project" in r.getMessage()
               for r in warnings)


# ---------------------------------------------------------------------------
# Projects (session B2): publish and remove write the registry synchronously
# ---------------------------------------------------------------------------

def _seed_project(config, project_id: str, home: str | None, owner: str | None = CONFIGURED_OWNER) -> None:
    """Write one project row into the tmp registry, homed on `home`, with one tag."""
    registry = RegistryDB(registry_db_path(config))
    try:
        registry.upsert_project(project_id, f"/mnt/nfs/Florian/{project_id}", project_id, owner, home)
        registry.replace_project_tags(project_id, {"kind": "seed"})
    finally:
        registry.close()


def _publish_body(**overrides) -> dict:
    """A nas project publish, as the NAS side sends it: the marker's fields, its owner and its tags."""
    body = {
        "id": "marker-proj",
        "name": "Marker Project",
        "path": "/mnt/nfs/Florian/marker-proj",
        "owner": CONFIGURED_OWNER,
        "tags": {"kind": "repo", "area": "home"},
    }
    body.update(overrides)
    return body


def test_nas_publish_writes_the_project_row_and_its_tags(api, config):
    """A publish from nas writes the row (home nas, the checked owner, path, name) and replaces the tags."""
    response = api.post("/v1/projects", headers=_auth(NAS_SECRET), json=_publish_body())

    assert response.status_code == 200
    assert response.json() == {"published": True, "id": "marker-proj"}
    registry = _registry(config)
    try:
        project = next(p for p in registry.list_projects() if p["id"] == "marker-proj")
        assert project == {
            "id": "marker-proj",
            "path": "/mnt/nfs/Florian/marker-proj",
            "name": "Marker Project",
            "owner": CONFIGURED_OWNER,
            "home_source_id": SOURCE_ID_NAS,
        }
        assert registry.project_tags("marker-proj") == {"kind": "repo", "area": "home"}
    finally:
        registry.close()


def test_nas_republish_updates_the_row_and_replaces_the_tags(api, config):
    """Publishing the same project again updates it: a tag that is no longer sent is removed."""
    api.post("/v1/projects", headers=_auth(NAS_SECRET), json=_publish_body())

    response = api.post(
        "/v1/projects", headers=_auth(NAS_SECRET),
        json=_publish_body(name="Renamed", tags={"kind": "docs"}),
    )

    assert response.status_code == 200
    registry = _registry(config)
    try:
        project = next(p for p in registry.list_projects() if p["id"] == "marker-proj")
        assert project["name"] == "Renamed"
        assert registry.project_tags("marker-proj") == {"kind": "docs"}
    finally:
        registry.close()


def test_nas_publish_without_an_owner_is_refused_with_422(api, config):
    """nas has no fixed owner, so a publish must name one. Without it, 422 and nothing is written."""
    response = api.post("/v1/projects", headers=_auth(NAS_SECRET), json=_publish_body(owner=None))

    assert response.status_code == 422
    registry = _registry(config)
    try:
        assert all(p["id"] != "marker-proj" for p in registry.list_projects())
    finally:
        registry.close()


def test_nas_publish_with_an_unconfigured_owner_is_refused_with_422(api, config):
    """The project owner must be a name ragnart.yaml configures, as for an item."""
    response = api.post("/v1/projects", headers=_auth(NAS_SECRET), json=_publish_body(owner="mallory"))

    assert response.status_code == 422
    assert "not configured" in response.json()["detail"]


def test_publish_of_the_reserved_global_id_is_refused_with_422(api, config):
    """'global' is the reserved project; no source may publish it."""
    response = api.post("/v1/projects", headers=_auth(NAS_SECRET), json=_publish_body(id="global"))

    assert response.status_code == 422
    registry = _registry(config)
    try:
        assert registry.project_tags("global") == {}
    finally:
        registry.close()


def test_publish_with_a_non_string_tag_is_refused_with_422(api):
    """Tags are a map of string to string; a number is refused by the request check."""
    response = api.post(
        "/v1/projects", headers=_auth(NAS_SECRET),
        json=_publish_body(tags={"priority": 3}),
    )

    assert response.status_code == 422


def test_publish_of_a_project_homed_on_another_source_is_refused_with_409(api, config):
    """D21: a project id homed on MBP cannot be taken over by nas. The row and its tags are unchanged."""
    _seed_project(config, "mbp-proj", home="MBP", owner=CONFIGURED_OWNER)

    response = api.post(
        "/v1/projects", headers=_auth(NAS_SECRET),
        json=_publish_body(id="mbp-proj", name="Hijack", tags={"kind": "stolen"}),
    )

    assert response.status_code == 409
    body = response.json()
    assert body["refused"] is True
    assert body["reason"] == "project mbp-proj (mbp-proj) is homed on MBP; a project has one home (D21)"
    registry = _registry(config)
    try:
        project = next(p for p in registry.list_projects() if p["id"] == "mbp-proj")
        assert project["name"] == "mbp-proj"
        assert project["home_source_id"] == "MBP"
        assert registry.project_tags("mbp-proj") == {"kind": "seed"}
    finally:
        registry.close()


def test_home_comparison_is_case_insensitive(api, config):
    """A project homed as 'NAS' is the nas source's own: the D21 compare ignores case, so the republish is accepted."""
    _seed_project(config, "upper-proj", home="NAS", owner=CONFIGURED_OWNER)

    response = api.post(
        "/v1/projects", headers=_auth(NAS_SECRET),
        json=_publish_body(id="upper-proj", name="Same Source"),
    )

    assert response.status_code == 200


def test_a_copied_marker_homed_on_nas_published_by_mbp_gets_the_d21_reason(api, config):
    """D21 is checked before project support: MBP publishing nas's project id is refused as homed elsewhere."""
    response = api.post(
        "/v1/projects", headers=_auth(MBP_SECRET),
        json=_publish_body(id=PROJECT_ID, owner=None),
    )

    assert response.status_code == 409
    reason = response.json()["reason"]
    assert "is homed on nas; a project has one home (D21)" in reason
    assert "not supported yet" not in reason


def test_a_new_project_id_from_mbp_gets_the_not_supported_yet_reason(api):
    """A project id no one homes, published by MBP, is refused for want of project support, not by D21."""
    response = api.post("/v1/projects", headers=_auth(MBP_SECRET), json=_publish_body(id="brand-new", owner=None))

    assert response.status_code == 409
    reason = response.json()["reason"]
    assert reason.startswith("project records from MBP are not supported yet")
    assert "D21" not in reason


def test_a_source_without_project_support_writes_nothing_to_the_registry(api, config):
    """MBP has no project support built: its publish is refused with 409 and the registry gets no row."""
    api.post("/v1/projects", headers=_auth(MBP_SECRET), json=_publish_body(id="from-mbp", owner=None))

    registry = _registry(config)
    try:
        assert all(p["id"] != "from-mbp" for p in registry.list_projects())
    finally:
        registry.close()


def test_fixed_owner_supported_source_refuses_an_owner_in_the_publish(api, config):
    """A source with project support and a fixed owner may not name one: the same rule as an upsert (_checked_owner)."""
    # nas is made to have a fixed owner for this test only: the owner rule reads the registry row.
    raw = sqlite3.connect(registry_db_path(config))
    raw.execute("UPDATE sources SET owner = ? WHERE id = ?", (CONFIGURED_OWNER, SOURCE_ID_NAS))
    raw.commit()
    raw.close()

    response = api.post("/v1/projects", headers=_auth(NAS_SECRET), json=_publish_body())

    assert response.status_code == 422
    assert "fixed owner" in response.json()["detail"]


def test_nas_removes_a_project_it_homes(api, config):
    """remove deletes the project row and its tags, for a project homed on the calling source."""
    response = api.post("/v1/projects/remove", headers=_auth(NAS_SECRET), json={"id": PROJECT_ID})

    assert response.status_code == 200
    assert response.json() == {"removed": True, "id": PROJECT_ID}
    registry = _registry(config)
    try:
        assert all(p["id"] != PROJECT_ID for p in registry.list_projects())
    finally:
        registry.close()


def test_remove_of_an_unknown_project_is_404(api):
    """A project the registry does not hold answers 404."""
    response = api.post("/v1/projects/remove", headers=_auth(NAS_SECRET), json={"id": "no-such-project"})

    assert response.status_code == 404


def test_remove_of_a_project_homed_elsewhere_is_409_and_keeps_it(api, config):
    """Only the homing source may remove a project. Another source's request is refused and nothing is deleted."""
    _seed_project(config, "mbp-proj", home="MBP")

    response = api.post("/v1/projects/remove", headers=_auth(NAS_SECRET), json={"id": "mbp-proj"})

    assert response.status_code == 409
    assert response.json()["refused"] is True
    registry = _registry(config)
    try:
        assert any(p["id"] == "mbp-proj" for p in registry.list_projects())
    finally:
        registry.close()


def test_remove_from_a_source_without_project_support_is_409(api, config):
    """MBP has no project support built, so its remove is refused (here by D21: the project is nas's)."""
    response = api.post("/v1/projects/remove", headers=_auth(MBP_SECRET), json={"id": PROJECT_ID})

    assert response.status_code == 409
    registry = _registry(config)
    try:
        assert any(p["id"] == PROJECT_ID for p in registry.list_projects())
    finally:
        registry.close()


def test_remove_of_the_global_project_is_409(api, config):
    """'global' has no home source, so no source can remove it."""
    response = api.post("/v1/projects/remove", headers=_auth(NAS_SECRET), json={"id": "global"})

    assert response.status_code == 409
    registry = _registry(config)
    try:
        assert any(p["id"] == "global" for p in registry.list_projects())
    finally:
        registry.close()


def test_project_routes_refuse_a_wrong_secret(api):
    """The project routes sit behind the same auth gate as every other write."""
    assert api.post("/v1/projects", headers=_auth(WRONG), json=_publish_body()).status_code == 401
    assert api.post("/v1/projects/remove", headers=_auth(WRONG), json={"id": PROJECT_ID}).status_code == 401


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


# ---------------------------------------------------------------------------
# Connectors: opencode sessions, the route each connector may use
# ---------------------------------------------------------------------------

def test_opencode_text_upsert_is_accepted_under_the_opencode_namespace(api, config):
    """An opencode source sends its sessions as text, keyed 'opencode://...'. The row is recorded under it."""
    response = api.post(
        "/v1/upsert/text",
        headers=_auth(OPENCODE_SECRET),
        json={"key": "opencode://ses_abc", "text": "the opencode session", "owner": CONFIGURED_OWNER},
    )

    assert response.status_code == 202
    registry = _registry(config)
    try:
        row = registry.intake_by_id(response.json()["request_id"])
    finally:
        registry.close()
    assert row["source_id"] == SOURCE_ID_OPENCODE_K11
    assert row["kind"] == "upsert"
    assert row["key"] == "opencode://ses_abc"
    assert row["text"] == "the opencode session"
    assert row["owner"] == CONFIGURED_OWNER


def test_opencode_key_outside_its_namespace_is_422(api):
    """An opencode source's key must start with 'opencode://'; a claude-session key is refused."""
    response = api.post(
        "/v1/upsert/text",
        headers=_auth(OPENCODE_SECRET),
        json={"key": "claude-session://abc", "text": "x", "owner": CONFIGURED_OWNER},
    )
    assert response.status_code == 422


def test_opencode_source_cannot_send_bytes(api, config):
    """An opencode source sends text only: a bytes upsert from it is refused and nothing is spooled."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(OPENCODE_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "opencode://ses_abc", "file_type": "md"},
    )
    assert response.status_code == 422
    assert _spool_files(config) == []


def test_filesystem_source_cannot_send_text(api):
    """A filesystem source sends bytes only: a text upsert from NAS is refused."""
    response = api.post(
        "/v1/upsert/text",
        headers=_auth(NAS_SECRET),
        json={"key": "/mnt/nfs/Florian/a.md", "text": "x", "owner": CONFIGURED_OWNER},
    )
    assert response.status_code == 422


def test_opencode_session_cannot_change_key(api, config):
    """An opencode session is keyed by its id and never moves: a change_key from it is refused, nothing recorded."""
    response = api.post(
        "/v1/change-key",
        headers=_auth(OPENCODE_SECRET),
        json={"key": "opencode://ses_a", "new_key": "opencode://ses_b"},
    )

    assert response.status_code == 422
    assert "never move" in response.json()["detail"]
    registry = _registry(config)
    try:
        count = registry._conn.execute("SELECT COUNT(*) FROM intake").fetchone()[0]
    finally:
        registry.close()
    assert count == 0


# ---------------------------------------------------------------------------
# Owner: fixed for a source that has one, named per item for one that has none
# ---------------------------------------------------------------------------

def test_no_fixed_owner_source_stores_the_configured_owner_it_names(api, config):
    """nas has no fixed owner: an upsert names the item's owner, and the name is stored on the row."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(NAS_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "/mnt/nfs/Florian/a.md", "file_type": "md", "owner": CONFIGURED_OWNER},
    )

    assert response.status_code == 202
    registry = _registry(config)
    try:
        row = registry.intake_by_id(response.json()["request_id"])
    finally:
        registry.close()
    assert row["owner"] == CONFIGURED_OWNER
    assert row["source_id"] == SOURCE_ID_NAS


def test_no_fixed_owner_source_may_name_the_shared_owner(api):
    """The shared owner in ragnart.yaml is a configured name, so nas may name it too."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(NAS_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "/mnt/nfs/La-Familia/a.md", "file_type": "md", "owner": SHARED_OWNER},
    )
    assert response.status_code == 202


def test_no_fixed_owner_source_without_an_owner_is_422_and_not_spooled(api, config):
    """A source with no fixed owner must name the owner: without one the points would have none."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(NAS_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "/mnt/nfs/Florian/a.md", "file_type": "md"},
    )
    assert response.status_code == 422
    assert _spool_files(config) == []


def test_owner_outside_the_configured_names_is_422_and_not_spooled(api, config):
    """An owner that ragnart.yaml does not configure is refused with 422, and nothing is spooled."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(NAS_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "/mnt/nfs/Florian/a.md", "file_type": "md", "owner": "mallory"},
    )
    assert response.status_code == 422
    assert _spool_files(config) == []


def test_owner_on_a_text_upsert_is_checked_the_same_way(api):
    """The text route applies the same owner rule: an unconfigured name is 422 for opencode-k11."""
    response = api.post(
        "/v1/upsert/text",
        headers=_auth(OPENCODE_SECRET),
        json={"key": "opencode://ses_abc", "text": "x", "owner": "mallory"},
    )
    assert response.status_code == 422


def test_fixed_owner_source_refuses_an_owner_in_the_request(api, config):
    """MBP has the fixed owner florian: naming any owner in an upsert is 422, nothing recorded."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "/Users/fotel/MBP/a.md", "file_type": "md", "owner": CONFIGURED_OWNER},
    )
    assert response.status_code == 422
    assert _spool_files(config) == []


def test_fixed_owner_source_text_upsert_refuses_an_owner(api):
    """claude-MBP has a fixed owner too: a text upsert that names one is 422."""
    response = api.post(
        "/v1/upsert/text",
        headers=_auth(CLAUDE_SECRET),
        json={"key": "claude-session://abc", "text": "x", "owner": CONFIGURED_OWNER},
    )
    assert response.status_code == 422


def test_fixed_owner_source_stores_no_owner_on_the_row(api, config):
    """With no owner in the request, a fixed-owner source's row has a NULL owner: the worker uses the source's."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "/Users/fotel/MBP/a.md", "file_type": "md"},
    )
    assert response.status_code == 202
    registry = _registry(config)
    try:
        row = registry.intake_by_id(response.json()["request_id"])
    finally:
        registry.close()
    assert row["owner"] is None


# ---------------------------------------------------------------------------
# Tag and project_id on an upsert
# ---------------------------------------------------------------------------

def test_bytes_upsert_stores_tag_and_project_id(api, config):
    """tag and project_id are dedicated upsert fields: they reach the row, not the metadata."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "/Users/fotel/MBP/a.md", "file_type": "md", "tag": "cisco", "project_id": PROJECT_ID},
    )

    assert response.status_code == 202
    registry = _registry(config)
    try:
        row = registry.intake_by_id(response.json()["request_id"])
    finally:
        registry.close()
    assert (row["tag"], row["project_id"]) == ("cisco", PROJECT_ID)
    assert row["metadata"] is None


def test_text_upsert_stores_tag_and_project_id(api, config):
    """The text route stores the same two fields."""
    response = api.post(
        "/v1/upsert/text",
        headers=_auth(CLAUDE_SECRET),
        json={"key": "claude-session://abc", "text": "x", "tag": "demo", "project_id": "global"},
    )

    assert response.status_code == 202
    registry = _registry(config)
    try:
        row = registry.intake_by_id(response.json()["request_id"])
    finally:
        registry.close()
    assert (row["tag"], row["project_id"]) == ("demo", "global")


def test_upsert_with_unknown_project_is_422_and_not_spooled(api, config):
    """A project_id that is not in the registry is refused with 422 before anything is spooled."""
    response = api.post(
        "/v1/upsert/bytes",
        headers=_auth(MBP_SECRET),
        files={"file": ("a.md", b"x", "text/markdown")},
        data={"key": "/Users/fotel/MBP/a.md", "file_type": "md", "project_id": "no-such-project"},
    )
    assert response.status_code == 422
    assert _spool_files(config) == []


def test_text_upsert_with_unknown_project_is_422(api):
    """The text route refuses an unknown project_id the same way."""
    response = api.post(
        "/v1/upsert/text",
        headers=_auth(CLAUDE_SECRET),
        json={"key": "claude-session://abc", "text": "x", "project_id": "no-such-project"},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# change-metadata: project_id
# ---------------------------------------------------------------------------

def test_change_metadata_project_id_must_name_a_registry_project(api):
    """project_id is changeable; an existing project and 'global' are accepted, an unknown one is 422."""
    url = "/v1/change-metadata"
    key = "/Users/fotel/MBP/a.md"
    assert api.post(url, headers=_auth(MBP_SECRET),
                    json={"key": key, "metadata": {"project_id": PROJECT_ID}}).status_code == 202
    assert api.post(url, headers=_auth(MBP_SECRET),
                    json={"key": key, "metadata": {"project_id": "global"}}).status_code == 202
    assert api.post(url, headers=_auth(MBP_SECRET),
                    json={"key": key, "metadata": {"project_id": "no-such-project"}}).status_code == 422


def test_change_metadata_project_id_must_be_a_string(api):
    """A non-string project_id, null included, is refused with 422."""
    url = "/v1/change-metadata"
    key = "/Users/fotel/MBP/a.md"
    for value in (None, 7, ["proj-1"]):
        response = api.post(url, headers=_auth(MBP_SECRET), json={"key": key, "metadata": {"project_id": value}})
        assert response.status_code == 422, value


def test_change_metadata_project_id_and_owner_together_are_both_accepted(api, config):
    """The two changeable fields may be sent in one request, on a source with no fixed owner."""
    response = api.post(
        "/v1/change-metadata",
        headers=_auth(NAS_SECRET),
        json={"key": "/mnt/nfs/Florian/a.md", "metadata": {"project_id": PROJECT_ID, "owner": CONFIGURED_OWNER}},
    )
    assert response.status_code == 202
    registry = _registry(config)
    try:
        row = registry.intake_by_id(response.json()["request_id"])
    finally:
        registry.close()
    # The stored metadata carries the checked owner and the project id, both as sent.
    assert row["metadata"] == {"project_id": PROJECT_ID, "owner": CONFIGURED_OWNER}


def test_change_metadata_owner_on_a_fixed_owner_source_is_422_and_records_nothing(api, config):
    """MBP has the fixed owner florian: a change_metadata that names an owner is refused, as an upsert is."""
    response = api.post(
        "/v1/change-metadata",
        headers=_auth(MBP_SECRET),
        json={"key": "/Users/fotel/MBP/a.md", "metadata": {"owner": CONFIGURED_OWNER}},
    )
    assert response.status_code == 422
    assert _change_metadata_rows(config, "MBP") == []


def test_change_metadata_unconfigured_owner_on_a_no_fixed_owner_source_is_422(api, config):
    """nas has no fixed owner: a change_metadata naming an owner ragnart.yaml does not configure is refused."""
    response = api.post(
        "/v1/change-metadata",
        headers=_auth(NAS_SECRET),
        json={"key": "/mnt/nfs/Florian/a.md", "metadata": {"owner": "mallory"}},
    )
    assert response.status_code == 422
    assert _change_metadata_rows(config, SOURCE_ID_NAS) == []
