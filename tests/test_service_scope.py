"""
Tests for the `scope` parameter of GET /v1/search (ragnart/service.py, ragnart/scope.py).

No live Qdrant, Ollama or NAS database is used:
  - get_client() and ensure_collection() are replaced, as in test_service_health.py.
  - search_rag() is replaced by a recorder, so each test can see the Qdrant filter
    and the other arguments the handler passed.
  - The hub registry is a real RegistryDB in tmp_path (service_registry_db fixture in
    tests/conftest.py), seeded through the public RegistryDB methods. The queue
    database is opened too, because the lifespan opens it (service_state_db fixture).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import ragnart.service as service
from ragnart.cli import FatalError
from ragnart.schema import FIELD_PROJECT_ID, SOURCE_ID_CLAUDE_LINUX_SHARED_NFS, SOURCE_ID_NAS

RAGNART_PATH = "/mnt/nfs/Florian/Gin-AI/projects/RAGNART"
OTHER_PATH = "/mnt/nfs/Florian/Gin-AI/projects/Other"


@pytest.fixture(autouse=True)
def _dbs(service_state_db, service_registry_db):
    """Every test here starts the service, whose lifespan opens the queue and the registry; use tmp ones."""
    return service_registry_db


@pytest.fixture
def served(monkeypatch):
    """Stub the Qdrant client, the collection check and search_rag; record each search call."""
    client = MagicMock(name="qdrant_client")
    monkeypatch.setattr(service, "get_client", lambda url, timeout=60: client)
    monkeypatch.setattr(service, "ensure_collection", lambda c: None)

    recorder = SimpleNamespace(calls=[])

    async def fake_search_rag(**kwargs):
        recorder.calls.append(kwargs)
        return []

    monkeypatch.setattr(service, "search_rag", fake_search_rag)
    return recorder


@pytest.fixture
def seeded(service_registry_db):
    """Two ragnart projects owned by florian, both homed in nas; RAGNART carries the tag kind=code."""
    service_registry_db.upsert_project("proj-ragnart", RAGNART_PATH, "RAGNART", "florian", SOURCE_ID_NAS)
    service_registry_db.replace_project_tags("proj-ragnart", {"kind": "code"})
    service_registry_db.upsert_project("proj-other", OTHER_PATH, "Other", "florian", SOURCE_ID_NAS)
    return service_registry_db


def _filter_project_ids(call: dict) -> list[str] | None:
    """Return the project_id values a recorded search_rag call restricts to, or None if unrestricted."""
    project_filter = call["project_filter"]
    if project_filter is None:
        return None
    (condition,) = project_filter.must
    assert condition.key == FIELD_PROJECT_ID
    return sorted(condition.match.any)


def _nas(path: str) -> str:
    """Return a path term qualified by the nas source."""
    return f"{SOURCE_ID_NAS}:{path}"


def test_search_without_scope_returns_422(seeded, served):
    # scope has no default, so FastAPI rejects the request before the handler runs.
    with TestClient(service.app) as client:
        response = client.get("/v1/search", params={"q": "hello"})

    assert response.status_code == 422
    assert served.calls == []


def test_search_scope_all_returns_200_and_echoes_global(seeded, served):
    with TestClient(service.app) as client:
        response = client.get("/v1/search", params={"q": "hello", "scope": "all"})

    assert response.status_code == 200
    body = response.json()
    assert body["scope"] == {
        "all": True,
        "global": True,
        "projects": [
            {"id": "proj-other", "path": OTHER_PATH, "name": "Other", "source_id": SOURCE_ID_NAS},
            {"id": "proj-ragnart", "path": RAGNART_PATH, "name": "RAGNART", "source_id": SOURCE_ID_NAS},
        ],
    }
    # `all` means no project restriction at all, so the filter passed is None.
    assert len(served.calls) == 1
    assert served.calls[0]["project_filter"] is None


def test_bare_path_scope_returns_422_with_message(seeded, served):
    with TestClient(service.app) as client:
        response = client.get("/v1/search", params={"q": "hello", "scope": RAGNART_PATH})

    assert response.status_code == 422
    assert response.json() == {
        "detail": f"{RAGNART_PATH}: a path term must name its source, e.g. nas:<path>",
    }
    # A bad scope must fail before any embedding or Qdrant call.
    assert served.calls == []


def test_unknown_source_scope_returns_422(seeded, served):
    with TestClient(service.app) as client:
        response = client.get("/v1/search", params={"q": "hello", "scope": f"nsa:{RAGNART_PATH}"})

    assert response.status_code == 422
    assert "unknown source 'nsa'" in response.json()["detail"]
    assert served.calls == []


def test_source_that_cannot_hold_projects_returns_422(seeded, served):
    with TestClient(service.app) as client:
        response = client.get(
            "/v1/search",
            params={"q": "hello", "scope": f"{SOURCE_ID_CLAUDE_LINUX_SHARED_NFS}:{RAGNART_PATH}"},
        )

    assert response.status_code == 422
    assert "has no project support yet" in response.json()["detail"]
    assert served.calls == []


def test_name_scope_returns_422(seeded, served):
    with TestClient(service.app) as client:
        response = client.get("/v1/search", params={"q": "hello", "scope": "name:RAGNART"})

    assert response.status_code == 422
    assert response.json() == {"detail": "name: terms are not supported yet"}
    assert served.calls == []


def test_search_echoes_resolved_projects(seeded, served):
    with TestClient(service.app) as client:
        exact = client.get(
            "/v1/search", params={"q": "hello", "scope": _nas(RAGNART_PATH)}
        ).json()
        tagged = client.get(
            "/v1/search", params={"q": "hello", "scope": "tag:kind=code"}
        ).json()
        repeated = client.get(
            "/v1/search",
            params=[("q", "hello"), ("scope", _nas(RAGNART_PATH)), ("scope", _nas(OTHER_PATH))],
        ).json()

    # An exact path selects only that project; the echo lists it with its source and no global.
    assert exact["scope"] == {
        "all": False,
        "global": False,
        "projects": [
            {"id": "proj-ragnart", "path": RAGNART_PATH, "name": "RAGNART", "source_id": SOURCE_ID_NAS},
        ],
    }
    assert tagged["scope"]["projects"] == [
        {"id": "proj-ragnart", "path": RAGNART_PATH, "name": "RAGNART", "source_id": SOURCE_ID_NAS},
    ]
    # Repeated terms are unioned.
    assert [p["id"] for p in repeated["scope"]["projects"]] == ["proj-other", "proj-ragnart"]

    # The restriction handed to search_rag matches the echo.
    assert [_filter_project_ids(c) for c in served.calls] == [
        ["proj-ragnart"],
        ["proj-ragnart"],
        ["proj-other", "proj-ragnart"],
    ]


def test_subtree_scope_selects_projects_below_path(seeded, served):
    with TestClient(service.app) as client:
        body = client.get(
            "/v1/search",
            params={"q": "hello", "scope": _nas("/mnt/nfs/Florian/Gin-AI/projects/")},
        ).json()

    assert [p["id"] for p in body["scope"]["projects"]] == ["proj-other", "proj-ragnart"]
    assert _filter_project_ids(served.calls[0]) == ["proj-other", "proj-ragnart"]


def test_search_returns_503_when_registry_could_not_be_opened(seeded, served, monkeypatch):
    # A missing registry must not guess a scope. Every scope needs it, `all` included,
    # because the echo of `all` lists every project.
    def unavailable(config):
        raise FatalError("cannot open hub registry read-only")

    monkeypatch.setattr(service, "open_registry_read_only", unavailable)
    with TestClient(service.app) as client:
        response = client.get("/v1/search", params={"q": "hello", "scope": "all"})

    assert response.status_code == 503
    assert response.json() == {"detail": "Hub registry unavailable"}
    assert served.calls == []


def test_path_scope_also_returns_503_when_registry_is_missing(seeded, served, monkeypatch):
    def unavailable(config):
        raise FatalError("cannot open hub registry read-only")

    monkeypatch.setattr(service, "open_registry_read_only", unavailable)
    with TestClient(service.app) as client:
        response = client.get("/v1/search", params={"q": "hello", "scope": _nas(RAGNART_PATH)})

    assert response.status_code == 503
    assert served.calls == []
