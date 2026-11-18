"""
Tests for GET /v1/projects (ragnart/service.py).

No live Qdrant or NAS database is used:
  - get_client() and ensure_collection() are replaced, as in test_service_health.py.
  - The hub registry is a real RegistryDB in tmp_path (service_registry_db fixture in
    tests/conftest.py). The projects and tags come from it.
  - The queue database is a real StateDB in tmp_path (service_state_db fixture). The
    item counts come from it.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import ragnart.service as service
from ragnart.cli import FatalError
from ragnart.schema import GLOBAL_PROJECT_ID, SOURCE_ID_NAS

FLORIAN_RAGNART = "/mnt/nfs/Florian/Gin-AI/projects/RAGNART"
FLORIAN_OTHER = "/mnt/nfs/Florian/Gin-AI/projects/Other"
EVA_PROJECT = "/mnt/nfs/Eva/projects/Notes"
SHARED_PROJECT = "/mnt/nfs/la-familia/Shared"


@pytest.fixture
def served(monkeypatch):
    """Stub the Qdrant client and the collection check; /v1/projects needs neither."""
    client = MagicMock(name="qdrant_client")
    monkeypatch.setattr(service, "get_client", lambda url, timeout=60: client)
    monkeypatch.setattr(service, "ensure_collection", lambda c: None)
    return client


def _seed_completed(db, file_path: str, owner: str, project_id: str) -> None:
    """Queue one file in a project and mark it completed, as the worker would."""
    db.discover_or_update(file_path, owner, 1000.0, origin_path=file_path)
    db.mark_completed(
        file_path,
        chunk_count=1,
        completed_at="2026-01-01T00:00:00+00:00",
        project_id=project_id,
    )


@pytest.fixture
def seeded(service_state_db, service_registry_db):
    """Projects owned by florian, eva and la-familia, with completed items in some of them."""
    registry = service_registry_db
    registry.upsert_project("proj-ragnart", FLORIAN_RAGNART, "RAGNART", "florian", SOURCE_ID_NAS)
    registry.replace_project_tags("proj-ragnart", {"kind": "code", "lang": "python"})
    registry.upsert_project("proj-other", FLORIAN_OTHER, "Other", "florian", SOURCE_ID_NAS)
    registry.upsert_project("proj-eva", EVA_PROJECT, "Notes", "eva", SOURCE_ID_NAS)
    registry.upsert_project("proj-shared", SHARED_PROJECT, "Shared", "la-familia", SOURCE_ID_NAS)

    # Two completed items in RAGNART, one in Eva's project. A pending item must not count.
    db = service_state_db
    _seed_completed(db, "/mnt/nfs/Florian/Gin-AI/projects/RAGNART/a.md", "florian", "proj-ragnart")
    _seed_completed(db, "/mnt/nfs/Florian/Gin-AI/projects/RAGNART/b.md", "florian", "proj-ragnart")
    _seed_completed(db, "/mnt/nfs/Eva/projects/Notes/c.md", "eva", "proj-eva")
    db.discover_or_update("/mnt/nfs/Florian/Gin-AI/projects/RAGNART/pending.md", "florian", 1000.0)
    return registry


def test_projects_endpoint_lists_global_and_discovered_projects(seeded, served):
    with TestClient(service.app) as client:
        response = client.get("/v1/projects")

    assert response.status_code == 200
    projects = response.json()["projects"]

    # Every row is listed: the global row plus all four ragnart projects.
    assert [p["id"] for p in projects] == [
        GLOBAL_PROJECT_ID,
        "proj-eva",
        "proj-other",
        "proj-ragnart",
        "proj-shared",
    ]

    by_id = {p["id"]: p for p in projects}
    # The global row has no path, no owner and no home source.
    assert by_id[GLOBAL_PROJECT_ID] == {
        "id": GLOBAL_PROJECT_ID,
        "path": None,
        "name": "Global",
        "owner": None,
        "source_id": None,
        "tags": {},
        "item_count": 0,
    }
    # A discovered project carries the v0.3 fields, with its tags. No parent, no mode.
    assert by_id["proj-ragnart"] == {
        "id": "proj-ragnart",
        "path": FLORIAN_RAGNART,
        "name": "RAGNART",
        "owner": "florian",
        "source_id": SOURCE_ID_NAS,
        "tags": {"kind": "code", "lang": "python"},
        "item_count": 2,
    }


def test_projects_endpoint_filters_by_user(seeded, served):
    with TestClient(service.app) as client:
        response = client.get("/v1/projects", params={"user": "florian"})

    ids = [p["id"] for p in response.json()["projects"]]
    # Florian's own projects and la-familia are listed; Eva's is not. Global is always listed.
    assert ids == [GLOBAL_PROJECT_ID, "proj-other", "proj-ragnart", "proj-shared"]


def test_projects_endpoint_global_is_listed_for_a_user_with_no_projects(seeded, served):
    with TestClient(service.app) as client:
        response = client.get("/v1/projects", params={"user": "nobody"})

    # Only the global row and la-familia projects are visible to an unknown user.
    ids = [p["id"] for p in response.json()["projects"]]
    assert ids == [GLOBAL_PROJECT_ID, "proj-shared"]


def test_projects_endpoint_includes_item_count(seeded, served):
    with TestClient(service.app) as client:
        projects = client.get("/v1/projects").json()["projects"]

    counts = {p["id"]: p["item_count"] for p in projects}
    # Only completed items count; the pending file in RAGNART is not included.
    assert counts == {
        GLOBAL_PROJECT_ID: 0,
        "proj-eva": 1,
        "proj-other": 0,
        "proj-ragnart": 2,
        "proj-shared": 0,
    }


def test_projects_endpoint_returns_503_when_registry_is_missing(seeded, served, monkeypatch):
    def unavailable(config):
        raise FatalError("cannot open hub registry read-only")

    monkeypatch.setattr(service, "open_registry_read_only", unavailable)
    with TestClient(service.app) as client:
        response = client.get("/v1/projects")

    assert response.status_code == 503
    assert response.json() == {"detail": "Hub registry unavailable"}


def test_projects_endpoint_returns_503_when_queue_is_missing(seeded, served, monkeypatch):
    # The item counts come from the queue, so a missing queue also answers 503.
    def unavailable(config):
        raise FatalError("cannot open queue database read-only")

    monkeypatch.setattr(service, "open_queue_read_only", unavailable)
    with TestClient(service.app) as client:
        response = client.get("/v1/projects")

    assert response.status_code == 503
    assert response.json() == {"detail": "Queue database unavailable"}
