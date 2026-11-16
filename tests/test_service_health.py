"""
Tests for ragnart/service.py start-up tolerance and the /health endpoint.

No live Qdrant or model is used:
  - get_client() and ensure_collection() are replaced in the service module,
    so the lifespan does not contact Qdrant and does not load any model.
  - httpx.get() is replaced, so /health never makes a real HTTP call.
  - The lifespan opens the queue database read-only; an empty tmp StateDB is
    used instead of the NAS database (see the `service_state_db` fixture).
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

import ragnart.service as service

EXPECTED_HEALTHZ_URL = "http://192.168.1.93:6333/healthz"


@pytest.fixture
def qdrant_client(monkeypatch):
    """Replace get_client() so the lifespan gets a mock client, and return that mock."""
    client = MagicMock(name="qdrant_client")
    monkeypatch.setattr(service, "get_client", lambda url, timeout=60: client)
    return client


@pytest.fixture(autouse=True)
def _queue_db(service_state_db):
    """Every test here starts the service, whose lifespan opens the queue DB; use a tmp one."""
    return service_state_db


@pytest.fixture
def ensure_ok(monkeypatch):
    """Replace ensure_collection() with a no-op success."""
    monkeypatch.setattr(service, "ensure_collection", lambda c: None)


@pytest.fixture
def healthz(monkeypatch):
    """Replace httpx.get() and return a recorder.

    Set recorder.response to a status code (int) or an exception instance to make
    the fake /healthz answer that way. recorder.calls records each (url, timeout).
    """
    recorder = SimpleNamespace(calls=[], response=200)

    def fake_get(url, timeout=None, **kwargs):
        recorder.calls.append((url, timeout))
        if isinstance(recorder.response, BaseException):
            raise recorder.response
        return SimpleNamespace(status_code=recorder.response)

    monkeypatch.setattr(service.httpx, "get", fake_get)
    return recorder


# ---------------------------------------------------------------------------
# Start-up: client always created, ensure_collection failure is only a warning
# ---------------------------------------------------------------------------

def test_client_created_even_when_ensure_collection_fails(qdrant_client, monkeypatch, caplog, healthz):
    def failing_ensure(client):
        raise ConnectionError("qdrant down")

    monkeypatch.setattr(service, "ensure_collection", failing_ensure)

    with caplog.at_level(logging.WARNING, logger="ragnart"):
        with TestClient(service.app) as client:
            # The client exists despite the failure, so it is not None.
            assert service.app.state.qdrant_client is qdrant_client
            assert client.get("/health").status_code == 200

    assert any("Qdrant unavailable at startup" in r.getMessage() for r in caplog.records)


def test_startup_calls_ensure_collection_on_success(qdrant_client, monkeypatch, healthz):
    seen = []
    monkeypatch.setattr(service, "ensure_collection", lambda c: seen.append(c))

    with TestClient(service.app):
        pass

    assert seen == [qdrant_client]


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------

def test_health_ok_when_healthz_returns_200(qdrant_client, ensure_ok, healthz):
    healthz.response = 200
    with TestClient(service.app) as client:
        body = client.get("/health").json()

    assert body == {"status": "ok", "qdrant": True}
    # Probe is Qdrant's /healthz with the 2.0 s timeout from the step spec.
    assert healthz.calls == [(EXPECTED_HEALTHZ_URL, 2.0)]


def test_health_degraded_on_non_200(qdrant_client, ensure_ok, healthz):
    healthz.response = 503
    with TestClient(service.app) as client:
        body = client.get("/health").json()

    assert body == {"status": "degraded", "qdrant": False}


def test_health_degraded_on_connection_error(qdrant_client, ensure_ok, healthz):
    healthz.response = httpx.ConnectError("connection refused")
    with TestClient(service.app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "degraded", "qdrant": False}


def test_health_degraded_on_timeout(qdrant_client, ensure_ok, healthz):
    healthz.response = httpx.ReadTimeout("slow")
    with TestClient(service.app) as client:
        body = client.get("/health").json()

    assert body == {"status": "degraded", "qdrant": False}


def test_health_does_not_use_qdrant_client(qdrant_client, ensure_ok, healthz):
    # The old /health called qdrant_client.get_collections() (60 s timeout).
    # The new one must probe /healthz with httpx and leave the client alone.
    healthz.response = 200
    with TestClient(service.app) as client:
        client.get("/health")

    qdrant_client.get_collections.assert_not_called()


def test_health_degraded_when_ensure_failed_and_healthz_down(qdrant_client, monkeypatch, healthz):
    def failing_ensure(client):
        raise RuntimeError("collection check failed")

    monkeypatch.setattr(service, "ensure_collection", failing_ensure)
    healthz.response = httpx.ConnectError("down")
    with TestClient(service.app) as client:
        body = client.get("/health").json()

    assert body == {"status": "degraded", "qdrant": False}


# ---------------------------------------------------------------------------
# /v1/search: Qdrant failures map to 503 "Qdrant unavailable"
# ---------------------------------------------------------------------------

@pytest.fixture
def fake_search(monkeypatch):
    """Replace search_rag() with a coroutine that raises `error` if it is set.

    Set recorder.error to an exception instance to make the search fail that way.
    """
    recorder = SimpleNamespace(error=None)

    async def fake_search_rag(**kwargs):
        if recorder.error is not None:
            raise recorder.error
        return []

    monkeypatch.setattr(service, "search_rag", fake_search_rag)
    return recorder


def _qdrant_http_error(status_code: int) -> UnexpectedResponse:
    """Build the exception qdrant_client raises for an HTTP error answer."""
    return UnexpectedResponse(
        status_code=status_code,
        reason_phrase="reason",
        content=b"",
        headers=httpx.Headers(),
    )


def test_search_returns_503_when_qdrant_is_unreachable(qdrant_client, ensure_ok, fake_search):
    # qdrant_client wraps a refused connection (or timeout) in ResponseHandlingException.
    fake_search.error = ResponseHandlingException(httpx.ConnectError("connection refused"))
    with TestClient(service.app) as client:
        response = client.get("/v1/search", params={"q": "hello", "scope": "all"})

    assert response.status_code == 503
    assert response.json() == {"detail": "Qdrant unavailable"}


def test_search_returns_503_when_qdrant_answers_5xx(qdrant_client, ensure_ok, fake_search):
    fake_search.error = _qdrant_http_error(503)
    with TestClient(service.app) as client:
        response = client.get("/v1/search", params={"q": "hello", "scope": "all"})

    assert response.status_code == 503
    assert response.json() == {"detail": "Qdrant unavailable"}


def test_search_passes_qdrant_4xx_through(qdrant_client, ensure_ok, fake_search):
    # A 4xx is a problem with the request, not an outage, so it is not turned into 503.
    fake_search.error = _qdrant_http_error(400)
    with TestClient(service.app) as client:
        with pytest.raises(UnexpectedResponse):
            client.get("/v1/search", params={"q": "hello", "scope": "all"})


def test_search_does_not_blame_qdrant_for_embedder_errors(qdrant_client, ensure_ok, fake_search):
    # The embedder (Ollama) is called with plain httpx, not through qdrant_client, so a
    # connection error there must not be reported as "Qdrant unavailable".
    fake_search.error = httpx.ConnectError("ollama down")
    with TestClient(service.app) as client:
        with pytest.raises(httpx.ConnectError):
            client.get("/v1/search", params={"q": "hello", "scope": "all"})


def test_search_success_still_returns_results(qdrant_client, ensure_ok, fake_search):
    with TestClient(service.app) as client:
        response = client.get("/v1/search", params={"q": "hello", "scope": "all"})

    assert response.status_code == 200
    # max_hits_per_source is echoed with the config default (ragnart.yaml: 3).
    assert response.json() == {
        "query": "hello",
        "user": None,
        "scope": {"all": True, "global": True, "projects": []},
        "max_hits_per_source": 3,
        "results": [],
    }


def test_search_echoes_max_hits_per_source_default_and_override(qdrant_client, ensure_ok, fake_search):
    # Without the parameter the echoed value is the ragnart.yaml default (3).
    # With ?max_hits_per_source=7 the request value wins and is echoed back.
    with TestClient(service.app) as client:
        default_body = client.get("/v1/search", params={"q": "hello", "scope": "all"}).json()
        override_body = client.get(
            "/v1/search", params={"q": "hello", "scope": "all", "max_hits_per_source": 7}
        ).json()

    assert default_body["max_hits_per_source"] == 3
    assert override_body["max_hits_per_source"] == 7
