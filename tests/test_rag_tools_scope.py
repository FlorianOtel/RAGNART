"""
Tests that the utils/ search tools send `scope=all` to RAGNART's GET /v1/search.

GET /v1/search requires `scope` (no default; a request without it is HTTP 422).
No network is used: urllib.request.urlopen is replaced by a recorder that returns
an empty result list, so each test sees the exact URLs the tool requests.

The utils/ scripts are not a package, so they are loaded by file path.
"""

from __future__ import annotations

import importlib.util
import json
import urllib.parse
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import pytest

UTILS_DIR = Path(__file__).resolve().parent.parent / "utils"


def _load(name: str):
    """Import utils/<name>.py as a module without touching sys.path."""
    spec = importlib.util.spec_from_file_location(name, UTILS_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeResponse:
    """Minimal stand-in for the object urlopen returns (context manager with read())."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *exc_info) -> bool:
        return False

    def read(self) -> bytes:
        return self._body


@pytest.fixture
def requested_urls(monkeypatch) -> list[str]:
    """Replace urlopen with a recorder; every call returns an empty `results` list."""
    urls: list[str] = []

    def fake_urlopen(url, timeout=None):
        urls.append(url)
        return _FakeResponse(json.dumps({"results": []}).encode())

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    return urls


def _query_params(url: str) -> dict[str, list[str]]:
    """Return the parsed query string of a URL as {name: [values]}."""
    return urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)


def test_smoke_test_sends_scope_all(requested_urls):
    smoke = _load("rag_smoke_test")
    args = SimpleNamespace(
        query="hello",
        user=None,
        no_filter=False,
        top_k=5,
        file_type=None,
        server="http://search.invalid:8100",
        timeout=5,
        expect=None,
    )

    assert smoke.phase1_retrieval(args) is True

    assert len(requested_urls) == 1
    assert requested_urls[0].startswith("http://search.invalid:8100/v1/search?")
    assert _query_params(requested_urls[0])["scope"] == ["all"]


def test_smoke_test_sends_scope_all_with_file_type(requested_urls):
    # The file_types branch builds its own query string; scope must survive it too.
    smoke = _load("rag_smoke_test")
    args = SimpleNamespace(
        query="hello",
        user="florian",
        no_filter=False,
        top_k=5,
        file_type=["claude_chat"],
        server="http://search.invalid:8100",
        timeout=5,
        expect=None,
    )

    assert smoke.phase1_retrieval(args) is True

    params = _query_params(requested_urls[0])
    assert params["scope"] == ["all"]
    assert params["file_types"] == ["claude_chat"]


def test_compare_tool_sends_scope_all_to_ragnart_only(requested_urls):
    compare = _load("rag_compare_endpoints")
    old_url = "http://legacy.invalid:8000/v1/rag/search"
    new_url = "http://ragnart.invalid:8100/v1/search"

    # run_query makes six requests: three to each endpoint (HARD, control, rerank).
    row = compare.run_query(1, "hello", old_url, new_url, "florian", 5)
    assert row.hard_ok is True

    new_requests = [u for u in requested_urls if u.startswith(new_url)]
    old_requests = [u for u in requested_urls if u.startswith(old_url)]
    assert len(new_requests) == 2
    assert len(old_requests) == 4

    # Every RAGNART request carries scope=all.
    for url in new_requests:
        assert _query_params(url)["scope"] == ["all"], url

    # The legacy endpoint request is left unchanged: no scope parameter.
    for url in old_requests:
        assert "scope" not in _query_params(url), url


def test_compare_tool_sends_scope_all_when_both_urls_are_ragnart(requested_urls):
    # The docs' self-comparison example points --old-url and --new-url at RAGNART.
    # Both sides are /v1/search requests, so both must carry scope=all.
    compare = _load("rag_compare_endpoints")
    ragnart_url = "http://ragnart.invalid:8100/v1/search"

    row = compare.run_query(1, "hello", ragnart_url, ragnart_url, "florian", 5)
    assert row.hard_ok is True

    assert len(requested_urls) == 6
    for url in requested_urls:
        assert _query_params(url)["scope"] == ["all"], url
