"""
Unit tests for collapse by content_hash, the project_filter parameter, and their
place in the search_rag() shaping order (ragnart/search.py).

No live Qdrant or Ollama is used, the same way as tests/test_search_max_hits.py:
  - embed_text() and ensure_collection() are replaced in the search module,
    so no embedding model is called and no collection is touched.
  - The Qdrant client is a MagicMock whose query_points() returns canned hits.

The async tests are driven with asyncio.run().
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from qdrant_client.models import FieldCondition, Filter, MatchValue

import ragnart.search as search
from ragnart.moves import EMPTY_FILE_MD5
from ragnart.schema import (
    FIELD_CONTENT_HASH,
    FIELD_OWNER,
    FIELD_PARENT_TEXT,
    FIELD_PROJECT_ID,
    FIELD_SOURCE_ID,
    FIELD_SOURCE_PATH,
)


def _hit(source_path: str, content_hash: str | None, score: float = 1.0) -> SimpleNamespace:
    """Build a fake Qdrant ScoredPoint carrying the payload fields search_rag() reads."""
    return SimpleNamespace(
        payload={
            FIELD_SOURCE_PATH: source_path,
            FIELD_PARENT_TEXT: f"text from {source_path}",
            FIELD_CONTENT_HASH: content_hash,
        },
        score=score,
    )


@pytest.fixture
def stub_search(monkeypatch):
    """Stub the embedder and the collection check so search_rag() needs no services."""

    async def fake_embed_text(text, model, ollama_url):
        return [0.0]

    monkeypatch.setattr(search, "embed_text", fake_embed_text)
    monkeypatch.setattr(search, "ensure_collection", lambda client: None)


def _run_search(points: list, **kwargs) -> tuple[list[dict], MagicMock]:
    """
    Run search_rag() over canned points with the dense-only path (empty rag_cfg).

    Returns the results and the mock Qdrant client, so tests can inspect the call.
    """
    qdrant_client = MagicMock(name="qdrant_client")
    qdrant_client.query_points.return_value = SimpleNamespace(points=points)
    results = asyncio.run(
        search.search_rag(
            query="q",
            qdrant_client=qdrant_client,
            rag_cfg={},
            **kwargs,
        )
    )
    return results, qdrant_client


# ---------------------------------------------------------------------------
# _collapse_by_content_hash: pure list logic
# ---------------------------------------------------------------------------

def test_identical_hash_hits_collapse_into_one_with_locations_list():
    # A, B and D share a hash (D is a third copy); C is unique. Rank order is A, B, C, D.
    hits = [
        {"source_path": "A", "content_hash": "h1", "rank": 1},
        {"source_path": "B", "content_hash": "h1", "rank": 2},
        {"source_path": "C", "content_hash": "h2", "rank": 3},
        {"source_path": "D", "content_hash": "h1", "rank": 4},
    ]

    kept = search._collapse_by_content_hash(hits)

    # B and D are dropped; A stands for them and lists every location in rank order.
    assert [h["rank"] for h in kept] == [1, 3]
    assert kept[0]["locations"] == ["A", "B", "D"]
    # The unique hit gets a one-entry locations list, so the result shape is uniform.
    assert kept[1]["locations"] == ["C"]


def test_collapse_does_not_mutate_input_hits():
    hits = [
        {"source_path": "A", "content_hash": "h1"},
        {"source_path": "B", "content_hash": "h1"},
    ]

    search._collapse_by_content_hash(hits)

    assert "locations" not in hits[0]
    assert hits[1]["source_path"] == "B"


def test_empty_file_hash_never_collapses():
    # Every empty file has the same MD5, so such hits are not copies of each other.
    hits = [
        {"source_path": "A", "content_hash": EMPTY_FILE_MD5},
        {"source_path": "B", "content_hash": EMPTY_FILE_MD5},
    ]

    kept = search._collapse_by_content_hash(hits)

    assert [h["source_path"] for h in kept] == ["A", "B"]
    assert [h["locations"] for h in kept] == [["A"], ["B"]]


def test_null_hash_never_collapses():
    # A point with no content_hash (None, or an empty string) never collapses.
    hits = [
        {"source_path": "A", "content_hash": None},
        {"source_path": "B", "content_hash": None},
        {"source_path": "C", "content_hash": ""},
        {"source_path": "D", "content_hash": ""},
    ]

    kept = search._collapse_by_content_hash(hits)

    assert [h["source_path"] for h in kept] == ["A", "B", "C", "D"]
    assert all(h["locations"] == [h["source_path"]] for h in kept)


def test_chunks_of_one_file_are_not_collapsed():
    # content_hash is per file, so two chunks of the same file share a hash. They are
    # the same location, not copies: both are kept, and no location is listed twice.
    hits = [
        {"source_path": "A", "content_hash": "h1", "rank": 1},
        {"source_path": "A", "content_hash": "h1", "rank": 2},
        {"source_path": "B", "content_hash": "h1", "rank": 3},
    ]

    kept = search._collapse_by_content_hash(hits)

    assert [h["rank"] for h in kept] == [1, 2]
    assert kept[0]["locations"] == ["A", "B"]
    assert kept[1]["locations"] == ["A"]


# ---------------------------------------------------------------------------
# search_rag: collapse runs before the per-source cap
# ---------------------------------------------------------------------------

def test_collapse_runs_before_max_hits_per_source_cap(stub_search):
    # cap = 1 per source, limit = 10. Input order: X1 and Y1 are identical copies
    # (hash h1); Y2 is a different Y file (hash h2).
    #   Cap first would count Y1 against Y's cap, so Y2 would be dropped; then
    #   collapse would leave only X1, so the result would be a single hit.
    #   Collapse first drops Y1 before it is counted, so Y2 survives the cap.
    points = [
        _hit("X", "h1", score=0.9),
        _hit("Y", "h1", score=0.8),
        _hit("Y", "h2", score=0.7),
    ]

    results, _ = _run_search(points, user_id=None, limit=10, max_hits_per_source=1)

    assert [r["source_path"] for r in results] == ["X", "Y"]
    assert results[0]["locations"] == ["X", "Y"]
    assert results[1]["locations"] == ["Y"]


def test_search_results_carry_content_hash_and_locations(stub_search):
    points = [_hit("A", "h1"), _hit("Z", None)]

    results, _ = _run_search(points, user_id=None, limit=10)

    assert results[0]["content_hash"] == "h1"
    assert results[0]["locations"] == ["A"]
    assert results[1]["content_hash"] is None
    assert results[1]["locations"] == ["Z"]


def test_search_results_carry_source_id_and_none_when_the_point_has_none(stub_search):
    # A point the source backfill has reached carries source_id; one it has not reached reads as None.
    with_source = SimpleNamespace(
        payload={
            FIELD_SOURCE_PATH: "A",
            FIELD_PARENT_TEXT: "text from A",
            FIELD_CONTENT_HASH: "h1",
            FIELD_SOURCE_ID: "nas",
        },
        score=1.0,
    )
    without_source = _hit("Z", None)

    results, _ = _run_search([with_source, without_source], user_id=None, limit=10)

    assert results[0]["source_id"] == "nas"
    assert results[1]["source_id"] is None


# ---------------------------------------------------------------------------
# search_rag: project_filter is ANDed with the owner filter
# ---------------------------------------------------------------------------

def test_project_filter_is_anded_with_owner_filter(stub_search):
    project_filter = Filter(
        must=[FieldCondition(key=FIELD_PROJECT_ID, match=MatchValue(value="project-1"))]
    )

    _, qdrant_client = _run_search(
        [], user_id="florian", limit=10, project_filter=project_filter
    )

    query_filter = qdrant_client.query_points.call_args.kwargs["filter"]
    keys = [condition.key for condition in query_filter.must]
    # Both the owner condition and the project condition must hold.
    assert keys == [FIELD_OWNER, FIELD_PROJECT_ID]
    assert query_filter.must[1].match.value == "project-1"


def test_no_project_filter_leaves_owner_filter_unchanged(stub_search):

    _, qdrant_client = _run_search([], user_id="florian", limit=10)

    query_filter = qdrant_client.query_points.call_args.kwargs["filter"]
    assert [condition.key for condition in query_filter.must] == [FIELD_OWNER]
