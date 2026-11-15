"""
Unit tests for the per-source cap (max_hits_per_source) in ragnart/search.py.

No live Qdrant or Ollama is used:
  - embed_text() and ensure_collection() are replaced in the search module,
    so no embedding model is called and no collection is touched.
  - The Qdrant client is a MagicMock whose query_points() returns canned hits.

The async test is driven with asyncio.run(), the same style as tests/test_ingest_admission.py.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ragnart.search as search
from ragnart.schema import FIELD_PARENT_TEXT, FIELD_SOURCE_PATH


def _hit(source_path: str, score: float = 1.0) -> SimpleNamespace:
    """Build a fake Qdrant ScoredPoint carrying the payload fields search_rag() reads."""
    return SimpleNamespace(
        payload={FIELD_SOURCE_PATH: source_path, FIELD_PARENT_TEXT: f"text from {source_path}"},
        score=score,
    )


# ---------------------------------------------------------------------------
# _cap_hits_per_source: pure list logic
# ---------------------------------------------------------------------------

def test_cap_keeps_at_most_n_per_source_preserving_order():
    # Input order: A, B, A, A, B, C. With a cap of 2 the third "A" must be dropped,
    # the first two "B" are kept, and "C" (only one hit) is kept as is.
    hits = [
        {"source_path": "A", "rank": 1},
        {"source_path": "B", "rank": 2},
        {"source_path": "A", "rank": 3},
        {"source_path": "A", "rank": 4},
        {"source_path": "B", "rank": 5},
        {"source_path": "C", "rank": 6},
    ]

    kept = search._cap_hits_per_source(hits, max_hits_per_source=2)

    # Ranks 1, 2, 3, 5, 6 survive: rank 4 is the third "A" and is dropped.
    assert [h["rank"] for h in kept] == [1, 2, 3, 5, 6]


# ---------------------------------------------------------------------------
# resolve_max_hits_per_source: one default, request value wins
# ---------------------------------------------------------------------------

def test_resolve_max_hits_per_source_falls_back_to_config():
    # No request value: the config value is used.
    assert search.resolve_max_hits_per_source(None, {"search": {"max_hits_per_source": 7}}) == 7


def test_resolve_max_hits_per_source_falls_back_to_module_default():
    # No request value and no config block: the module default is used.
    assert search.resolve_max_hits_per_source(None, {}) == search.DEFAULT_MAX_HITS_PER_SOURCE
    assert search.DEFAULT_MAX_HITS_PER_SOURCE == 3


def test_resolve_max_hits_per_source_request_override_wins():
    # A request value beats the config value.
    assert search.resolve_max_hits_per_source(2, {"search": {"max_hits_per_source": 7}}) == 2


# ---------------------------------------------------------------------------
# search_rag: the cap is applied before the top_k slice
# ---------------------------------------------------------------------------

def test_search_rag_applies_cap_before_top_k_slice(monkeypatch):
    # Stub out the embedder and the collection check; the query itself is never sent.
    async def fake_embed_text(text, model, ollama_url):
        return [0.0]

    monkeypatch.setattr(search, "embed_text", fake_embed_text)
    monkeypatch.setattr(search, "ensure_collection", lambda client: None)

    # Ten interleaved hits: five from "A", five from "B". Qdrant returns all ten
    # in one call, so the cap decides what is left before the limit of 10 applies.
    points = [_hit("A" if i % 2 == 0 else "B", score=1.0 - i * 0.01) for i in range(10)]
    qdrant_client = MagicMock(name="qdrant_client")
    qdrant_client.query_points.return_value = SimpleNamespace(points=points)

    # An empty rag_cfg keeps rerank and hybrid off, so the dense-only branch runs.
    results = asyncio.run(
        search.search_rag(
            query="q",
            user_id=None,
            limit=10,
            qdrant_client=qdrant_client,
            rag_cfg={},
            max_hits_per_source=2,
        )
    )

    # At most 2 per source, and 4 in total: fewer than the limit of 10.
    # The pool is not re-queried to fill the gap.
    assert len(results) == 4
    sources = [r["source_path"] for r in results]
    assert sources.count("A") == 2
    assert sources.count("B") == 2
    assert qdrant_client.query_points.call_count == 1
