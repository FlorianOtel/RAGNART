"""
RAG search — query → embed → Qdrant query_points → parent_text + provenance.

search_rag() is the single function exported by ragnart/__init__.py.
Ownership filtering is applied internally; the caller only provides
query, user_id, and limit.
"""

from __future__ import annotations

import logging

from qdrant_client import QdrantClient
from qdrant_client.models import (
    FieldCondition,
    Filter,
    Fusion,
    FusionQuery,
    MatchAny,
    Prefetch,
    SparseVector,
)

from .collection import DOCUMENTS_COLLECTION, collection_has_sparse, ensure_collection
from .embeddings import embed_sparse, embed_text
from .moves import EMPTY_FILE_MD5
from .rerank import rerank as _rerank_candidates
from .schema import (
    FIELD_CONTENT_HASH,
    FIELD_FILE_NAME,
    FIELD_FILE_TYPE,
    FIELD_OWNER,
    FIELD_PARENT_TEXT,
    FIELD_SESSION_TITLE,
    FIELD_SOURCE_ID,
    FIELD_SOURCE_PATH,
    FIELD_TEXT,
    SPARSE_VECTOR_NAME,
)

logger = logging.getLogger(__name__)

# Fallback per-source cap, used only when ragnart.yaml omits rag.search.max_hits_per_source.
# resolve_max_hits_per_source() is the single place that applies it.
DEFAULT_MAX_HITS_PER_SOURCE = 3


def resolve_max_hits_per_source(value: int | None, rag_cfg: dict) -> int:
    """
    Return the effective per-source cap for one search.

    A per-request value wins; otherwise the config value; otherwise the module default.

    This is the single source of truth for the default. search_rag(),
    ragnart/service.py and utils/rag_search_cli.py all call it, so the value echoed
    in the HTTP response is always the value that was actually applied.

    Args:
        value:   Per-request override, or None to use the config default.
        rag_cfg: config["rag"] dict; read rag_cfg["search"]["max_hits_per_source"].

    Returns:
        The cap as an int (>= 1 when the caller validates input).
    """
    if value is not None:
        return value
    return rag_cfg.get("search", {}).get("max_hits_per_source", DEFAULT_MAX_HITS_PER_SOURCE)


def _collapse_by_content_hash(results: list[dict]) -> list[dict]:
    """
    Collapse identical copies of a document into one hit that lists every location.

    Walks the list once, in input (rank) order. Every hit gets a "locations" list.

      - A hit with no content_hash (None or "") or with the empty-file MD5 never
        collapses: it is kept with locations = [its own source_path]. Empty files are
        all identical, so grouping them would be meaningless.
      - The first hit with a given content_hash represents its group and is kept.
      - A later hit with the same hash from a different source_path is an identical
        copy: it is dropped, and its source_path is appended to the representative's
        locations (once).
      - A later hit with the same hash from the SAME source_path is another chunk of
        the same file, not a copy: it is kept as its own hit. content_hash is per
        file, so every chunk of one file carries the same hash.

    Input dicts are not mutated; kept hits are shallow copies.
    """
    kept: list[dict] = []
    # content_hash -> the kept hit that represents that group of identical copies.
    representatives: dict[str, dict] = {}
    for hit in results:
        content_hash = hit.get("content_hash")
        source = hit["source_path"]

        # Empty-file hash and NULL hash never collapse.
        if not content_hash or content_hash == EMPTY_FILE_MD5:
            kept.append({**hit, "locations": [source]})
            continue

        representative = representatives.get(content_hash)
        if representative is None:
            # First hit with this hash: it represents the group.
            representative = {**hit, "locations": [source]}
            representatives[content_hash] = representative
            kept.append(representative)
        elif source == representative["source_path"]:
            # Another chunk of the representative's own file: not a copy, keep it.
            kept.append({**hit, "locations": [source]})
        elif source not in representative["locations"]:
            # An identical copy from a new location: list it, drop the hit.
            representative["locations"].append(source)
        # else: this copy's location is already listed; drop the hit.
    return kept


def _cap_hits_per_source(results: list[dict], max_hits_per_source: int) -> list[dict]:
    """
    Keep at most max_hits_per_source hits for each source_path, in input order.

    Walks the list once. A hit is kept only while its source's running count is
    still within the cap. Hits are never added back from outside the list, so a
    short candidate pool returns fewer results rather than triggering a re-query.
    """
    kept: list[dict] = []
    counts: dict[str, int] = {}
    for hit in results:
        source = hit["source_path"]
        counts[source] = counts.get(source, 0) + 1
        if counts[source] <= max_hits_per_source:
            kept.append(hit)
    return kept


async def search_rag(
    query: str,
    user_id: str | None,
    limit: int,
    qdrant_client: QdrantClient,
    rag_cfg: dict,
    file_types: list[str] | None = None,
    score_threshold: float = 0.0,
    rerank: bool | None = None,
    rerank_cfg: dict | None = None,
    hybrid: bool | None = None,
    max_hits_per_source: int | None = None,
    project_filter: Filter | None = None,
) -> list[dict]:
    """
    Semantic search over the user's documents + shared (la-familia) content.

    Steps:
      1. Embed the query via Ollama (bge-m3); optionally compute sparse vectors (BM25).
      2. Build a Qdrant filter scoped to user_id + "la-familia", ANDed with the
         optional project_filter and file_types conditions.
      3. query_points() → ranked ScoredPoint list (hybrid RRF or dense-only).
      4. Optionally rerank via cross-encoder.
      5. Collapse identical copies by content_hash (each kept hit lists every location).
      6. Apply the per-source cap, then the limit slice.
      7. Return parent_text (richer context for the LLM) + provenance metadata.

    If user_id is None (pre-auth / dev mode), no ownership filter is applied
    and all documents are searched.

    Args:
        query:           User's question or search string.
        user_id:         Owner string (e.g. "florian") from the authenticated session,
                         or None to search without an ownership filter.
        limit:           Max number of results to return.
        qdrant_client:   Shared Qdrant client (created once at app startup).
        rag_cfg:         config["rag"] dict — embedding_model, ollama_url, rerank, hybrid.
        file_types:      Optional list of file types to filter by.
        score_threshold: Minimum cosine score; 0.0 = no filter (only for dense-only path).
        rerank:          Enable cross-encoder reranking; None = read from rag_cfg default.
        rerank_cfg:      Rerank config dict; None = read from rag_cfg default.
        hybrid:          Enable hybrid dense+sparse search; None = read from rag_cfg default.
        max_hits_per_source: Per-source cap applied after collapse and before the limit
                         slice; None = read from rag_cfg default (see resolve_max_hits_per_source).
        project_filter:  Optional Qdrant Filter for project scoping. Only its `must`
                         conditions are read; they are ANDed with the owner and file_types
                         conditions. None = no project scoping.

    Returns:
        List of dicts with keys:
          content         — parent_text injected into the LLM prompt
          source_path     — full NFS path returned to the user as provenance
          source_id       — the source the point came from (e.g. 'nas'); None on a point
                            the source backfill has not reached yet
          score           — cosine similarity (0–1, higher = more relevant) or RRF rank
          rerank_score    — (optional) cross-encoder relevance score
          file_name       — filename for display
          file_type       — pdf | md | ipynb | etc.
          content_hash    — MD5 of the source content, or None when the point has none
          locations       — every source_path this hit stands for: [source_path] alone for
                            a unique hit; the kept hit's own path followed by the paths of
                            the identical copies it absorbed
    """
    model      = rag_cfg.get("embedding_model", "bge-m3")
    ollama_url = rag_cfg.get("ollama_url", "http://192.168.1.95:11434/api/embeddings")

    # -- 0b. Resolve effective rerank settings ----
    _rerank_section = rag_cfg.get("rerank", {})
    effective_rerank = rerank if rerank is not None else _rerank_section.get("enabled", False)
    effective_rerank_cfg = rerank_cfg or _rerank_section

    # -- 0c. Resolve effective hybrid settings ----
    _hybrid_section = rag_cfg.get("hybrid", {})
    effective_hybrid = hybrid if hybrid is not None else _hybrid_section.get("enabled", False)

    # -- 0d. Resolve effective per-source cap ----
    effective_max_hits_per_source = resolve_max_hits_per_source(max_hits_per_source, rag_cfg)

    # -- 1. Embed the query ------------------------------------------------
    vector = await embed_text(query, model=model, ollama_url=ollama_url)

    # -- 2. Build filter (ownership + optional file_types + optional project) ----
    must: list = []
    if user_id:
        must.append(
            FieldCondition(
                key=FIELD_OWNER,
                match=MatchAny(any=[user_id, "la-familia"]),
            )
        )
    if file_types:
        must.append(
            FieldCondition(
                key=FIELD_FILE_TYPE,
                match=MatchAny(any=file_types),
            )
        )
    # Project scoping is ANDed in: every project condition must hold as well.
    if project_filter is not None and project_filter.must:
        must.extend(project_filter.must)
    query_filter: Filter | None = Filter(must=must) if must else None

    # -- 3. Search ---------------------------------------------------------
    ensure_collection(qdrant_client)

    # Determine fetch limit based on reranking
    if effective_rerank:
        fetch_multiplier = effective_rerank_cfg.get("fetch_multiplier", 6)
        fetch_cap = effective_rerank_cfg.get("fetch_cap", 30)
        _fetch_limit = min(limit * fetch_multiplier, fetch_cap)
    else:
        _fetch_limit = min(limit * 3, 50)

    # Hybrid or dense-only query
    if effective_hybrid:
        # Check if collection supports sparse vectors; fall back to dense if not
        if not collection_has_sparse(qdrant_client):
            logger.info(
                "hybrid query requested but collection lacks sparse_text; using dense-only"
            )
            result = qdrant_client.query_points(
                collection_name=DOCUMENTS_COLLECTION,
                query=vector,
                filter=query_filter,
                limit=_fetch_limit,
                score_threshold=score_threshold if score_threshold > 0.0 else None,
                with_payload=True,
            )
        else:
            # Compute sparse vectors for the query
            sparse_indices, sparse_values = embed_sparse(query)

            # Read hybrid config parameters
            dense_fetch = _hybrid_section.get("dense_fetch", 30)
            sparse_fetch = _hybrid_section.get("sparse_fetch", 30)

            # Compute outer limit for RRF fusion
            if effective_rerank:
                fetch_cap = effective_rerank_cfg.get("fetch_cap", 30)
                outer_limit = min(dense_fetch + sparse_fetch, fetch_cap)
            else:
                outer_limit = min(dense_fetch + sparse_fetch, 50)

            # Execute hybrid query with RRF fusion
            result = qdrant_client.query_points(
                collection_name=DOCUMENTS_COLLECTION,
                prefetch=[
                    Prefetch(
                        query=vector,
                        using="",
                        limit=dense_fetch,
                        filter=query_filter,
                    ),
                    Prefetch(
                        query=SparseVector(indices=sparse_indices, values=sparse_values),
                        using=SPARSE_VECTOR_NAME,
                        limit=sparse_fetch,
                        filter=query_filter,
                    ),
                ],
                query=FusionQuery(fusion=Fusion.RRF),
                limit=outer_limit,
                with_payload=True,
            )
    else:
        # Dense-only query (existing behavior)
        result = qdrant_client.query_points(
            collection_name=DOCUMENTS_COLLECTION,
            query=vector,
            filter=query_filter,
            limit=_fetch_limit,
            score_threshold=score_threshold if score_threshold > 0.0 else None,
            with_payload=True,
        )

    # -- 4. Unpack hits and build full results (with FIELD_TEXT for reranker) ----
    # source_id is None for a point that the source backfill has not reached yet.
    results_full = [
        {
            "content":       (hit.payload or {}).get(FIELD_PARENT_TEXT, ""),
            "source_path":   (hit.payload or {}).get(FIELD_SOURCE_PATH, ""),
            "source_id":     (hit.payload or {}).get(FIELD_SOURCE_ID),
            "score":         hit.score,
            "file_name":     (hit.payload or {}).get(FIELD_FILE_NAME, ""),
            "file_type":     (hit.payload or {}).get(FIELD_FILE_TYPE, ""),
            "session_title": (hit.payload or {}).get(FIELD_SESSION_TITLE, ""),
            "content_hash":  (hit.payload or {}).get(FIELD_CONTENT_HASH),
            FIELD_TEXT:      (hit.payload or {}).get(FIELD_TEXT, ""),
        }
        for hit in result.points
    ]

    # -- 5. Optionally rerank ----
    if effective_rerank and results_full:
        results_full = await _rerank_candidates(query, results_full, effective_rerank_cfg)

    # -- 6. Collapse identical copies (after rerank, before the per-source cap) ----
    # Fixed shaping order (docs/RAGNART-design.md §9.1): rerank, collapse, cap, top_k.
    results_full = _collapse_by_content_hash(results_full)

    # -- 7. Apply per-source cap (after collapse, before the limit slice) ----
    # Runs on the full reranked pool so that the top_k cut is taken from the capped list.
    results_full = _cap_hits_per_source(results_full, effective_max_hits_per_source)

    # -- 8. Slice to limit ----
    results = results_full[:limit]

    logger.debug(
        "search_rag: user_id=%s  query=%r  hybrid=%s  rerank=%s → %d result(s)",
        user_id, query[:60], effective_hybrid, effective_rerank, len(results),
    )
    return results
