"""
Qdrant collection configuration — single source of truth.

All code that creates, opens, or queries the documents collection imports
constants and helpers from here. No collection names, vector dimensions,
or distance metrics are hardcoded anywhere else.
"""

from __future__ import annotations

import logging

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, Modifier, PayloadSchemaType, SparseVectorParams, VectorParams

from .schema import (
    FIELD_CONTENT_HASH,
    FIELD_FILE_TYPE,
    FIELD_INGESTED_AT,
    FIELD_MODIFIED_AT,
    FIELD_OWNER,
    FIELD_PIPELINE_VERSION,
    FIELD_PROJECT_ID,
    FIELD_SOURCE_ID,
    FIELD_SOURCE_PATH,
    FIELD_SOURCE_TYPE,
    SPARSE_VECTOR_NAME,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Collection constants
# ---------------------------------------------------------------------------

DOCUMENTS_COLLECTION = "documents"
VECTOR_SIZE = 1024          # bge-m3 output dimension (same as mxbai-embed-large)
DISTANCE = Distance.COSINE

# Payload fields that get a KEYWORD index (exact-match filters).
_KEYWORD_INDEX_FIELDS = (
    FIELD_SOURCE_PATH,
    FIELD_PROJECT_ID,
    FIELD_OWNER,
    FIELD_SOURCE_TYPE,
    FIELD_FILE_TYPE,
    FIELD_CONTENT_HASH,
    # v0.3: every point carries its source and the pipeline version that wrote it.
    FIELD_SOURCE_ID,
    FIELD_PIPELINE_VERSION,
)

# Payload fields that get a DATETIME index (range filters).
_DATETIME_INDEX_FIELDS = (
    FIELD_MODIFIED_AT,
    FIELD_INGESTED_AT,
)

# Per-process cache of payload fields already confirmed indexed. Tracked per
# field (not one bool) so a collection that is only partly indexed still gets
# the missing indexes created on the next call. Mirrors the _sparse_ready
# cache pattern in ingest.py.
_payload_indexes_ready: set[str] = set()


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------

def get_client(url: str, timeout: int = 60) -> QdrantClient:
    """Connect to a running Qdrant server.

    Default timeout is 60 seconds to handle index optimization on large batches.
    During heavy ingestion (e.g., 70K+ points), Qdrant may take >5 seconds to
    respond to delete/upsert requests while it optimizes indexes. Default httpx
    timeout (~5s) is too short; 60s covers normal operations. Pass a higher
    value for bulk-delete operations that may trigger longer re-indexing passes.
    """
    return QdrantClient(url=url, timeout=timeout)


# ---------------------------------------------------------------------------
# Collection lifecycle
# ---------------------------------------------------------------------------

def collection_exists(client: QdrantClient, name: str) -> bool:
    """Return True if `name` resolves to a real collection OR to a Qdrant alias.

    Qdrant aliases do not appear in get_collections(), so checking only that
    list reports False for a working alias. rag_sparse_migrate.py --swap makes
    the 'documents' name an alias, which is why this check looks at both.
    """
    real_collections = {c.name for c in client.get_collections().collections}
    aliases = {a.alias_name for a in client.get_aliases().aliases}
    return name in real_collections or name in aliases


def collection_is_alias(client: QdrantClient, name: str) -> bool:
    """Return True if `name` is an alias and NOT itself a real collection.

    Used by destructive operations that must not act on an alias (for example,
    delete_collection() on an alias name would fail or hit the wrong target).
    """
    real_collections = {c.name for c in client.get_collections().collections}
    aliases = {a.alias_name for a in client.get_aliases().aliases}
    return name in aliases and name not in real_collections


def ensure_collection(client: QdrantClient) -> None:
    """
    Create the documents collection if it does not already exist.

    Safe to call on every startup — no-op when the collection is already present.
    Handles both real collections and Qdrant aliases (the alias is created by
    rag_sparse_migrate.py --swap after migration).

    On fresh creation: configures both dense (unnamed, 1024-dim) and sparse
    (SPARSE_VECTOR_NAME, BM25-style with Qdrant IDF modifier).

    Qdrant 1.17 does NOT support adding a sparse vector space to an existing
    collection that was created without one; use rag_sparse_migrate.py to
    migrate the corpus to a new collection with the correct schema.

    Also ensures the payload indexes exist (see ensure_payload_indexes()),
    regardless of whether the collection was just created or already present.
    """
    real_collections = {c.name for c in client.get_collections().collections}
    aliases = {a.alias_name for a in client.get_aliases().aliases}
    if DOCUMENTS_COLLECTION not in real_collections and DOCUMENTS_COLLECTION not in aliases:
        client.create_collection(
            collection_name=DOCUMENTS_COLLECTION,
            vectors_config=VectorParams(size=VECTOR_SIZE, distance=DISTANCE),
            sparse_vectors_config={
                SPARSE_VECTOR_NAME: SparseVectorParams(modifier=Modifier.IDF)
            },
        )
    ensure_payload_indexes(client)


def ensure_payload_indexes(client: QdrantClient) -> None:
    """
    Ensure every field in _KEYWORD_INDEX_FIELDS and _DATETIME_INDEX_FIELDS has a payload index.

    Why: ingestion and sync delete points via
    `Filter(must=[FieldCondition(key="source_path", match=MatchValue(...))])`,
    and the new metadata fields are used for exact-match and range filters.
    Without a payload index, Qdrant must linearly scan every point's payload
    to satisfy such a filter. At the corpus sizes this collection reaches
    (millions of points), that scan routinely exceeds the QdrantClient
    timeout (60s — see get_client()) and ingestion fails with
    "ResponseHandlingException: timed out" — see `docs/RAG-troubleshoot.md`
    §2.8 (Qdrant timeouts during ingestion or sync).

    Keyword fields are indexed with PayloadSchemaType.KEYWORD; datetime fields
    with PayloadSchemaType.DATETIME.

    Idempotent and cheap after the first call in a process: fields already in
    the module-level cache are skipped with no Qdrant round-trip. Otherwise the
    collection's live payload_schema is checked and only the missing fields
    are registered. Building an index over an existing large collection happens
    in the background on the Qdrant server; this call returns as soon as each
    index is registered.
    """
    # Work out which (field, schema) pairs are not yet confirmed in this process.
    wanted = [(field, PayloadSchemaType.KEYWORD) for field in _KEYWORD_INDEX_FIELDS]
    wanted += [(field, PayloadSchemaType.DATETIME) for field in _DATETIME_INDEX_FIELDS]
    missing = [(field, schema) for field, schema in wanted if field not in _payload_indexes_ready]
    if not missing:
        return

    try:
        info = client.get_collection(DOCUMENTS_COLLECTION)
        existing = info.payload_schema or {}
    except Exception:
        return  # collection not created yet; nothing to index

    for field, field_schema in missing:
        if field in existing:
            # Already indexed on the server (possibly by an earlier process).
            _payload_indexes_ready.add(field)
            continue
        logger.info(
            "Creating payload index on '%s' (one-time; Qdrant builds it in the background)",
            field,
        )
        client.create_payload_index(
            collection_name=DOCUMENTS_COLLECTION,
            field_name=field,
            field_schema=field_schema,
        )
        _payload_indexes_ready.add(field)


def collection_has_sparse(client: QdrantClient) -> bool:
    """Return True if the documents collection has the sparse_text vector space.

    Used by ingest.py to decide whether to compute and include sparse vectors.
    False before rag_sparse_migrate.py runs; True after migration + swap.
    """
    try:
        info = client.get_collection(DOCUMENTS_COLLECTION)
        sparse = getattr(info.config.params, "sparse_vectors", None)  # qdrant-client uses "sparse_vectors", not "sparse_vectors_config"
        return sparse is not None and SPARSE_VECTOR_NAME in sparse
    except Exception:
        return False
