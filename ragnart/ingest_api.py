"""
RAGNART ingestion API (v0.3 session B1): the hub's write endpoints for endpoint sources.

An endpoint source is a store that a host submits to (the operator's Mac, for example). The API
is served by its own uvicorn process on port 8101 (scripts/ragnart-ingest-api.service). The search
service on port 8100 stays read-only and does not import this module.

Run (systemd, see scripts/ragnart-ingest-api.service):
    uvicorn ragnart.ingest_api:app --host 0.0.0.0 --port 8101

What the API does, and what it never does:
  - Every request carries `Authorization: Bearer <secret>`. The secret identifies one endpoint
    source. The registry stores only the SHA-256 of each secret.
  - A request writes one row to the intake table of the hub registry, with status 'received'.
    The hub's intake worker applies the row later, under the ingestion lock. A bytes upload is
    first written to the spool directory, <db_base_path>/spool/<request id>.
  - The API never touches Qdrant for a write. The only Qdrant access is the read in GET /v1/items.

Source identity invariant: the source of a request is the source its secret belongs to. The
request body never names a source, so a host cannot write to, or read the status of, another
source's items by putting that source's id in a body or a key.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from qdrant_client.models import FieldCondition, Filter, MatchValue

from ragnart.cli import load_config, registry_db_path
from ragnart.collection import DOCUMENTS_COLLECTION, get_client
from ragnart.registry import RegistryDB
from ragnart.schema import (
    FIELD_CONTENT_HASH,
    FIELD_PIPELINE_VERSION,
    FIELD_SESSION_ID,
    FIELD_SESSION_TITLE,
    FIELD_PROJECT,
    FIELD_SOURCE_ID,
    FIELD_SOURCE_PATH,
    PIPELINE_VERSION_P1,
    SOURCES_HOLDING_PROJECTS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The connector names of the two endpoint connectors built in B1 (ragnart/connectors.py).
CONNECTOR_FILESYSTEM = "filesystem"
CONNECTOR_CLAUDE_CODE = "claude_code"

# Key namespaces. A claude_code source keys its items by session; a filesystem source keys them by
# absolute path. The claude prefix is the same one as ragnart/schema.py _CLAUDE_KEY_PREFIX.
CLAUDE_KEY_PREFIX = "claude-session://"
FILESYSTEM_KEY_PREFIX = "/"

# File types that docling converts. ragnart/ingest.py _DOCLING_TYPES holds the same set.
# ipynb and jsonl are read by their own parsers. Neither is text-like.
_DOCLING_FILE_TYPES = frozenset({"pdf", "docx", "pptx"})
_NON_TEXT_FILE_TYPES = frozenset({"ipynb", "jsonl"})

# The only metadata an upsert may carry. The worker spreads upsert metadata into every point it
# writes, so any other name (source_id, source_path, content_hash, ...) would let a host overwrite
# a field that the hub sets itself. These three are the chat-session fields that
# ragnart/chat_render.py _parse_claude_chat returns.
ALLOWED_UPSERT_METADATA = frozenset({FIELD_SESSION_ID, FIELD_PROJECT, FIELD_SESSION_TITLE})

# The only metadata a change_metadata request may set in B1. Anything else is refused with 422.
CHANGEABLE_METADATA = frozenset({"owner"})

# The intake row fields that GET /v1/status returns. The metadata and text of a request are not returned.
STATUS_FIELDS = (
    "status", "admission_reason", "error", "chunk_count", "content_hash",
    "pipeline_version", "kind", "key", "created_at", "updated_at",
)

# The payload fields that GET /v1/items reads from each point. No vectors are read.
ITEM_PAYLOAD_FIELDS = [FIELD_SOURCE_PATH, FIELD_CONTENT_HASH, FIELD_PIPELINE_VERSION]

# Points per Qdrant scroll page.
SCROLL_PAGE_SIZE = 1000

# Bytes read from an upload per step, so a large upload is never held in memory at once.
UPLOAD_CHUNK_BYTES = 1024 * 1024


# ---------------------------------------------------------------------------
# Config and startup
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Load the config, open the hub registry for writing, and create the spool directory.

    The registry is opened once for the whole process. Every registry call runs on the event-loop
    thread (the endpoints are async, and the Qdrant scroll runs in a worker thread that never
    touches the registry), so the single connection is never used from two threads.
    """
    config = load_config()
    app.state.config = config
    app.state.registry = RegistryDB(registry_db_path(config))

    # The spool holds uploaded bytes until the intake worker ingests them. Owner-only access,
    # because the spool holds other people's files until they are deleted.
    app.state.spool_dir = Path(config["db_base_path"]) / "spool"
    app.state.spool_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

    # The Qdrant client is made on the first /v1/items request, so the API starts even when Qdrant is down.
    app.state.qdrant_client = None
    logger.info("RAGNART ingestion API ready (spool %s)", app.state.spool_dir)

    yield

    app.state.registry.close()


app = FastAPI(title="RAGNART ingestion API", version="0.3.0-b1", lifespan=lifespan)


def _qdrant_client(state) -> Any:
    """Return the Qdrant client, creating it on first use from rag.qdrant_url."""
    if state.qdrant_client is None:
        state.qdrant_client = get_client(state.config["rag"]["qdrant_url"])
    return state.qdrant_client


# ---------------------------------------------------------------------------
# Auth: the secret decides the source
# ---------------------------------------------------------------------------

def _bearer_token(authorization: str | None) -> str | None:
    """Return the token of an `Authorization: Bearer <token>` header, or None when it is absent or malformed."""
    if not authorization:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def _unauthorized() -> HTTPException:
    """The one 401 answer. It does not say whether the secret was absent, malformed or wrong."""
    return HTTPException(
        status_code=401,
        detail="missing or invalid source secret",
        headers={"WWW-Authenticate": "Bearer"},
    )


async def authenticated_source(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict:
    """
    FastAPI dependency: return the endpoint source that the request's secret belongs to.

    The presented secret is hashed with SHA-256 and compared with the stored hash of each endpoint
    source, using hmac.compare_digest. Hub sources have no secret and are not endpoint sources, so
    they can never match. The loop does not stop at the first match, so the time taken does not
    depend on which source matched.

    Returns the source row ({id, connector, owner, secret_hash}). Callers use id, connector and
    owner only; the hash is never returned to a client.
    """
    token = _bearer_token(authorization)
    if token is None:
        raise _unauthorized()
    presented = hashlib.sha256(token.encode("utf-8")).hexdigest()

    matched = None
    for source in request.app.state.registry.endpoint_sources():
        if source["secret_hash"] is None:
            continue
        if hmac.compare_digest(source["secret_hash"], presented):
            matched = source
    if matched is None:
        raise _unauthorized()
    return matched


# ---------------------------------------------------------------------------
# Request checks
# ---------------------------------------------------------------------------

def _check_key(source: dict, key: str) -> None:
    """
    Refuse a key that is outside the source's namespace, with 422.

    A claude_code source keys by 'claude-session://...'. A filesystem source keys by absolute path.
    """
    connector = source["connector"]
    if connector == CONNECTOR_CLAUDE_CODE:
        if key.startswith(CLAUDE_KEY_PREFIX):
            return
        raise HTTPException(422, f"key {key!r} is outside the {source['id']} namespace: "
                                 f"keys for a claude_code source start with {CLAUDE_KEY_PREFIX!r}")
    if connector == CONNECTOR_FILESYSTEM:
        if key.startswith(FILESYSTEM_KEY_PREFIX):
            return
        raise HTTPException(422, f"key {key!r} is outside the {source['id']} namespace: "
                                 f"keys for a filesystem source are absolute paths")
    # Unreachable for the connectors built in B1. Refusing is the safe answer for any other.
    raise HTTPException(422, f"source {source['id']} has connector {connector!r}, which takes no keys here")


def is_text_like_file_type(file_type: str) -> bool:
    """
    True when ingestion reads a file of this type directly as UTF-8 text.

    This mirrors ragnart/ingest.py is_text_like_path(), which cannot be imported here because it
    lives in a module that imports docling. tests/test_ingest_api.py asserts that the two sets
    agree, so a change in ingest.py that is not made here fails the suite.
    """
    raw = file_type.lstrip(".").lower()
    return raw not in _DOCLING_FILE_TYPES and raw not in _NON_TEXT_FILE_TYPES


def _check_upsert_metadata(metadata: dict | None) -> None:
    """Refuse upsert metadata with a field outside ALLOWED_UPSERT_METADATA, with 422."""
    if metadata is None:
        return
    unknown = set(metadata) - ALLOWED_UPSERT_METADATA
    if unknown:
        raise HTTPException(422, f"metadata field(s) not allowed on an upsert: {sorted(unknown)}")


def _parse_metadata_form(raw: str | None) -> dict | None:
    """Parse the metadata form field (a JSON object string), or refuse it with 422."""
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(422, f"metadata is not valid JSON: {exc.msg}") from exc
    if not isinstance(value, dict):
        raise HTTPException(422, "metadata must be a JSON object")
    _check_upsert_metadata(value)
    return value


def _checked_file_type(config: dict, file_type: str) -> str:
    """
    Return a declared file type in canonical form, or refuse it with 422.

    Canonical form is lowercase with no leading dot, so '.MD' becomes 'md'. The type must be one of
    rag.scanner.include_extensions, the list GET /v1/info publishes, so a host can only send types the
    hub would ingest. That list is compared in the same canonical form.
    """
    canonical = file_type.lstrip(".").lower()
    accepted = {ext.lstrip(".").lower() for ext in config["rag"]["scanner"]["include_extensions"]}
    if canonical not in accepted:
        raise HTTPException(
            422,
            f"file type {file_type!r} is not accepted: the hub ingests only the types listed by GET /v1/info",
        )
    return canonical


def _checked_modified_at(raw: str | None) -> str | None:
    """
    Return an item's modification time as an ISO-8601 UTC string, or refuse it with 422.

    The value must carry a UTC offset: a naive time means different instants on different hosts. It is
    converted to UTC, so the stored form equals the one ingest_file writes (isoformat() of a UTC datetime).
    """
    if raw is None:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(422, f"modified_at is not ISO-8601: {raw!r}") from exc
    if parsed.tzinfo is None:
        raise HTTPException(422, "modified_at must carry a UTC offset, e.g. 2023-11-14T22:13:20+00:00")
    return parsed.astimezone(timezone.utc).isoformat()


def _accepted(request_id: str) -> JSONResponse:
    """The 202 answer every write returns: the request id, and the status it was recorded with."""
    return JSONResponse(status_code=202, content={"request_id": request_id, "status": "received"})


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------

class TextUpsertBody(BaseModel):
    """A text upsert: the item's text travels inline. Used by claude_code sources."""
    key: str
    text: str
    metadata: dict[str, Any] | None = None
    # ISO-8601 with a UTC offset; checked by _checked_modified_at().
    modified_at: str | None = None


class DeleteBody(BaseModel):
    """Remove one item of this source."""
    key: str


class ChangeKeyBody(BaseModel):
    """Move one item of this source to a new key, with its points kept."""
    key: str
    new_key: str


class ChangeMetadataBody(BaseModel):
    """Change fields of one item of this source. In B1 only `owner` is allowed."""
    key: str
    metadata: dict[str, Any]


class ProjectBody(BaseModel):
    """A request to publish a ragnart project. Always refused in B1."""
    id: str
    name: str
    path: str | None = None


# ---------------------------------------------------------------------------
# Writes: each one records an intake row and returns 202
# ---------------------------------------------------------------------------

@app.post("/v1/upsert/bytes", status_code=202)
async def upsert_bytes(
    request: Request,
    file: UploadFile = File(...),
    key: str = Form(...),
    file_type: str = Form(...),
    metadata: str | None = Form(default=None),
    modified_at: str | None = Form(default=None),
    source: dict = Depends(authenticated_source),
):
    """
    Accept a file's bytes for a filesystem source. The bytes are spooled, and ingested later.

    The file type must be an accepted include extension (422 otherwise), and is stored in canonical
    form. A text-like file above rag.admission.max_text_file_size_bytes is refused with 413 and nothing
    is spooled. The cap is checked while the upload streams, and a partial file is removed. Other
    file types have no size cap here: docling files are checked by the worker.
    """
    # A claude_code source sends sessions as text (POST /v1/upsert/text), never as bytes.
    if source["connector"] == CONNECTOR_CLAUDE_CODE:
        raise HTTPException(422, f"source {source['id']} is a claude_code source: send its sessions "
                                 "with POST /v1/upsert/text, not as bytes")
    _check_key(source, key)
    parsed_metadata = _parse_metadata_form(metadata)
    parsed_modified_at = _checked_modified_at(modified_at)

    state = request.app.state
    # From here on the canonical type is used: the one stored on the row and the one the size rule sees.
    file_type = _checked_file_type(state.config, file_type)
    size_cap = None
    if is_text_like_file_type(file_type):
        size_cap = state.config["rag"]["admission"]["max_text_file_size_bytes"]

    # The request id names the spool file, and is also the intake row's id, so the worker can find
    # the file from the row. The file is written in full before the row exists.
    request_id = uuid.uuid4().hex
    spool_path = state.spool_dir / request_id
    written = 0
    try:
        # 'xb' fails if the file already exists, so a spool file is never overwritten.
        with open(spool_path, "xb") as out:
            while chunk := await file.read(UPLOAD_CHUNK_BYTES):
                written += len(chunk)
                if size_cap is not None and written > size_cap:
                    raise HTTPException(
                        413,
                        f"text-like file is larger than the {size_cap}-byte admission cap",
                    )
                out.write(chunk)
        state.registry.insert_intake(
            source["id"], "upsert", key,
            file_type=file_type, metadata=parsed_metadata, modified_at=parsed_modified_at,
            spool_path=str(spool_path), request_id=request_id,
        )
    except BaseException:
        # Any failure (the cap, a dropped connection, a registry error) leaves no spool file behind.
        if spool_path.exists():
            os.remove(spool_path)
        raise
    return _accepted(request_id)


@app.post("/v1/upsert/text", status_code=202)
async def upsert_text(
    request: Request,
    body: TextUpsertBody,
    source: dict = Depends(authenticated_source),
):
    """Accept an item's text inline. Only claude_code sources send text: a filesystem source sends bytes."""
    if source["connector"] == CONNECTOR_FILESYSTEM:
        raise HTTPException(422, f"source {source['id']} is a filesystem source: send its files as bytes "
                                 "with POST /v1/upsert/bytes, not as text")
    _check_key(source, body.key)
    _check_upsert_metadata(body.metadata)
    parsed_modified_at = _checked_modified_at(body.modified_at)
    request_id = request.app.state.registry.insert_intake(
        source["id"], "upsert", body.key, text=body.text, metadata=body.metadata,
        modified_at=parsed_modified_at,
    )
    return _accepted(request_id)


@app.post("/v1/delete", status_code=202)
async def delete_item(
    request: Request,
    body: DeleteBody,
    source: dict = Depends(authenticated_source),
):
    """Ask the hub to remove one item of this source. The worker deletes its points."""
    _check_key(source, body.key)
    request_id = request.app.state.registry.insert_intake(source["id"], "delete", body.key)
    return _accepted(request_id)


@app.post("/v1/change-key", status_code=202)
async def change_key(
    request: Request,
    body: ChangeKeyBody,
    source: dict = Depends(authenticated_source),
):
    """Ask the hub to rewrite one item's key, so a moved file keeps its points."""
    # A claude_code item is a session, keyed by its id. A session never moves, so the request is refused.
    if source["connector"] == CONNECTOR_CLAUDE_CODE:
        raise HTTPException(422, f"source {source['id']} is a claude_code source: sessions are keyed by id "
                                 "and never move")
    _check_key(source, body.key)
    _check_key(source, body.new_key)
    request_id = request.app.state.registry.insert_intake(
        source["id"], "change_key", body.key, new_key=body.new_key,
    )
    return _accepted(request_id)


@app.post("/v1/change-metadata", status_code=202)
async def change_metadata(
    request: Request,
    body: ChangeMetadataBody,
    source: dict = Depends(authenticated_source),
):
    """Ask the hub to change one item's metadata. In B1 only `owner` may change; anything else is 422."""
    _check_key(source, body.key)
    unknown = set(body.metadata) - CHANGEABLE_METADATA
    if unknown:
        raise HTTPException(422, f"metadata field(s) not changeable in this API version: {sorted(unknown)}")
    if "owner" in body.metadata and not isinstance(body.metadata["owner"], str):
        raise HTTPException(422, "owner must be a string")
    request_id = request.app.state.registry.insert_intake(
        source["id"], "change_metadata", body.key, metadata=body.metadata,
    )
    return _accepted(request_id)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

@app.get("/v1/status/{request_id}")
async def request_status(
    request: Request,
    request_id: str,
    source: dict = Depends(authenticated_source),
):
    """
    Return how far one request has got, with its admission result and error when it has them.

    A request id that belongs to another source answers 404, the same as an unknown id, so a host
    cannot tell which request ids exist elsewhere.
    """
    row = request.app.state.registry.intake_by_id(request_id)
    if row is None or row["source_id"].casefold() != source["id"].casefold():
        raise HTTPException(404, "no such request")
    return {field: row[field] for field in STATUS_FIELDS}


def _scroll_source_points(client: Any, source_id: str) -> list[dict]:
    """
    Return the payload of every point whose source_id is `source_id`, read from Qdrant page by page.

    Runs in a worker thread (asyncio.to_thread), because the Qdrant client is synchronous.
    """
    scroll_filter = Filter(must=[
        FieldCondition(key=FIELD_SOURCE_ID, match=MatchValue(value=source_id)),
    ])
    payloads: list[dict] = []
    offset = None
    while True:
        points, offset = client.scroll(
            collection_name=DOCUMENTS_COLLECTION,
            scroll_filter=scroll_filter,
            with_payload=ITEM_PAYLOAD_FIELDS,
            with_vectors=False,
            limit=SCROLL_PAGE_SIZE,
            offset=offset,
        )
        payloads.extend(point.payload for point in points)
        if offset is None:
            return payloads


def group_points_by_key(payloads: list[dict]) -> list[dict]:
    """
    Group point payloads into one item per source_path (the key), sorted by key.

    Each item carries its chunk count, and the content hash and pipeline version of its points.
    The points of one item share them, so the first point's values are taken.

    The hub keeps no per-item table: the items are derived from the points each time, because the
    hub keeps nothing beyond the chunks themselves (design D19).
    """
    items: dict[str, dict] = {}
    for payload in payloads:
        key = payload[FIELD_SOURCE_PATH]
        if key not in items:
            items[key] = {
                "key": key,
                "chunk_count": 0,
                "content_hash": payload.get(FIELD_CONTENT_HASH),
                "pipeline_version": payload.get(FIELD_PIPELINE_VERSION),
            }
        items[key]["chunk_count"] += 1
    return sorted(items.values(), key=lambda item: item["key"])


@app.get("/v1/items")
async def list_items(
    request: Request,
    source: dict = Depends(authenticated_source),
):
    """
    List this source's items, derived live from the Qdrant points whose source_id is this source.

    Only this source's points are read: the filter carries the authenticated source's id, never a
    value from the request.
    """
    client = _qdrant_client(request.app.state)
    payloads = await asyncio.to_thread(_scroll_source_points, client, source["id"])
    return {"items": group_points_by_key(payloads)}


@app.get("/v1/info")
async def info(
    request: Request,
    source: dict = Depends(authenticated_source),
):
    """Return the pipeline version, the accepted file types and the admission limits, from ragnart.yaml."""
    rag = request.app.state.config["rag"]
    admission = rag["admission"]
    return {
        "pipeline_version": PIPELINE_VERSION_P1,
        "accepted_file_types": list(rag["scanner"]["include_extensions"]),
        "admission": {
            "max_text_file_size_bytes": admission["max_text_file_size_bytes"],
            "max_chunks_per_item": admission["max_chunks_per_item"],
            "fffd_ratio_threshold": admission["fffd_ratio_threshold"],
        },
    }


# ---------------------------------------------------------------------------
# Projects: refused in B1
# ---------------------------------------------------------------------------

@app.post("/v1/projects")
async def publish_project(
    body: ProjectBody,
    source: dict = Depends(authenticated_source),
):
    """
    Refuse to publish a ragnart project from this source, and report the attempt in the log.

    Only sources with the ragnart_projects capability may hold projects. In B1 that is nas alone,
    so every other source is refused. Project mode arrives in v0.4.
    """
    capable = ", ".join(sorted(SOURCES_HOLDING_PROJECTS))
    reason = (
        f"{source['id']} cannot hold ragnart projects "
        f"(only sources with the ragnart_projects capability, today {capable})"
    )
    logger.warning(
        "refused project publish: source=%s project=%s name=%s",
        source["id"], body.id, body.name,
    )
    return JSONResponse(status_code=409, content={"refused": True, "reason": reason})
