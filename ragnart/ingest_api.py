"""
RAGNART ingestion API (v0.3 sessions B1 and B2): the hub's write endpoints for every source.

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
    FIELD_FILE_TYPE,
    FIELD_OWNER,
    FIELD_PIPELINE_VERSION,
    FIELD_PROJECT_ID,
    FIELD_SESSION_ID,
    FIELD_SESSION_TITLE,
    FIELD_PROJECT,
    FIELD_SOURCE_ID,
    FIELD_SOURCE_PATH,
    FIELD_SOURCE_TYPE,
    GLOBAL_PROJECT_ID,
    OPENCODE_KEY_PREFIX,
    PIPELINE_VERSION_P1,
    SOURCES_WITH_PROJECT_SUPPORT,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The connector names of the connectors that may submit (ragnart/connectors.py).
CONNECTOR_FILESYSTEM = "filesystem"
CONNECTOR_CLAUDE_CODE = "claude_code"
CONNECTOR_OPENCODE = "opencode"

# The connectors whose items are sessions. They send their items as text (never as bytes), and a
# session is keyed by its id, so it never moves: a change_key from one of them is refused.
SESSION_CONNECTORS = frozenset({CONNECTOR_CLAUDE_CODE, CONNECTOR_OPENCODE})

# Key namespaces. A claude_code source keys its items by session; a filesystem source keys them by
# absolute path; an opencode source keys them by session as well. The claude prefix is the same one
# as ragnart/schema.py _CLAUDE_KEY_PREFIX, and the opencode prefix is ragnart/schema.py OPENCODE_KEY_PREFIX.
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

# The only metadata a change_metadata request may set. Anything else is refused with 422.
# project_id must name a project in the registry (or 'global'); the owner is checked as a string only.
CHANGEABLE_METADATA = frozenset({"owner", "project_id"})

# The intake row fields that GET /v1/status returns. The metadata and text of a request are not returned.
STATUS_FIELDS = (
    "status", "admission_reason", "error", "chunk_count", "content_hash",
    "pipeline_version", "kind", "key", "created_at", "updated_at",
)

# The payload fields that GET /v1/items reads from each point. No vectors are read.
# owner, project_id, source_type and file_type are read too, so the NAS reconcile can compare them with its
# own rows (drift) and rebuild a lost state database from the listing. Every item field is one value; see
# group_points_by_key() for how points that disagree are reported.
ITEM_PAYLOAD_FIELDS = [
    FIELD_SOURCE_PATH,
    FIELD_CONTENT_HASH,
    FIELD_PIPELINE_VERSION,
    FIELD_OWNER,
    FIELD_PROJECT_ID,
    FIELD_SOURCE_TYPE,
    FIELD_FILE_TYPE,
]

# The per-item fields that a listed item takes from its points, besides chunk_count. Each one is
# read from the item's first point. When another point of the same item disagrees, the field name is
# reported in the item's `inconsistent` list, so a caller can see that the value is not reliable.
ITEM_VALUE_FIELDS = (
    FIELD_CONTENT_HASH,
    FIELD_PIPELINE_VERSION,
    FIELD_OWNER,
    FIELD_PROJECT_ID,
    FIELD_SOURCE_TYPE,
    FIELD_FILE_TYPE,
)

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

    The presented secret is hashed with SHA-256 and compared with the stored hash of each source that
    holds a secret, using hmac.compare_digest. A source's kind does not decide whether it may submit:
    a source with a secret hash may, and a source without one (a NULL hash) may not. The loop does not
    stop at the first match, so the time taken does not depend on which source matched.

    Returns the source row ({id, connector, owner, secret_hash}). Callers use id, connector and
    owner only; the hash is never returned to a client.
    """
    token = _bearer_token(authorization)
    if token is None:
        raise _unauthorized()
    presented = hashlib.sha256(token.encode("utf-8")).hexdigest()

    matched = None
    for source in request.app.state.registry.sources_with_secret():
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

    A claude_code source keys by 'claude-session://...'. An opencode source keys by 'opencode://...'.
    A filesystem source keys by absolute path.
    """
    connector = source["connector"]
    if connector == CONNECTOR_CLAUDE_CODE:
        if key.startswith(CLAUDE_KEY_PREFIX):
            return
        raise HTTPException(422, f"key {key!r} is outside the {source['id']} namespace: "
                                 f"keys for a claude_code source start with {CLAUDE_KEY_PREFIX!r}")
    if connector == CONNECTOR_OPENCODE:
        if key.startswith(OPENCODE_KEY_PREFIX):
            return
        raise HTTPException(422, f"key {key!r} is outside the {source['id']} namespace: "
                                 f"keys for an opencode source start with {OPENCODE_KEY_PREFIX!r}")
    if connector == CONNECTOR_FILESYSTEM:
        if key.startswith(FILESYSTEM_KEY_PREFIX):
            return
        raise HTTPException(422, f"key {key!r} is outside the {source['id']} namespace: "
                                 f"keys for a filesystem source are absolute paths")
    # Unreachable for the connectors named above. Refusing is the safe answer for any other.
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
    converted to UTC, so the stored form is isoformat() of a UTC datetime.
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


def configured_owners(config: dict) -> set[str]:
    """
    Return the owner names configured in ragnart.yaml: the owner of each `users` entry, and `shared`'s owner.

    These are the only names an upsert's owner may take. The same keys are read by ragnart/scanner.py
    when the NAS is crawled, so a name the hub accepts is one the crawl would use.
    """
    owners = {entry["owner"] for entry in config.get("users", {}).values()}
    shared = config.get("shared", {})
    if "owner" in shared:
        owners.add(shared["owner"])
    return owners


def _checked_owner(config: dict, source: dict, owner: str | None) -> str | None:
    """
    Return the owner an upsert may carry for this source, or refuse it with 422.

    A source with a fixed owner (its registry owner is set) takes no owner in the request: the worker
    writes that fixed owner. A source with no fixed owner (nas, whose items have several owners) must
    name the owner of each item, and the name must be one configured in ragnart.yaml. The owner is
    stored on the request row; None means the request did not carry one.
    """
    if source["owner"] is not None:
        if owner is not None:
            raise HTTPException(422, f"source {source['id']} has the fixed owner {source['owner']!r}: "
                                     "an upsert may not name an owner")
        return None
    if owner is None:
        raise HTTPException(422, f"source {source['id']} has no fixed owner: an upsert must name the "
                                 "owner of the item")
    if owner not in configured_owners(config):
        raise HTTPException(422, f"owner {owner!r} is not configured in ragnart.yaml (users or shared)")
    return owner


def _checked_project_id(registry: RegistryDB, project_id: str | None) -> str | None:
    """
    Return a project id that exists in the hub registry, or 'global'; refuse any other with 422.

    None means the request did not name a project, and passes through unchanged.
    """
    if project_id is None or project_id == GLOBAL_PROJECT_ID:
        return project_id
    known = {project["id"] for project in registry.list_projects()}
    if project_id not in known:
        raise HTTPException(422, f"project {project_id!r} does not exist in the registry")
    return project_id


def _accepted(request: Request, source: dict, request_id: str) -> JSONResponse:
    """
    The 202 answer every write returns: the request id, and the status it was recorded with.

    Logs one INFO line per accepted write, naming the route, the source and the caller's User-Agent
    (cut to 200 characters), so the journal shows which program submitted which request.
    The line holds no secret: the Authorization header is never read here.
    """
    user_agent = request.headers.get("user-agent", "-")[:200]
    logger.info("%s from %s (%s) -> request %s", request.url.path, source["id"], user_agent, request_id)
    return JSONResponse(status_code=202, content={"request_id": request_id, "status": "received"})


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------

class TextUpsertBody(BaseModel):
    """A text upsert: the item's text travels inline. Used by claude_code and opencode sources."""
    key: str
    text: str
    metadata: dict[str, Any] | None = None
    # ISO-8601 with a UTC offset; checked by _checked_modified_at().
    modified_at: str | None = None
    # The item's owner (only for a source with no fixed owner), its tag, and its ragnart project.
    # Checked by _checked_owner() and _checked_project_id().
    owner: str | None = None
    tag: str | None = None
    project_id: str | None = None


class DeleteBody(BaseModel):
    """Remove one item of this source."""
    key: str


class ChangeKeyBody(BaseModel):
    """Move one item of this source to a new key, with its points kept."""
    key: str
    new_key: str


class ChangeMetadataBody(BaseModel):
    """Change fields of one item of this source: `owner` or `project_id` (see CHANGEABLE_METADATA)."""
    key: str
    metadata: dict[str, Any]


class ProjectBody(BaseModel):
    """
    A request to publish a ragnart project.

    id, name and path are the marker's fields. owner is the project's owner: it is required for a
    source with no fixed owner (nas) and must be configured in ragnart.yaml, and it may not be sent
    to a source with a fixed owner (checked by _checked_owner). tags is the marker's tag set, each
    value a string; the registry replaces the project's whole tag set with it. A request from a
    source whose project support is not built is refused before these fields are checked.
    """
    id: str
    name: str
    path: str | None = None
    owner: str | None = None
    tags: dict[str, str] = {}


class ProjectRemoveBody(BaseModel):
    """A request to remove a ragnart project. Only the source that homes the project may remove it."""
    id: str


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
    owner: str | None = Form(default=None),
    tag: str | None = Form(default=None),
    project_id: str | None = Form(default=None),
    source: dict = Depends(authenticated_source),
):
    """
    Accept a file's bytes for a filesystem source. The bytes are spooled, and ingested later.

    The file type must be an accepted include extension (422 otherwise), and is stored in canonical
    form. A text-like file above rag.admission.max_text_file_size_bytes is refused with 413 and nothing
    is spooled. The cap is checked while the upload streams, and a partial file is removed. Other
    file types have no size cap here: docling files are checked by the worker.

    Only a filesystem source sends bytes (422 for any other connector). The owner, tag and project_id
    fields are checked before anything is spooled.
    """
    # Only a filesystem source sends bytes. A claude_code or opencode source sends its sessions as text.
    if source["connector"] != CONNECTOR_FILESYSTEM:
        raise HTTPException(422, f"source {source['id']} is a {source['connector']} source: only a filesystem "
                                 "source sends bytes; send its sessions with POST /v1/upsert/text")
    _check_key(source, key)
    parsed_metadata = _parse_metadata_form(metadata)
    parsed_modified_at = _checked_modified_at(modified_at)

    state = request.app.state
    checked_owner = _checked_owner(state.config, source, owner)
    checked_project_id = _checked_project_id(state.registry, project_id)
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
            owner=checked_owner, tag=tag, project_id=checked_project_id,
        )
    except BaseException:
        # Any failure (the cap, a dropped connection, a registry error) leaves no spool file behind.
        if spool_path.exists():
            os.remove(spool_path)
        raise
    return _accepted(request, source, request_id)


@app.post("/v1/upsert/text", status_code=202)
async def upsert_text(
    request: Request,
    body: TextUpsertBody,
    source: dict = Depends(authenticated_source),
):
    """Accept an item's text inline. Only session sources (claude_code, opencode) send text: a filesystem source sends bytes."""
    if source["connector"] not in SESSION_CONNECTORS:
        raise HTTPException(422, f"source {source['id']} is a {source['connector']} source: send its files as bytes "
                                 "with POST /v1/upsert/bytes, not as text")
    _check_key(source, body.key)
    _check_upsert_metadata(body.metadata)
    parsed_modified_at = _checked_modified_at(body.modified_at)
    state = request.app.state
    checked_owner = _checked_owner(state.config, source, body.owner)
    checked_project_id = _checked_project_id(state.registry, body.project_id)
    request_id = state.registry.insert_intake(
        source["id"], "upsert", body.key, text=body.text, metadata=body.metadata,
        modified_at=parsed_modified_at,
        owner=checked_owner, tag=body.tag, project_id=checked_project_id,
    )
    return _accepted(request, source, request_id)


@app.post("/v1/delete", status_code=202)
async def delete_item(
    request: Request,
    body: DeleteBody,
    source: dict = Depends(authenticated_source),
):
    """Ask the hub to remove one item of this source. The worker deletes its points."""
    _check_key(source, body.key)
    request_id = request.app.state.registry.insert_intake(source["id"], "delete", body.key)
    return _accepted(request, source, request_id)


@app.post("/v1/change-key", status_code=202)
async def change_key(
    request: Request,
    body: ChangeKeyBody,
    source: dict = Depends(authenticated_source),
):
    """Ask the hub to rewrite one item's key, so a moved file keeps its points."""
    # A session item (claude_code or opencode) is keyed by its id. A session never moves, so the request is refused.
    if source["connector"] in SESSION_CONNECTORS:
        raise HTTPException(422, f"source {source['id']} is a {source['connector']} source: sessions are keyed by id "
                                 "and never move")
    _check_key(source, body.key)
    _check_key(source, body.new_key)
    request_id = request.app.state.registry.insert_intake(
        source["id"], "change_key", body.key, new_key=body.new_key,
    )
    return _accepted(request, source, request_id)


@app.post("/v1/change-metadata", status_code=202)
async def change_metadata(
    request: Request,
    body: ChangeMetadataBody,
    source: dict = Depends(authenticated_source),
):
    """Ask the hub to change one item's metadata: `owner` or `project_id` (CHANGEABLE_METADATA); anything else is 422."""
    _check_key(source, body.key)
    unknown = set(body.metadata) - CHANGEABLE_METADATA
    if unknown:
        raise HTTPException(422, f"metadata field(s) not changeable in this API version: {sorted(unknown)}")
    state = request.app.state
    # The metadata that is stored: a copy of the request's, with the owner replaced by its checked value.
    stored_metadata = dict(body.metadata)
    if "owner" in body.metadata:
        if not isinstance(body.metadata["owner"], str):
            raise HTTPException(422, "owner must be a string")
        # The same owner rule as an upsert: a fixed-owner source may not name an owner, and a source
        # with no fixed owner may name only a configured one. The checked value is the one stored.
        stored_metadata["owner"] = _checked_owner(state.config, source, body.metadata["owner"])
    if "project_id" in body.metadata:
        if not isinstance(body.metadata["project_id"], str):
            raise HTTPException(422, "project_id must be a string")
        _checked_project_id(state.registry, body.metadata["project_id"])
    request_id = state.registry.insert_intake(
        source["id"], "change_metadata", body.key, metadata=stored_metadata,
    )
    return _accepted(request, source, request_id)


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

    Each item carries its chunk count, and the values of ITEM_VALUE_FIELDS (content hash, pipeline
    version, owner, project_id, source_type, file_type). The points of one item should share them, so
    each value is the one the item's FIRST point carries.

    When a later point of the same item carries a different value, the first point's value stays, and
    the field's name is listed in the item's `inconsistent` list. The list is empty for an item whose
    points agree. Nothing is dropped: a caller that needs the true value of a mixed item must look at
    the points themselves.

    The hub keeps no per-item table: the items are derived from the points each time, because the
    hub keeps nothing beyond the chunks themselves (design D19).
    """
    items: dict[str, dict] = {}
    # Per item, the field names on which a point disagrees with the first point. Kept apart from the
    # item so the item's shape stays the same for every item.
    disagreeing: dict[str, set[str]] = {}
    for payload in payloads:
        key = payload[FIELD_SOURCE_PATH]
        if key not in items:
            item = {"key": key, "chunk_count": 0}
            for field in ITEM_VALUE_FIELDS:
                item[field] = payload.get(field)
            items[key] = item
            disagreeing[key] = set()
        else:
            for field in ITEM_VALUE_FIELDS:
                if payload.get(field) != items[key][field]:
                    disagreeing[key].add(field)
        items[key]["chunk_count"] += 1

    result = []
    for key, item in items.items():
        item["inconsistent"] = sorted(disagreeing[key])
        result.append(item)
    return sorted(result, key=lambda item: item["key"])


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
# Projects (session B2): the hub registry is written here, synchronously
#
# A project is not an item, so it does not go through the intake queue: the registry row and its
# tags are written when the request arrives. Two refusals, both 409, checked in this order:
#   1. D21 (one home): a project id homed on another source is refused, so one source can never
#      take over another source's project.
#   2. Project support: only the sources in SOURCES_WITH_PROJECT_SUPPORT (today nas) have project
#      support built in their ingestion; endpoint project support is v0.4. Every other source is
#      refused until then.
# ---------------------------------------------------------------------------

def _has_project_support(source_id: str) -> bool:
    """True when project support is built for the source. Source ids compare case-insensitively."""
    supported = {source.casefold() for source in SOURCES_WITH_PROJECT_SUPPORT}
    return source_id.casefold() in supported


def _project_refusal(source: dict, project_id: str, name: str, why: str) -> JSONResponse:
    """
    The 409 refusal of a project request, logged at WARNING with the source and the project.

    The body keeps the B1 shape: {'refused': True, 'reason': ...}. The client reads the refusal as
    an answer (ragnart/endpoint_client.py publish_project), not as a failure.
    """
    logger.warning(
        "refused project request: source=%s project=%s name=%s reason=%s",
        source["id"], project_id, name, why,
    )
    return JSONResponse(status_code=409, content={"refused": True, "reason": why})


def _unsupported_refusal(source: dict, project_id: str, name: str) -> JSONResponse:
    """The 409 refusal for a source whose project support is not built yet."""
    return _project_refusal(
        source, project_id, name,
        f"project records from {source['id']} are not supported yet: "
        "project support is built for the NAS ingestion only (v0.4)",
    )


def _home_refusal(source: dict, project: dict) -> JSONResponse:
    """The 409 refusal of the D21 gate: the project is homed on another source (or has no home)."""
    home = project["home_source_id"] or "no source"
    name = project["name"] or ""
    return _project_refusal(
        source, project["id"], name,
        f"project {project['id']} ({name}) is homed on {home}; a project has one home (D21)",
    )


def _project_by_id(registry: RegistryDB, project_id: str) -> dict | None:
    """Return the registry row of one project, or None. The id is compared exactly."""
    return next((project for project in registry.list_projects() if project["id"] == project_id), None)


def _homed_on_another_source(project: dict, source: dict) -> bool:
    """
    True when a project row is homed on a source other than the requesting one (the D21 gate).

    Source ids are compared case-insensitively. A project with no home (the reserved 'global' row)
    counts as homed elsewhere, so no source can take it over.
    """
    home = project["home_source_id"]
    return home is None or home.casefold() != source["id"].casefold()


@app.post("/v1/projects")
async def publish_project(
    request: Request,
    body: ProjectBody,
    source: dict = Depends(authenticated_source),
):
    """
    Publish one ragnart project from this source: write its registry row and replace its tags.

    Refused with 409 when the project id is already homed on another source (D21, checked first),
    or when project support is not built for the source. Refused with 422 when the id is the
    reserved 'global', or when the owner does not pass _checked_owner. On success the registry matches the request.
    Re-publishing the same project from the same source updates it (upsert, idempotent).
    """
    if body.id == GLOBAL_PROJECT_ID:
        raise HTTPException(422, f"project id {GLOBAL_PROJECT_ID!r} is reserved")
    registry = request.app.state.registry
    # D21 first: a project homed elsewhere is refused whatever the source's project support.
    existing = _project_by_id(registry, body.id)
    if existing is not None and _homed_on_another_source(existing, source):
        return _home_refusal(source, existing)
    if not _has_project_support(source["id"]):
        return _unsupported_refusal(source, body.id, body.name)
    checked_owner = _checked_owner(request.app.state.config, source, body.owner)

    # The project row and its tags are written in turn. Both writes are idempotent, so a failure
    # between them is repaired by the next publish of the same project.
    registry.upsert_project(body.id, body.path, body.name, checked_owner, source["id"])
    registry.replace_project_tags(body.id, body.tags)
    logger.info("published project %s from source %s", body.id, source["id"])
    return {"published": True, "id": body.id}


@app.post("/v1/projects/remove")
async def remove_project(
    request: Request,
    body: ProjectRemoveBody,
    source: dict = Depends(authenticated_source),
):
    """
    Remove one ragnart project that this source homes: its row, tags, references and grants.

    404 when no such project exists. 409 when the project is homed on another source or is the
    reserved 'global' (D21, checked first), or when project support is not built for the source. Items attached to the project are not
    touched here; the sync re-attaches them in the same run.
    """
    registry = request.app.state.registry
    existing = _project_by_id(registry, body.id)
    # D21 first, then project support, then existence.
    if existing is not None and _homed_on_another_source(existing, source):
        return _home_refusal(source, existing)
    if not _has_project_support(source["id"]):
        return _unsupported_refusal(source, body.id, "")
    if existing is None:
        raise HTTPException(404, f"no project {body.id!r} in the registry")
    registry.remove_project(body.id)
    logger.info("removed project %s from source %s", body.id, source["id"])
    return {"removed": True, "id": body.id}
