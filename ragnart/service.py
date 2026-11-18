"""
RAGNART — search service (Server 2, 192.168.1.95:8100)

Stateless FastAPI service exposing RAG retrieval over Qdrant.
Moved out of SoHoAI's main.py (`/v1/rag/search`) on 2026-11-13 — see
docs/v0--Implementation-log.md, v0.1 -- Spin-off.

Run (systemd — see scripts/ragnart-search.service):
    uvicorn ragnart.service:app --host 0.0.0.0 --port 8100
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import yaml
from fastapi import FastAPI, HTTPException, Query
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

from ragnart import search_rag
from ragnart.cli import FatalError, open_queue_read_only, open_registry_read_only
from ragnart.collection import ensure_collection, get_client
from ragnart.registry import RegistryDB
from ragnart.schema import GLOBAL_PROJECT_ID
from ragnart.scope import ScopeError, resolve_scope
from ragnart.search import resolve_max_hits_per_source
from ragnart.state import StateDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("ragnart")

# Config path is relative to the repo root (this file's parent's parent),
# never to the process's current working directory — so the systemd
# unit's WorkingDirectory is not load-bearing for config discovery.
_CONFIG_PATH = Path(__file__).resolve().parent.parent / "ragnart.yaml"

# Used only when config omits rag.qdrant_url.
_DEFAULT_QDRANT_URL = "http://192.168.1.93:6333"

# /health probes Qdrant's plain /healthz endpoint with this short timeout, so a
# dead Qdrant makes /health answer quickly instead of hanging for the 60 s
# client timeout used by the qdrant_client calls.
_HEALTH_TIMEOUT_S = 2.0

# The owner value that every search also includes (shared La-Familia content).
_SHARED_OWNER = "la-familia"

with open(_CONFIG_PATH) as f:
    _config = yaml.safe_load(f)


def _open_state_db() -> StateDB | None:
    """
    Open the queue database read-only for scope resolution and /v1/projects.

    A missing or unreadable database is logged loudly and returns None, so the
    service still starts (the same tolerance as Qdrant). The scope-dependent
    endpoints then answer 503 instead of guessing a scope.
    """
    try:
        return open_queue_read_only(_config)
    except FatalError as exc:
        logger.warning("Queue database unavailable at startup — scope endpoints will answer 503: %s", exc)
        return None


def _require_state_db() -> StateDB:
    """Return the startup-opened StateDB, or answer 503 when it could not be opened."""
    state_db = app.state.state_db
    if state_db is None:
        raise HTTPException(status_code=503, detail="Queue database unavailable")
    return state_db


def _open_registry_db() -> RegistryDB | None:
    """
    Open the hub registry read-only for scope resolution and /v1/projects.

    A missing or unreadable registry is logged loudly and returns None, the same
    tolerance as the queue database. The endpoints that need it then answer 503.
    """
    try:
        return open_registry_read_only(_config)
    except FatalError as exc:
        logger.warning("Hub registry unavailable at startup — scope endpoints will answer 503: %s", exc)
        return None


def _require_registry_db() -> RegistryDB:
    """Return the startup-opened RegistryDB, or answer 503 when it could not be opened."""
    registry_db = app.state.registry_db
    if registry_db is None:
        raise HTTPException(status_code=503, detail="Hub registry unavailable")
    return registry_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting RAGNART search service...")

    app.state.rag_cfg = _config.get("rag", {})
    _oc_cfg = _config.get("opencode", {})
    app.state.rag_cfg["opencode_api_url"] = _oc_cfg.get("api_url", "http://localhost:4096")
    app.state.rag_cfg["opencode_password_env_file"] = _oc_cfg.get("password_env_file")

    # Client construction does not contact Qdrant, so the client is always
    # created. Search and /health can then use it even if Qdrant is down now.
    qdrant_url = app.state.rag_cfg.get("qdrant_url", _DEFAULT_QDRANT_URL)
    app.state.qdrant_client = get_client(qdrant_url)

    # Startup must tolerate Qdrant being down, so a failure here is only a warning.
    try:
        ensure_collection(app.state.qdrant_client)
        logger.info("Qdrant collection ready ✓")
    except Exception as exc:
        logger.warning("Qdrant unavailable at startup — collection not verified: %s", exc)

    # One read-only StateDB for the whole process, opened here with the same
    # lifecycle as qdrant_client. Reads are cheap local SQLite queries, so no
    # per-request connection is made. Every handler runs on the event loop
    # thread, which is why check_same_thread=False is safe. The connection is
    # read-only and holds no write lock. In WAL mode each read statement sees
    # the latest committed data, so changes the sync process commits show up
    # on the next request.
    app.state.state_db = _open_state_db()
    # The hub registry (projects, tags, sources) is opened with the same lifecycle.
    app.state.registry_db = _open_registry_db()

    yield

    if app.state.state_db is not None:
        app.state.state_db.close()
    if app.state.registry_db is not None:
        app.state.registry_db.close()
    logger.info("RAGNART search service shut down cleanly.")


app = FastAPI(
    title="RAGNART",
    description="RAG retrieval service — Archive tier search (v0.1)",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/v1/search")
async def search(
    q: str,
    # Required on purpose: there is no server-side default. A missing scope is
    # answered with HTTP 422 by FastAPI before this handler runs. Repeated values
    # are unioned (see ragnart/scope.py).
    scope: list[str] = Query(...,
                             description=(
                                 "Required. 'all', '<source>:<path>', '<source>:<path>/', "
                                 "or 'tag:<key>=<value>'; repeatable"
                             )),
    user: str | None = Query(None),
    top_k: int = Query(5, ge=1, le=100),
    file_types: list[str] | None = Query(None),
    score_threshold: float = Query(0.0, ge=0.0, le=1.0,
                                   description="Minimum cosine score; 0=no filter"),
    rerank: bool | None = Query(None,
                                description="Enable cross-encoder reranking; None=config default"),
    max_hits_per_source: int | None = Query(None, ge=1,
                                            description="Per-source cap on this search's results; None=config default"),
):
    """Retrieve RAG document hits without invoking any LLM. Moved from
    SoHoAI's /v1/rag/search (main.py) on 2026-11-13; same params and
    response shape, minus the multi-query variant (dropped — see
    docs/Implementation-plan.md §2)."""
    # Resolve the cap here, not only inside search_rag(), so the value echoed in the
    # response is exactly the value that was applied.
    effective_cap = resolve_max_hits_per_source(max_hits_per_source, app.state.rag_cfg)

    # Resolve the scope before any embedding or Qdrant call, so a bad scope fails
    # fast. The echo says exactly which projects were searched.
    try:
        project_filter, scope_echo = resolve_scope(scope, _require_registry_db())
    except ScopeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        results = await search_rag(
            query=q,
            user_id=user,
            limit=top_k,
            qdrant_client=app.state.qdrant_client,
            rag_cfg=app.state.rag_cfg,
            file_types=file_types,
            score_threshold=score_threshold,
            rerank=rerank,
            max_hits_per_source=effective_cap,
            project_filter=project_filter,
        )
    except ResponseHandlingException as exc:
        # qdrant_client wraps every transport failure raised by its HTTP layer
        # (connection refused, DNS, timeout) in this class. Only Qdrant calls go
        # through that layer, so a failure in the embedder or reranker, which use
        # plain httpx, is not mistaken for a Qdrant outage here.
        logger.warning("Qdrant unreachable during search: %s", exc)
        raise HTTPException(status_code=503, detail="Qdrant unavailable") from exc
    except UnexpectedResponse as exc:
        # Qdrant answered with a 5xx: it is up but unable to serve the request.
        # A 4xx is a request problem, so it is not mapped to 503.
        if exc.status_code is not None and exc.status_code >= 500:
            logger.warning("Qdrant answered HTTP %s during search: %s", exc.status_code, exc)
            raise HTTPException(status_code=503, detail="Qdrant unavailable") from exc
        raise

    logger.info(
        "RAG search: q=%r user=%s top_k=%d score_threshold=%.2f rerank=%s → %d result(s)",
        q, user, top_k, score_threshold, rerank, len(results),
    )
    return {
        "query": q,
        "user": user,
        "scope": scope_echo,
        "max_hits_per_source": effective_cap,
        "results": results,
    }


@app.get("/v1/projects")
async def projects(user: str | None = Query(None)):
    """
    List the RAGNART projects plus the reserved `global` row.

    Each entry has id, path, name, owner, source_id (the project's home source), tags
    and item_count. The projects and tags come from the hub registry; item_count is
    counted from the queue database. Both must be open, or the endpoint answers 503.
    `global` has no path and is always listed. Other projects are listed only
    when their owner is `user` or `la-familia`; with no `user`, every project is
    listed. Order: `global` first, then by path.
    """
    registry_db = _require_registry_db()
    state_db = _require_state_db()
    item_counts = state_db.item_counts_by_project()

    visible = [
        p for p in registry_db.list_projects()
        if user is None
        or p["id"] == GLOBAL_PROJECT_ID
        or p["owner"] in (user, _SHARED_OWNER)
    ]
    visible.sort(key=lambda p: (p["id"] != GLOBAL_PROJECT_ID, p["path"] or ""))

    return {
        "projects": [
            {
                "id": p["id"],
                "path": p["path"],
                "name": p["name"],
                "owner": p["owner"],
                "source_id": p["home_source_id"],
                "tags": registry_db.project_tags(p["id"]),
                "item_count": item_counts.get(p["id"], 0),
            }
            for p in visible
        ]
    }


def _qdrant_healthy(qdrant_url: str) -> bool:
    """Return True only if Qdrant's /healthz answers HTTP 200 within the timeout.

    Any exception (connection refused, timeout, DNS failure) counts as unhealthy.
    """
    try:
        response = httpx.get(f"{qdrant_url.rstrip('/')}/healthz", timeout=_HEALTH_TIMEOUT_S)
    except Exception:
        return False
    return response.status_code == 200


# Plain `def` on purpose: FastAPI runs it in a worker thread, so the blocking
# httpx call (bounded by _HEALTH_TIMEOUT_S) does not stall the event loop.
@app.get("/health")
def health():
    """Liveness probe; reports whether Qdrant's /healthz answers within 2 s."""
    qdrant_url = app.state.rag_cfg.get("qdrant_url", _DEFAULT_QDRANT_URL)
    qdrant_ok = _qdrant_healthy(qdrant_url)
    return {"status": "ok" if qdrant_ok else "degraded", "qdrant": qdrant_ok}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8100)
