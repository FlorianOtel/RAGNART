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
from ragnart.collection import ensure_collection, get_client

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

with open(_CONFIG_PATH) as f:
    _config = yaml.safe_load(f)


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

    yield

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
    user: str | None = Query(None),
    top_k: int = Query(5, ge=1, le=100),
    file_types: list[str] | None = Query(None),
    score_threshold: float = Query(0.0, ge=0.0, le=1.0,
                                   description="Minimum cosine score; 0=no filter"),
    rerank: bool | None = Query(None,
                                description="Enable cross-encoder reranking; None=config default"),
):
    """Retrieve RAG document hits without invoking any LLM. Moved from
    SoHoAI's /v1/rag/search (main.py) on 2026-11-13; same params and
    response shape, minus the multi-query variant (dropped — see
    docs/Implementation-plan.md §2)."""
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
    return {"query": q, "user": user, "results": results}


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
