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

import yaml
from fastapi import FastAPI, HTTPException, Query

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

with open(_CONFIG_PATH) as f:
    _config = yaml.safe_load(f)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting RAGNART search service...")

    app.state.rag_cfg = _config.get("rag", {})
    _oc_cfg = _config.get("opencode", {})
    app.state.rag_cfg["opencode_api_url"] = _oc_cfg.get("api_url", "http://localhost:4096")
    app.state.rag_cfg["opencode_password_env_file"] = _oc_cfg.get("password_env_file")

    try:
        qdrant_url = app.state.rag_cfg.get("qdrant_url", "http://192.168.1.93:6333")
        app.state.qdrant_client = get_client(qdrant_url)
        ensure_collection(app.state.qdrant_client)
        logger.info("Qdrant connected and collection ready ✓")
    except Exception as exc:
        logger.warning("Qdrant unavailable — search disabled: %s", exc)
        app.state.qdrant_client = None

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
    docs/Implementation-plan.md §6 v0.1)."""
    if app.state.qdrant_client is None:
        raise HTTPException(status_code=503, detail="Qdrant client not available")

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

    logger.info(
        "RAG search: q=%r user=%s top_k=%d score_threshold=%.2f rerank=%s → %d result(s)",
        q, user, top_k, score_threshold, rerank, len(results),
    )
    return {"query": q, "user": user, "results": results}


@app.get("/health")
async def health():
    """Liveness probe; reports whether Qdrant is reachable."""
    qdrant_ok = False
    if app.state.qdrant_client is not None:
        try:
            app.state.qdrant_client.get_collections()
            qdrant_ok = True
        except Exception:
            qdrant_ok = False
    return {"status": "ok" if qdrant_ok else "degraded", "qdrant": qdrant_ok}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8100)
