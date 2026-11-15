#!/usr/bin/env python3
"""
Test the RAG search pipeline from the command line.

Embeds the query, applies the ownership filter, and prints the top-k results
with cosine similarity scores and source paths — identical to what the
search service returns from GET /v1/search.

Usage (run from project root):
    python utils/rag_search_cli.py --query "what certifications do I have" --user florian
    python utils/rag_search_cli.py --query "family album" --user la-familia --top-k 10
    python utils/rag_search_cli.py --query "test" --no-filter   # search without owner filter
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml  # noqa: E402

from ragnart.collection import get_client
from ragnart.search import resolve_max_hits_per_source, search_rag


async def run(query: str, user_id: str | None, top_k: int, qdrant_url: str, rag_cfg: dict, file_types: list[str] | None = None, score_threshold: float = 0.0, rerank: bool | None = None, max_hits_per_source: int | None = None) -> None:
    qdrant_client = get_client(qdrant_url)

    results = await search_rag(
        query=query,
        user_id=user_id,
        limit=top_k,
        qdrant_client=qdrant_client,
        rag_cfg=rag_cfg,
        file_types=file_types,
        score_threshold=score_threshold,
        rerank=rerank,
        max_hits_per_source=max_hits_per_source,
    )

    if not results:
        print("No results found.")
        return

    print(f"\n{len(results)} result(s) for query: {query!r}  (user_id={user_id})\n")

    # Check if any result has a rerank_score field
    has_rerank = any("rerank_score" in r for r in results)

    if has_rerank:
        print(f"{'#':<3}  {'Score':>6}  {'Rerank':>7}  {'File/Title':<40}  Source")
        print("─" * 110)
        for i, r in enumerate(results, 1):
            ftype  = r.get("file_type", "")
            stitle = r.get("session_title", "")
            display = (stitle if ftype in ("claude_chat", "opencode") and stitle else r.get("file_name", ""))[:38]
            rr = r.get("rerank_score", 0.0)
            print(f"{i:<3}  {r['score']:>6.4f}  {rr:>+7.2f}  {display:<40}  {r['source_path']}")
    else:
        print(f"{'#':<3}  {'Score':>6}  {'File/Title':<40}  Source")
        print("─" * 100)
        for i, r in enumerate(results, 1):
            ftype  = r.get("file_type", "")
            stitle = r.get("session_title", "")
            display = (stitle if ftype in ("claude_chat", "opencode") and stitle else r.get("file_name", ""))[:38]
            print(f"{i:<3}  {r['score']:>6.4f}  {display:<40}  {r['source_path']}")

    print()
    if results:
        print("── Top result content (parent_text) ─────────────────────────────────────")
        print(results[0]["content"][:800])
        if len(results[0]["content"]) > 800:
            print(f"  ... [{len(results[0]['content'])} chars total]")


def positive_int(value: str) -> int:
    """argparse type: accept only integers >= 1 (matches the service's ge=1 on max_hits_per_source)."""
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"must be an integer, got {value!r}")
    if number < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {number}")
    return number


def main() -> None:
    parser = argparse.ArgumentParser(description="Test RAG search from the command line")
    parser.add_argument("--query", required=True, help="Search query")
    parser.add_argument("--user", metavar="OWNER", default=None,
                        help="Owner filter, e.g. florian (required unless --no-filter)")
    parser.add_argument("--no-filter", action="store_true",
                        help="Search all documents regardless of owner")
    parser.add_argument("--top-k", type=int, default=5, help="Number of results (default: 5)")
    parser.add_argument("--file-types", nargs="+", metavar="TYPE",
                        help="Filter by file type(s): pdf, pptx, ppt, docx, ipynb, md, yaml, txt, claude_chat")
    parser.add_argument("--score-threshold", type=float, default=0.0, metavar="FLOAT",
                        help="Minimum cosine score to include (0=no filter, e.g. 0.50)")
    parser.add_argument("--no-rerank", action="store_true",
                        help="Disable cross-encoder reranking (use Qdrant cosine order only)")
    parser.add_argument("--max-hits-per-source", type=positive_int, default=None, metavar="N",
                        help="Per-source cap on results (default: ragnart.yaml rag.search.max_hits_per_source)")
    args = parser.parse_args()

    if not args.no_filter and not args.user:
        parser.error("Provide --user OWNER or --no-filter")

    user_id = None if args.no_filter else args.user

    # Config lives at the repo root (ragnart.yaml), next to this utils/ directory.
    with open(Path(__file__).resolve().parent.parent / "ragnart.yaml") as f:
        config = yaml.safe_load(f)

    rag_cfg = config.get("rag", {})

    rerank = False if args.no_rerank else None

    # Resolve the cap here with the same helper search_rag() uses, so the value is
    # the one actually applied (CLI flag if given, otherwise ragnart.yaml).
    max_hits = resolve_max_hits_per_source(args.max_hits_per_source, rag_cfg)

    asyncio.run(run(args.query, user_id, args.top_k, rag_cfg.get("qdrant_url", "http://192.168.1.93:6333"), rag_cfg, file_types=args.file_types or None, score_threshold=args.score_threshold, rerank=rerank, max_hits_per_source=max_hits))


if __name__ == "__main__":
    main()
