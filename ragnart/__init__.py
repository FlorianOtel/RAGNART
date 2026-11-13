"""
ragnart — public surface.

Exports the retrieval function used by the search service (ragnart.service)
and the CLI utilities. Multi-query expansion was dropped in the v0.1 spin-off
(see docs/Implementation-plan.md §2).
"""
from .search import search_rag

__all__ = ["search_rag"]
