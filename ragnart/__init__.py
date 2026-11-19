"""
ragnart — package root. Deliberately exports nothing.

Importing any ragnart module runs this file first. It used to import ragnart.search,
which loads qdrant_client and the embedder, so the ingestion side (which also runs on
endpoint hosts without the hub stack) could not be imported without them. Callers import
from the module that defines a name, e.g. `from ragnart.search import search_rag`.
Multi-query expansion was dropped in the v0.1 spin-off
(see docs/Implementation-plan.md §2).
"""
