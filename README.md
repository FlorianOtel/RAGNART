<p align="center">
  <img src="assets/ragnart-avatar.png" alt="RAGNART — a Viking painter with a paint-dipped sword" width="240">
</p>

<h1 align="center">RAGNART</h1>

<p align="center">
  <b>RAG with Nested Archive, Runtime, and Transient tiers</b><br>
  A self-hosted retrieval service over a home NAS, AI-assistant chat history, and live project notes.
</p>

---

## What it is

RAGNART is a self-hosted Retrieval-Augmented Generation (RAG) **retrieval
service**. It indexes everything worth remembering on a home/small-office
setup (documents on a NAS, the transcripts of Claude Code and OpenCode
sessions, and scratch material captured while working on a project) and
serves it over a small HTTP API. Any LLM client, such as Claude Code, an
editor plugin, a script or a notebook UI, can call that API to ground its
answers in your own material.

RAGNART does **retrieval only**. It returns ranked text chunks with their
sources and leaves the generation step to the LLM client that called it.

It started as the RAG subsystem of [SoHoAI](https://github.com/FlorianOtel/SoHoAI)
and was spun off into its own repository and service in November 2026.

## The three tiers

The name spells out the design: one index, **nested** by project, holding
three tiers of material with different lifetimes.

| Tier | What it holds | How it gets in | Lifetime |
| --- | --- | --- | --- |
| **Archive** | Files on the NAS: PDF, DOCX, PPTX, Markdown, text, YAML, notebooks, HTML, shell scripts | Periodic incremental scan (systemd timer) | Long-lived; follows the files on disk |
| **Runtime** | AI-assistant sessions: Claude Code chat logs (`.jsonl`) and OpenCode sessions (through its HTTP API) | The same periodic ingestion | Long-lived; one record per session |
| **Transient** | Material in flight for the current project: pages the assistant fetched, captured into the project's `inbox/`. A working cache of files, read directly by the assistant; not indexed | Captured as you work, by the assistant skill | Ephemeral: expires after 30 days unless you **keep** it (it then becomes a project resource, which the Archive indexes) or **drop** it |

**Nested** means every chunk belongs to a project. A ragnart project is a
notebook: a folder created by `ragnart init <name>`, wherever you like, whose
`resources/` and `collateral/` hold the material kept for one purpose. It
includes assistant sessions and other ragnart projects by reference, and its
`.ragnart/` holds only its state, as `.git/` does. Everything else belongs to the
shared `global` project. A search can be scoped to one ragnart project, with the
ragnart projects and sessions it references, or run across everything you are
allowed to see.

> **Status:** Archive and Runtime exist today, with ragnart projects, nesting and scoped
> search. The Transient tier, a notebook UI and real authentication are planned. See [Roadmap](#roadmap).

## How it works

```
                  ┌───────────────────────── ingestion (systemd timer) ─────────────────────────┐
 NAS files ──────▶│ scanner ─▶ docling parse ─▶ parent/child chunking ─▶ bge-m3 dense embeddings │
 Claude Code logs │            (PDF/DOCX/PPTX/…)   (tiktoken)              + BM25 sparse vectors   │──▶ Qdrant
 OpenCode API ───▶│ state DB (SQLite): queue, retries, crash recovery, deletions               │
                  └──────────────────────────────────────────────────────────────────────────────┘

 LLM client ──GET /v1/search──▶ RAGNART service ──▶ hybrid search in Qdrant (dense + sparse, RRF fusion)
            ◀── ranked chunks ──                 ──▶ cross-encoder rerank (bge-reranker-v2-m3)
```

**Ingestion.** A scanner walks the configured NAS roots and assistant
transcripts, honouring include and exclude lists. A SQLite state database
tracks every source: what is queued, done or failed, and what disappeared.
New and changed files are parsed with [Docling](https://github.com/docling-project/docling)
and split into *parent/child* chunks. Small child chunks (~200–300 tokens)
are embedded for precise matching. The larger parent chunk (~800–1200
tokens) is what gets returned, so the LLM gets enough surrounding context.
Each chunk gets two vectors: a dense **bge-m3** embedding (served by Ollama)
and a sparse **BM25** vector (fastembed). Ingestion is idempotent: a changed
file is deleted, then re-inserted. It survives crashes and retries failed
files, and it is guarded by a cross-host lock on NFS so only one run is
active at a time.

**Retrieval.** `GET /v1/search` runs a hybrid query in
[Qdrant](https://qdrant.tech/): dense and sparse candidates are fused with
Reciprocal Rank Fusion. The pool is then reordered by a **cross-encoder
reranker** (bge-reranker-v2-m3). Results can be filtered by owner and file
type. Each hit carries its source path, scores and parent text.

**Ownership.** Every chunk records an owner (a person, or a shared
"family" space). A search with `user=<owner>` is limited to that owner plus
the shared space; without `user` it covers everything. There is no
authentication yet: the `user` value is trusted as given. A logical
access-control model (owners, projects and grants, checked in one place) is
planned, so multi-tenancy does not depend on filesystem permissions.

## Repository layout

| Path | What's there |
| --- | --- |
| `ragnart/` | The Python package: scanner, chunking/ingestion, embeddings, Qdrant collection, hybrid search, reranking, state DB, OpenCode client, and the FastAPI service (`ragnart/service.py`) |
| `ragnart.yaml` | All configuration: model and service URLs, scanner include rules, admission caps, sync guard, rerank and hybrid parameters, users and shared spaces |
| `config/ragignore` | Global exclusion rules for the NAS crawl (gitignore syntax), applied under every root; per-directory `.ragignore` files add to them |
| `utils/` | Operator tools: ingestion daemon, NFS sync, search CLI, status, corpus maintenance (purge, reset, sparse-vector migration), smoke and benchmark scripts, endpoint comparison |
| `scripts/` | systemd units (`ragnart-search.service`, `ragnart-ingest.{service,timer}`), the ingestion wrapper, Qdrant + SQLite snapshot script, logrotate config, install script; `scripts/qdrant/` holds Qdrant's config, unit and installer for Server 1 |
| `docs/` | Design and operations docs (see below) |
| `assets/` | Logo / avatar images |

## Running it

Requirements: Python 3.12, a reachable Qdrant, an Ollama instance serving
`bge-m3`, and (optionally) a `/v1/rerank` endpoint (e.g. llama-server behind
llama-swap) for the reranker. Endpoints are set in `ragnart.yaml`.

```bash
# from the repo root, in a Python 3.12 env with the dependencies from pyproject.toml
uvicorn ragnart.service:app --host 0.0.0.0 --port 8100    # start the search service

curl 'http://localhost:8100/v1/search?q=how+do+snapshots+work&top_k=5&scope=all'
curl 'http://localhost:8100/health'
```

`GET /v1/search` parameters: `q` (query), `scope` (required, no default: `all`, a ragnart project
path, `<path>/` for that project and every project whose folder lies below it, or `tag:<key>=<value>`;
repeatable), `user`
(owner scope), `top_k`, `file_types`, `score_threshold`, `rerank`, `max_hits_per_source` (per-source
cap on this search's results). `GET /v1/projects` lists the ragnart projects and `global`.

The `ragnart` command reads the NAS crawl and changes nothing. It is installed by
`pip install -e .` in the project venv; `python -m ragnart` runs the same commands without
the install.

```bash
ragnart status --ignored                  # queue and Qdrant counts; ignored rows with their reason
ragnart ls-files [--ignored|--others|--stage] [<path>...]   # indexed files, or files excluded or not yet queued; optionally under paths
ragnart check-ignore -v <path>            # the .ragignore or config/ragignore line that excludes a path
```

For a permanent install, `sudo scripts/install-units.sh` installs the
search service and the ingestion timer as systemd units, plus the log
rotation config. Qdrant has its own installer, `scripts/qdrant/install-qdrant.sh`.

## Roadmap

| Version | Scope |
| --- | --- |
| **v0.1 — Spin-off** (done) | Standalone repo, search service and ingestion timer |
| **v0.2 — Nested** (done) | Project tree, data model and access model in the state DB and Qdrant payload (payload-only backfill, no re-embedding); scoped search |
| **v0.3 — Sources** | Multi-point ingestion: connectors and sources, an index at the hub, fed by ingestion hosts through an ingestion API, a hub registry of sources and projects, source-qualified search scope |
| **v0.4 — Project mode** | `ragnart` CLI (`init`, `capture`, `add`, `add --no-commit`, `commit`, `drop`, `rm`, …), the `/ragnart` assistant skill with scope and grounding, capture into the Transient cache, project resources and collateral, references between ragnart projects |
| **v0.5 — Kotaemon** | Browser notebook UI via a [Kotaemon](https://github.com/Cinnamon/kotaemon) index plugin: project picker as scope, cited answers, uploads as resources |
| **v0.6 — Tenancy & auth** | Authentication (per-client tokens, optional OAuth with group claims), enforced grants, grant management |

Details: [`docs/Implementation-plan.md`](docs/Implementation-plan.md); design: [`docs/RAGNART-design.md`](docs/RAGNART-design.md).

## Documentation

- [`docs/RAGNART-design.md`](docs/RAGNART-design.md): the design: data model, pipeline, access model, tiers, scope and grounding, open issues
- [`docs/Implementation-plan.md`](docs/Implementation-plan.md): versioned plan (v0.1 to v0.6), decisions and invariants
- [`docs/RAG-strategy.md`](docs/RAG-strategy.md): design and operation of the RAG pipeline (corpus, chunking, hybrid search, reranking, ingestion, backup and recovery)
- [`docs/RAG-troubleshoot.md`](docs/RAG-troubleshoot.md): troubleshooting guide (symptom, check, fix)
- [`docs/v0--Implementation-log.md`](docs/v0--Implementation-log.md): session log for the v0.x versions
- [`docs/design-history.md`](docs/design-history.md): design decisions over time

## Not to be confused with

A few open-source RAG projects have similar names. They are unrelated to
RAGNART:

- **[ragnar](https://ragnar.tidyverse.org/)**, an R package from the
  tidyverse team for building RAG workflows in R (Markdown conversion,
  chunking, DuckDB store, vector + BM25 retrieval), designed to work with
  `ellmer`. On [CRAN](https://cran.r-project.org/package=ragnar).
- **[Ragna](https://github.com/Quansight/ragna)**, an open-source RAG
  orchestration framework in Python from Quansight, with a Python API, a
  FastAPI REST API and a Panel web UI.

RAGNART is neither a library nor a framework. It is one opinionated,
self-hosted service for one household/small-office setup.

## License

Apache License 2.0. See [LICENSE](LICENSE).
