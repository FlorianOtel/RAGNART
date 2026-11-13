---
title: "RAGNART Design History"
created_at: 2026-11-13--14-03
created_by: Claude Code (Claude Opus 5.5, /brain pipeline)
context: >
  RAG-relevant entries carried over verbatim from SoHoAI's docs/design-history.md
  on spin-off (2026-11-13), plus RAGNART's own entries from v0.1 onward.
---

# RAGNART Design History

---

## 2026-11-13 — v0.1 Spin-off: RAG moved from SoHoAI into RAGNART

Session log: `docs/v0--Implementation-log.md`, v0.1 -- Spin-off. Plan: `docs/Implementation-plan.md` §6 v0.1.

Commits: RAGNART `e4de8a3` (plain import of SoHoAI@5f4f1ed), `1d702fe` (adaptation),
`796635f` (old-vs-new comparison tool; pins aligned to the venv). SoHoAI `95d5d0d` (removal, 307 stub).

### What changed

- `rag_engine/` renamed to the `ragnart` package. `multi_query` and `rag_mmr_bench.py` dropped, not ported.
- Config split into `ragnart.yaml`. Search served by `ragnart.service:app` on Server 2, port 8100 (`GET /v1/search`, `GET /health`).
- Ingestion now runs from RAGNART (`ragnart-ingest.timer`). SoHoAI's `rag-ingest.timer` is disabled.
- Stale `ollama_url` fallbacks (`.93`) corrected to `.95` in `embeddings.py`, `search.py`, `ingest.py`.
- Snapshot script: download `--max-time` 3600 → 7200, failure cleanup trap. 03:00 cron entry removed. Six orphaned snapshot artefacts deleted.

### Decisions

- Old-vs-new equivalence is tie-aware, not identical ordering. Search is non-deterministic on the old endpoint alone (Qdrant RRF exact-score ties; reranker batch jitter). Operator-approved gate.
- SoHoAI `/v1/rag/search` stays a 307 redirect stub until 2026-11-16. Before removal, check `SoHoAI-gateway.log` for `GET /v1/rag/search ... 307` hits and repoint any caller found.

### Open

- Production search has no stable tie-break; candidate fix for v0.2+ (changes behaviour, so out of v0.1 scope).

---

## Carried over from SoHoAI (verbatim, as of SoHoAI@5f4f1ed)

## 2026-11-04 — RAG: OpenCode 2.x ingestion, venv exclusions, orchestrator ingest endpoints removed

Code: `e3e0a06`. Incident detail: `docs/RAG-troubleshoot.md` (2026-11-04 entry). Design:
`docs/RAG-strategy.md` §1.2, §4.6, §10.4, §10.6.

### What changed

- **OpenCode 2.x support.** New `rag_engine/opencode_client.py`: HTTP Basic auth, password
  from the OpenCode server's own env file (`opencode.password_env_file`), and a JSON
  content-type check. `scan_opencode_sessions()` authenticates and logs 401/403 at ERROR.
  `_parse_opencode_session()` rewritten for `/api/session/{id}` and a cursor walk of
  `/api/session/{id}/message?order=asc` with the 2.x message shape; errors now raise (item
  `failed`, retried) instead of returning empty.
- **Scan-failure guard fixed.** `utils/rag_sync_nfs.py` adds the queued `opencode://` rows
  (`StateDB.paths_with_prefix`) to the existing set when OpenCode cannot be scanned.
- **Scanner exclusions.** `.Gin-AI-python-3.12.old/`, `.uv-python/`, `site-packages/`, `.cache/`.
- **Orchestrator.** `/v1/rag/ingest/{sync,start,stop,status}` and the startup `StateDB`
  removed from `main.py`; search is unchanged.

### Why

- The OpenCode server became 2.x on 2026-10-14. The scanner got 401s, the parser's 1.x routes
  return HTML with status 200 on 2.x, and the "unreachable" guard deleted every opencode point
  on the first 401 run (445 sessions) while logging that they were preserved.
- The venv rebuild on the NFS interpreter (2026-11-01) left `.Gin-AI-python-3.12.old/`, which
  the name-based exclude did not match: 25,691 points of library code in the index.
- `rag_state.db` is SQLite in WAL mode on NFS; WAL needs every connection on one host. The
  orchestrator on Server 1 held it while Server 2 ingested, and a reload on Server 1 during an
  ingest produced "database disk image is malformed". No caller of the endpoints was found.
  Decision: remove the endpoints (option C) rather than open the DB per request (option A,
  which leaves the hazard reachable) or move to the rollback journal (option B, changes every
  writer; not needed under the single-host rule).

### Verified (2026-11-04)

- Re-ingest: 441 OpenCode sessions discovered, 441 completed (6 empty), 15,660 points; 480
  files processed, 0 failed. RAG search finds the "v2.1.2 acceptance" session and the AYA
  v2-D47 decision.
- `.old` venv: 25,691 points and 819 queue rows removed; directory removed after a reference
  check on both servers. Qdrant facet: 0 `.old`, `.uv-python`, `.cache`, `site-packages` paths.
- Orchestrator: single worker after reload, 0 file handles on `rag_state.db`, `/health` ok,
  `/v1/rag/search` 200, the four ingest endpoints 404.

### Accepted limitations / not tested

- The single-host rule is a convention, not enforced: a state-DB tool run on Server 1 during
  an ingest can still hit the WAL hazard.
- The OpenCode password file exists on Server 2 only; the scanner on any other host logs an
  ERROR and preserves existing points.
- `uvicorn --reload` on Server 1 does not reliably notice edits made from Server 2 over NFS;
  touch the file on Server 1 to force it.
- A run that logs WARNING/ERROR still ends "complete"; nothing yet counts sources per run.

---

## 2026-05-27 — Opencode RAG ingestion, single-slot llama-server, chat title rendering

Three related changes from one session, committed as `e1af344`, `306a99e`, `12df6a2`.

### Opencode session ingestion (commit `e1af344`)

OpenCode (the agent used by octmux) joins Claude Code as a second external LLM agent whose sessions get ingested into the `documents` Qdrant collection — same `session_title` + content treatment, queryable via `GET /v1/rag/search?file_types=opencode`.

Architectural shape diverges from Claude Code: OpenCode has no on-disk session files, so the scanner queries its HTTP API (`/project`, `/session?directory=...`) instead of walking NFS. Sessions are referenced via synthetic `opencode://{session_id}` keys (kept in `ingestion_queue.file_path` and Qdrant `source_path`); the parser then fetches `/session/{id}/message` to extract text-only parts (skipping file/tool/reasoning/patch/snapshot parts).

Critical invariant: if the API is unreachable, `scan_opencode_sessions()` returns `existing_paths=None` (not an empty set). Callers treat `None` as "skip this source in `find_deleted()`" so existing opencode Qdrant points are preserved instead of mass-purged when the opencode server happens to be down at sync time.

Two config keys must both point at the live opencode server (currently `http://192.168.1.95:4096`, not `localhost`):
- `opencode.api_url` — scanner uses this
- `rag.opencode_api_url` — parser uses this (injected at startup in `lifespan()` and at daemon entry in `rag_ingest_daemon.py`)

Initial run after activation: 30 opencode sessions discovered, 5 ingested as smoke test, remainder queued for the systemd-driven Server 2 ingestion service.

### llama-server: single-slot 262144 ctx (commit `306a99e`)

Qwen3.5-4B llama-server (and the llama-swap config that fronts it) had been running `--parallel 2 --ctx-size 262144`, giving two slots × 131072 ctx. KV cache slot session pinning is not yet implemented — a chat coming back into the conversation cache can land on whichever slot the scheduler picks, and miss its saved KV state. Stopgap: drop `--parallel 2` entirely, run a single slot at the full 262144 ctx. Proxy-advertised `max_tokens` / `max_input_tokens` / `context_window` bumped to 262144 to match what external clients (Cline, Claude Code) now see via `/proxy/v1/model/info`. Documented in CLAUDE.md's runbook command and RAG-troubleshoot.md's expected `ps aux` flags.

### Chat sessions render by title, not by UUID (commit `12df6a2`)

Search results for opencode hits were displaying the raw session id (`ses_1b5c9eed...`) in the File/Title column instead of the friendly title — the `utils/rag_search_cli.py` and `~/.claude/commands/rag.md` display paths special-cased `file_type == "claude_chat"` to swap in `session_title`, and opencode was omitted. Same root-cause as the Claude Code session_title work (2026-05-01): `file_name = Path(file_path).name` produces a meaningless identifier for chat sources (`UUID.jsonl` for CC, `ses_<id>` for opencode), so renderers needed special-casing to swap in `session_title`.

Root-cause fix in `rag_engine/ingest.py`: when a parser returns `chat_meta[FIELD_SESSION_TITLE]`, override `file_name` with it before the Qdrant upsert. Both chat types now store the human-readable title in `file_name` natively. The display fallback in `rag_search_cli.py` was also extended to recognise `file_type=opencode` alongside `claude_chat` as a belt-and-braces measure — covers ~16.8k pre-existing claude_chat points and 5 pre-existing opencode points whose stored `file_name` still holds the raw identifier (they pick up the new label on re-ingest).

---

---

## 2026-04-21 — Phase 2 RAG + Advanced features complete

### What was implemented

Initial ingestion run produced **2891 files completed, 0 pending, 0 ignored**, yielding **98,737 Qdrant points** in the `documents` collection (avg ~34 chunks/file). End-to-end retrieval + chat injection verified via `utils/rag_smoke_test.py`.

**Multi-tenancy** — per-user document isolation via Google OAuth2:
- Each family member has a private NFS root (`/mnt/nfs/{Florian,Eva,Annika,Laura}`)
- Shared content under `/mnt/nfs/La-Familia` visible to all authenticated users
- `owner` field in every Qdrant point; search filtered by `MatchAny(any=[user_owner, "la-familia"])`
- `user_id` field added to `ChatRequest`, `SearchRequest`, and SQLite `chats` table
- User→NFS root mapping in `SoHoAI-config.yaml` (`users:` + `shared:` sections)

**Document ingestion**: `docling` (PDF, PPTX, DOCX) + dedicated ipynb cell extractor + `python-pptx` PPTX fallback + direct UTF-8 read (TXT, MD, YAML, CSV):
- **ipynb NOT handled by docling** — Fix: `_parse_ipynb()` in `ingest.py` parses JSON directly, extracts markdown cells as prose and code cells as fenced blocks, skips outputs and empty cells.
- **PPTX docling format detection failure** — Fix: `_parse_pptx()` in `ingest.py` uses `python-pptx` as secondary fallback; iterates slides/shapes, extracts all text frame content with `Slide N` headers. Raw UTF-8 read is last resort only if `python-pptx` also fails. 29 affected files force-re-queued (2026-04-19).

**Chunking — parent-child strategy**:
- Child chunks (~250 tokens, 20 overlap) → embedded, stored in Qdrant as search index
- Parent chunks (~800–1200 tokens, 100 overlap) → raw text only, stored in Qdrant payload (`parent_text` field)
- Flat 512-token chunks used only for PPTX slides and short TXT/YAML/config files

**Embeddings via bge-m3 served by Ollama** — 1024 dimensions, 570M params, ~1.2GB, top MTEB scores:
- Two modes: CPU (Server 1) ~650ms/chunk; GPU (Server 2, RTX 5070) ~10–20ms/chunk
- `embed_batch()` runs up to N concurrent requests via asyncio semaphore; N = `--batch` in daemon
- `embed_batch()` accepts optional `progress_cb(done, total)` — called every 50 chunks; used by `ingest.py` to log lines read by `rag_status.py --watch`
- HTTP timeout in `embed_text()`: **120s** — must survive concurrent embedding load

**Package**: `rag_engine/` — fully implemented (schema, collection, embeddings, state, scanner, ingest, search, multi_query, tool_use)

**Advanced RAG features (§8.1–§8.3)**:
- **`rag_mode` enum** (`off` / `on` / `only`) — legacy boolean toggle removed
- **Tool-use via system prompt** — LLM decides when to retrieve; no unconditional top-k injection
- **Multi-query + MMR** — `rag_engine/multi_query.py`; **permanently disabled** (`rag.multi_query.enabled: false`). Evaluated 2026-04-22: no-go verdict. Standard single-query retrieval is sufficient for this corpus.

**Phase 2 implementation checklist** — all 14 items completed and strikethrough:
~~1. Replace `unstructured` with `docling`~~ ~~2. Replace `sentence-transformers` with Ollama in `rag.py`~~ ~~3. Fix `SoHoAI-config.yaml` RAG section (bge-m3, ollama_url)~~ ~~4. Add `owner` to Qdrant payload schema; `user_id` to `ChatRequest`/`SearchRequest`~~ ~~5. Add multi-user config (`users:` + `shared:` sections) to `SoHoAI-config.yaml`~~ ~~6. Implement `rag_engine/` package (schema, collection, embeddings, state, scanner, ingest, search)~~ ~~7. Wire `rag_engine` into `main.py`; delete `rag.py`~~ ~~8. Implement standalone CLI utils (`utils/rag_*.py`)~~ ~~9. Add `POST /v1/rag/ingest/*` FastAPI endpoints~~ ~~10. Add `db_base_path` global config variable~~ ~~11. Configure `users:` section in `SoHoAI-config.yaml` with real Google emails~~ ~~12. Run initial NFS scan and ingestion~~ ~~13. Implement `rag_mode` (off/on/only), system-prompt tool-use loop, `prompts/` module~~ ~~14. Implement multi-query + MMR reranking (`rag_engine/multi_query.py`)~~

**→ NOW**: Phase 3 — Google OAuth2 middleware + OpenAI-compatible response format for Open WebUI (not blocking; RAG works end-to-end today via `--user florian`).

Full design details and worker loop spec: `RAG-strategy.md`.

---

## 2026-11-12 — Cleanup: SoHoAI is now gateway + RAG only (RAG-relevant lines only)

### Kept

- `/proxy/v1/*`, `/upstream/*`, `/health` (no `redis` key), `/v1/models/health`, `/v1/rag/search`.
- RAG untouched.

> Resolved in v0.1: the `.95` cron mention is stale (03:00 cron removed in v0.1 P5).

### Server roles after cleanup

- `.95`: llama-swap, Ollama bge-m3, rag-ingest, 03:00 snapshot cron.

### Follow-ups

- RAG code defaults `ollama_url` to `.93` (`rag_engine/embeddings.py:22`, `search.py:88`, `ingest.py:710`, `multi_query.py:210`). Fix in RAG session.
> Resolved in v0.1 (P2): the `ollama_url` fallbacks now point at `.95` in RAGNART.
> Also resolved: `multi_query.py` and `rag_mmr_bench.py` were dropped, not ported.
