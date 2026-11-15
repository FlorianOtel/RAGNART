---
title: "RAGNART — Implementation plan"
created_at: 2026-11-13--08-30
created_by: Claude Code (Claude Opus 5.5)
updated_by: Claude Code (Claude Haiku 5.5, /brain pipeline -- v0.1 close: §6 v0.1 dated note, §7 changelog note, §9 status row, v0.2 entry criterion); Claude Code (Claude Opus 5.5, 2026-11-13 -- post-v0.1 ops: Qdrant local config, daily snapshot retention, data root RAGNART--databases); Claude Code (Claude Opus 5.5, 2026-11-13 -- as-is pass: current-state only); Claude Code (Claude Opus 5.5, 2026-11-13 -- v0.2 fix-first items); Claude Code (Claude Opus 5.5, 2026-11-14 -- aligned with docs/RAGNART-design.md; v0.3 rewritten as project mode); Claude Code (Claude Opus 5.5, 2026-11-14 -- v0.2 prerequisites: corpus hygiene item 0, unscheduled design items assigned); Claude Code (Claude Haiku 5.5, 2026-11-14 -- p1 version rule, ragnart-git parallel decision, items 0-4 delivered, v0.3 CLI verbs on the git mapping); Claude Code (Claude Haiku 5.5, 2026-11-14 -- deviations A–G reference, Ollama context length 2048, v0.2 fix-first item 5 and reconciliation findings, future-work subsection); Claude Code (Claude Haiku 5.5); Claude Code (Claude Haiku 5.5, 2026-11-15 -- §3 item 1 measured baseline, fix-first list order 0-6)
updated_at: 2026-11-15--09-05
context: >
  Versioned, staged implementation plan for RAGNART, a self-hosted RAG
  retrieval service, built out into a nested, three-tier (Archive / Runtime /
  Transient) RAG with a self-hosted notebook UI (Kotaemon). Operator
  constraints: backward compatible with the existing corpus and ingestion
  pipeline (no re-embedding migration; renames and payload-only changes are
  fine); access control open-ended for multi-tenancy, enforced through one
  logical-ACL seam rather than filesystem permissions; the work kept to five
  versions, one working session each except v0.2, which may take two. The
  design this plan implements is docs/RAGNART-design.md. The current
  baseline (§3) is checked against the repository and the live Qdrant
  collection on Server 1 (192.168.1.93:6333); operational detail lives in
  docs/RAG-strategy.md. This document states the plan as it currently stands.
---

# RAGNART — Implementation plan

## 1. Purpose and how to use this plan

This plan implements the design in `docs/RAGNART-design.md` in five versions,
each sized to one Claude Code `/brain` session (v0.2 may take two). The design
document says *what* RAGNART is and why; this plan says in which order it is
built and how each step is accepted. Each version section is meant to be
self-contained: a session reads this document, its own version section, and the
design sections it cites.

Rules for a version session:

1. Re-read this document in full and `docs/RAGNART-design.md` §1 (principles
   and the status table) and §14 (decision register), especially §2 (Decisions already taken),
   §3 (Current baseline), §4 (Backward-compatibility invariants)
   and §5 (Access-control model) — these apply to every version, not just
   one.
2. Read its own version section in §6 in full.
3. Do its own `/brain` planning for the detailed design. This document gives
   goals, work items and acceptance checks, not file-by-file diffs — that
   level of detail belongs to each version's own planning session.
4. At the end, update its row in §9 (Version status table) and add its
   entry to the implementation log, `docs/v0--Implementation-log.md` (see §7,
   Docs & git discipline).

This document states the current plan. When something planned here turns out
to be wrong, the affected text is rewritten in place; the reason goes in the
commit message and in the implementation log.

## 2. Decisions already taken

| Decision | Choice | Source |
| --- | --- | --- |
| Where RAG work happens | The RAGNART repo and service only; SoHoAI carries no RAG code | operator |
| Design basis | `docs/RAGNART-design.md`; its decision register (§14) is binding for every version | operator |
| Item identity | Natural key per type; `content_hash` (MD5) as attribute; no surrogate `source_id` | design D1 (§4.2) |
| Versions of data | Schema version (data model) and per-item pipeline version (`p1`) | design D1b (§4.1, §5) |
| Pipeline version | `p1` is the working name of the current pipeline. A version change is a major event: an incompatibility or a significant change of behaviour (e.g. a different embedding model, reranking, a results-changing pipeline change). Fixes and improvements stay `p1` | operator |
| ragnart ↔ git parallel | RAGNART mimics git as closely as possible for ingestion, exclusion and lifecycle: git command names where the mapping is one-to-one, RAGNART terms otherwise; deviations from git are named (A–G). Full mapping and deviations: `docs/RAGNART-design.md` §1.2 | operator; design §1.2 |
| Access model | Everything is a project; grants to persons and groups; `inherit_access` opt-out; `owner` is metadata; enforced only in `resolve_scope()`/`authorize()` | design D2 (§6) |
| Projects | Addressed by path; `project.toml` with required `name`, immutable `id`, free tags | design D2b (§4.5) |
| Tiers | Transient = ephemeral cache (not indexed); Archive; Runtime; one store, one pipeline, static and curated policies; `curated` default | design D3 (§7) |
| Retrieval | Scope × grounding, defaults `project+strict` / `all+strict`, changes only by the operator; retrieval-only | design D4, D4b (§8) |
| Number of versions / sessions | Five versions, one `/brain` session each; v0.2 may take two | operator |
| Version scope | v0.1 spin-off · v0.2 nested tree and data model · v0.3 project mode (CLI, skill, distill) · v0.4 Kotaemon · v0.5 tenancy & authentication | operator |
| Package name, dot-directory | `ragnart`, `.ragnart/` | operator |
| Service host | Server 2 (192.168.1.95), beside ingestion | operator |
| Symlink rule | Real path is the key; an alias in another project is a link | design §4.5, §13.4 |
| Session attribution | By launch directory | design §7.4 |
| v0.2 migration mechanism | Payload-only `set_payload` backfill on the live collection; no new collection, no re-embedding | operator; §3 items 1 and 4 |
| `multi_query` / `rag_mmr_bench.py` | Not part of RAGNART | operator |

## 3. Current baseline

These facts hold for every version below. They are checked against the
repository and the live services; operational detail is in
`docs/RAG-strategy.md`.

1. **Corpus.** Measured 2026-11-15, after fix-first item 0 and the operator's
   `.ragignore` files: **348,163 points** in **13,386 items**, in one
   collection: dense vector (1024-dim, Cosine, unnamed) plus sparse
   `sparse_text` (IDF), `on_disk_payload: true`. The largest item has 4,559
   points; none is above the 5,000-chunk cap. Composition (points / items):

   | Type | Points | Items |
   | --- | ---: | ---: |
   | Claude Code sessions | 100,615 | 4,251 |
   | `.md` | 72,047 | 3,198 |
   | `.html` | 56,249 | 523 |
   | `.pdf` | 53,308 | 1,002 |
   | `.txt` | 18,521 | 1,428 |
   | `.ipynb` | 15,721 | 910 |
   | OpenCode sessions | 14,439 | 433 |
   | `.docx` | 10,173 | 403 |
   | `.pptx` | 3,408 | 438 |
   | `.sh` | 2,639 | 463 |
   | `.jsonl` | 411 | 101 |
   | `.yaml` | 338 | 81 |
   | `.yml` | 294 | 155 |

   Queue: 14,654 `completed` (1,270 of them without points; see the v0.2
   reconciliation findings), 268 `ignored`, 0 `pending`. The pre-purge history
   is in `docs/v0--Implementation-log.md`. No migration step may require
   re-embedding or rewriting vectors.
2. **`documents` is an alias.** `documents` → `documents_new`. Every
   reference to "the `documents` collection" below means the alias; the
   underlying name `documents_new` matters only for snapshot restore
   (`docs/RAG-strategy.md` §4.2 and §9.3).
3. **Only `source_path` is Qdrant-indexed.** The live `payload_schema` has
   one keyword index, `source_path`; `ragnart/collection.py:121` is the only
   `create_payload_index()` call. v0.2 adds the indexes of
   `docs/RAGNART-design.md` §4.4.
4. **Point IDs are random; replace deletes by `source_path`.**
   `ragnart/ingest.py:846` uses `uuid.uuid4()`; a file replace deletes by a
   `source_path` filter (`ragnart/ingest.py:722-735`,
   `utils/rag_sync_nfs.py:45-67`). So `set_payload` filtered by
   `source_path` can add or rewrite payload fields (including the key itself)
   without touching vectors, and a later full re-ingest still replaces cleanly by
   filter.
5. **Owner comes from config, per NFS root.** The scanner assigns each
   configured root's `owner` literal (`ragnart/scanner.py:119-132`);
   `derive_owner()` in `ragnart/schema.py:33` is not called. Search filters
   `owner IN [user, 'la-familia']` only when `user` is supplied
   (`ragnart/search.py:104-110`).
6. **Filesystem permissions cannot carry tenancy.** Both NFS roots
   (`/mnt/nfs/Florian`, `/mnt/nfs/La-Familia`) are `florian:florian`, mode
   `777`; nearly every file is owned by uid 555; there are no per-person Unix
   accounts; the NFS mount is `sec=sys` with no `all_squash`/`anonuid` on the
   client side. Hence §5 is a logical ACL rather than a mirror of
   `uid`/`gid`/`mode`; those values may be recorded as hints only and are never
   consulted by `authorize()`.
7. **Data root and snapshots.** All persistent data lives under
   `/mnt/nfs/__Backups/RAGNART--databases/` (`sqlite/rag_state.db`, `logs/`,
   `qdrant-snapshots/documents/`, `rag-ingest.lock`). One restore pair
   (`documents_new-*.snapshot`, ~21 GB, plus `rag_state-*.db`) is taken per
   day at the end of an ingestion run by `scripts/sqlite-qdrant-snapshot.sh`;
   7 pairs are kept (`docs/RAG-strategy.md` §9.1).
8. **Services and hosts.** Qdrant on Server 1 (192.168.1.93:6333).
   `ragnart-search.service` (`GET /v1/search`, `GET /health`, port 8100) and
   `ragnart-ingest.timer` (01:00, 07:00, 13:00, 19:00) on Server 2
   (192.168.1.95), which is the only host that opens `rag_state.db`. Both
   `/rag` command copies (`~/.claude/commands/rag.md`,
   `~/.config/opencode/commands/rag.md`) call
   `http://192.168.1.95:8100/v1/search`. SoHoAI's `GET /v1/rag/search` is a
   307 redirect stub to this service (its removal is a v0.2 entry
   criterion).

## 4. Backward-compatibility invariants

These are hard rules that every version from v0.2 onward obeys.

- **No migration re-embeds.** Every schema or layout change after v0.1 is
  additive metadata, applied through `set_payload` (filtered by
  `source_path`, or later by `project_id`) —
  never a new collection, never a vector rewrite.
- **SQLite and Qdrant move together.** `ingestion_queue.file_path` is the
  SQLite primary key. A project or source move updates the SQLite row and
  the Qdrant payload in the same operation (plan-then-apply). Updating only
  one side makes the next scan see delete+new and re-embed — wasteful, not
  corrupting, but it defeats the point of move detection.
- **Schema changes are additive-only, through `_MIGRATIONS`.** New columns
  and tables go through the existing `_MIGRATIONS` list pattern in
  `ragnart/state.py` (a list of `ALTER TABLE` statements applied
  idempotently, with failures swallowed because the column already exists;
  there is no schema-version table). Never rename or drop a column, and
  never change `ingestion_queue.file_path`'s role as primary key.
- **Search without new parameters is unchanged.** Until the `/ragnart` client
  replaces `/rag` (v0.3), `GET /v1/search` called without `scope` must return
  the same ranking it returns today; from v0.3 `scope` is required
  (`docs/RAGNART-design.md` §8.8). The regression
  test is the 14 queries in `utils/rag_bench_queries.txt`, run before and
  after every version's data migration (tie-aware, as search ordering is not
  fully deterministic; `docs/RAG-strategy.md` §6.5).
- **Collection name/alias and `rag_state.db`'s name do not change.**
  `documents` → `documents_new` and the file name `rag_state.db` stay the
  same at every version. The recovery procedures in `docs/RAG-strategy.md`
  §9 refer to both by name.
- **One database host, one ingestion daemon.** `rag_state.db` is opened from
  Server 2 only, and one ingestion daemon runs at a time, enforced by an NFS
  `lockf` lock (`docs/RAG-strategy.md` §5.6). If the search service ever
  needs the SQLite file directly rather than just Qdrant, it opens it
  read-only.
- **Snapshot before scale.** A matching Qdrant snapshot and `rag_state.db`
  copy is taken before any step that touches payload or schema at scale
  (v0.2's backfill, in particular), in the data root of §3 item 7.

## 5. Access-control model

The model is `docs/RAGNART-design.md` §6: everything is a project; access is
granted on projects to persons and groups; grants inherit downward unless a
project sets `inherit_access = false`; `owner` is the accountable person
(metadata and default grantee), not a filter; grants live in the state
database. Filesystem permissions carry no tenancy (§3 item 6).

**The single seam:** `resolve_scope()` in `ragnart/scope.py` is the only
function that turns a request into a Qdrant filter; `authorize(principal,
action, project_id)` is the only function that decides whether a principal may
touch a project.

**What each version adds:**

| Version | Addition |
| --- | --- |
| v0.1 | No seam. `GET /v1/search` applies the owner-only filter (§3 item 5). |
| v0.2 | Projects, `project_tags`, `grants` (shaped for persons and groups, roles `read`/`admin`) and the seam. One principal, granted every top-level project; the result equals today's owner filter. |
| v0.3 | Project-mode actions (add, rm, distill, add --wait) go through the CLI and the skill, and take the ingestion lock (`docs/RAGNART-design.md` §10.3). |
| v0.4 | Kotaemon's login is mapped to a principal; still no authentication. |
| v0.5 | Authentication (per-client tokens first, OAuth with group claims optional); `authorize()` enforces grants; grant management through CLI and API. |

## 6. Versions

### v0.1 — Spin-off

**Status.** Done (§9). The RAG code, its operations and its docs live in the
RAGNART repo and service; SoHoAI carries no RAG code.

**What exists.**
- The `ragnart` package, `utils/` operator tools and `scripts/` (systemd
  units, ingestion wrapper, snapshot script, Qdrant installer) in this repo;
  configuration in `ragnart.yaml`; dependencies pinned in `pyproject.toml`.
- `ragnart-search.service` on Server 2, port 8100: `GET /v1/search` and
  `GET /health` (`ragnart/service.py`).
- `ragnart-ingest.timer` → `ragnart-ingest.service` on Server 2, the only
  ingestion schedule.
- Both `/rag` command copies call the RAGNART service (§3 item 8).
- `utils/rag_compare_endpoints.py`: tie-aware result comparison between two
  search endpoints (`docs/RAG-strategy.md` §6.5).
- Docs: `docs/RAG-strategy.md`, `docs/RAG-troubleshoot.md`,
  `docs/design-history.md` and the session record
  `docs/v0--Implementation-log.md`.
- SoHoAI's `GET /v1/rag/search` is a 307 redirect stub to this service; its
  removal is a v0.2 entry criterion.

### v0.2 — Nested

**Goal.** Land the project tree in `rag_state.db` and in the Qdrant payload
by payload-only backfill, with no vectors touched, and give search project
scoping.

**Entry criteria.** v0.1 done (§9). SoHoAI's `GET /v1/rag/search` 307 stub
removed, on or after 2026-11-16, after checking `SoHoAI-gateway.log` for 307
hits and repointing any caller found. Fix-first item 0 delivered (below).

**Fix first (before any schema or backfill work).** The backfill and the
move detection below rely on a clean corpus and on the sync never deleting
wrongly. Item 0 is delivered by a separate operator session that bridges
v0.1 and v0.2; the v0.2 session verifies it. Items 1–6 are fixed, tested and
run once by hand (log inspected) before the timer picks up the new sync.
None of items 1–6 changes Qdrant data or the schema.

0. **Corpus hygiene: admission rules and the purge, within `p1`.** The admission rules keep binary, oversized and over-chunked
   content out of the index (`docs/RAGNART-design.md` §5); the pollution measurements, root causes and
   the purge are recorded in `docs/v0--Implementation-log.md` (bridge session
   entry).
   Delivered by the bridge session:
   - admission rules in the current pipeline `p1`: no raw-read fallback for
     binary types, a binary check before parsing, size and chunk caps that mark
     items `ignored` with a visible reason, a corrected quality gate; the rules
     are stated in `docs/RAGNART-design.md` §5 (admission);
   - `.ragignore` exclusion rules with gitignore semantics, and the global
     `config/ragignore`; described in `docs/RAGNART-design.md` §1.2;
   - the purge tool `utils/rag_purge_rescan.py` (dry-run by default; applies
     only with `--apply`, after a snapshot pair) and the corrected test in
     `utils/rag_purge_corrupt.py`;
   - the policy decisions behind it (CSV and data files, cap values, mirrored
     trees) recorded in `docs/RAGNART-design.md`.
   Verified: point count and composition (no item above the
   caps, a sample of PDF chunks is text); §3 item 1 holds the measured figures.

1. **An unreachable NFS root deletes its whole tree from the index (critical).**
   Delivered by the bridge session.
   `ragnart/scanner.py` (`scan_nfs_roots`) skips a root that fails
   `os.path.isdir()`, so that root adds nothing to `existing_paths`;
   `StateDB.find_deleted()` (`ragnart/state.py`) treats every queue row missing
   from the scan as deleted, and `utils/rag_sync_nfs.py` then deletes the Qdrant
   points of every completed file under that root and purges the rows.
   Fix:
   - `scan_nfs_roots()` returns `scanned_roots` and `skipped_roots`.
   - If any in-scope root was skipped, `rag_sync_nfs.py` skips the whole deletion
     phase (`find_deleted`, Qdrant deletes, `purge_deleted`), logs an ERROR and
     exits non-zero; queueing new and modified files still runs.
   - A root that is a directory but empty while the queue holds completed rows
     for it counts as unmounted.
   - Missing-sources threshold: if `deleted_paths` exceeds
     `rag.sync.max_delete_fraction` of completed rows (default 0.10), the deletion
     phase is skipped with an ERROR; `--allow-mass-delete` overrides it.
   Test: a temporary root with one ingested file made inaccessible -> sync exits
   non-zero, row and points remain; deleting more than 10% of rows -> deletion
   phase aborted unless `--allow-mass-delete`.

2. **`--user florian` deletes other owners' rows.** The scanners honour
   `user_filter`, but `find_deleted()` selects all rows of `ingestion_queue`, so
   rows of any other owner (including the shared `la-familia` root) look gone on
   every scheduled run (`scripts/ragnart-ingest-run.sh` syncs only
   `RAG_SYNC_USERS=("florian")`). No live impact while `/mnt/nfs/La-Familia` is
   empty.
   Delivered by the bridge session.
   Fix:
   - `find_deleted(existing_paths, owners)` restricts to `WHERE owner IN (...)`
     (the `owner` column already exists); `rag_sync_nfs.py` passes the owners
     whose sources were actually scanned.
   - The scheduled sync covers every configured root (see Decisions to confirm).
   Test: a `la-familia` row whose file is on disk survives a `--user florian` sync,
   with its points.

3. **`rag_status.py` and `rag_reset.py` ignore the `documents` alias.** Both
   build their collection set from `get_collections()` (real names only:
   `documents_new`) and test it for `documents`. `rag_status.py` reports the
   collection as missing; `rag_reset.py --user X` skips the Qdrant delete but
   still resets rows to `pending`, and the full reset skips its Qdrant step.
   Delivered by the bridge session.
   Fix:
   - `collection_exists(client, name)` in `ragnart/collection.py`, true for a real
     collection or an alias (`get_aliases()`); used by both scripts.
   - `rag_reset.py` never drops a collection by alias name: the full reset either
     refuses and points to `docs/RAG-strategy.md` §9.3, or deletes points by filter.
   - `rag_reset.py --dry-run` reports what would be deleted.
   Test: `rag_status.py` shows the point count; `rag_reset.py --user florian
   --dry-run` reports the points it would delete.

4. **`/health` hangs and `/v1/search` stays 503 when Qdrant was down.**
   `ragnart/service.py` sets `app.state.qdrant_client = None` if Qdrant is
   unreachable at start-up, so every `/v1/search` answers 503 until the service
   restarts; `/health` calls `get_collections()` with the 60 s client timeout
   (`ragnart/collection.py` `get_client`).
   Delivered by the bridge session.
   Fix:
   - The client is always created at start-up; failures surface per request.
   - `/health` checks Qdrant's `/healthz` with a 2 s timeout and answers
     `{"status":"degraded","qdrant":false}` immediately when it fails.
   Test: Qdrant stopped -> `/health` answers `degraded` within 3 s; Qdrant started
   -> `/v1/search` answers 200 without restarting `ragnart-search.service`.

5. **A manual sync can overlap the ingestion daemon.** `utils/rag_sync_nfs.py`
   does not take `rag.ingest_lock`, so it can delete or purge while the daemon
   writes (`docs/RAGNART-design.md` §13.2, §10.3).
   Fix: the sync takes `rag.ingest_lock` as the daemon does, and a sync that
   cannot take it exits non-zero, naming the lock holder.
   Test: with the daemon holding the lock, a manual sync exits without deleting
   anything; with the lock free, the sync runs.

6. **The ingest daemon waits for the slowest item of each batch.**
   `utils/rag_ingest_daemon.py` `run()` fetches 10 pending rows per loop
   iteration (`_FETCH_BATCH_SIZE`, hardcoded). `--batch` is not the fetch size;
   it sets the Ollama embedding concurrency within one file. The 10 rows run
   under an `asyncio.Semaphore(--workers)` in one `asyncio.gather`, and the next
   fetch happens only when all 10 have finished. Parsing runs in a worker thread
   (`asyncio.to_thread`, with a thread-local converter from `_get_converter()` in
   `ragnart/ingest.py`). One long document (a book on CPU docling takes 6–25 min)
   leaves the other workers idle until it completes. Measured during the re-parse
   backlog on Server 2: mostly one of three workers busy.
   Fix:
   - Replace the fetch-and-gather loop with a continuous pool: `--workers`
     tasks, each taking the next pending row as soon as its previous file
     finishes.
   - The fetch and the claim of the row are one step on the event loop, so two
     workers never take the same row.
   - Keep the single-writer rule (one ingestion daemon, `rag_state.db` written
     from it, `docs/RAG-strategy.md` §5.6 lock on `rag.ingest_lock`) unchanged.
   Test: with `ingest_file` mocked and `--workers 3`, one item that sleeps for
   30 s (scaled down for the test) and 20 fast items: all fast items complete
   while the slow one is still running. With 12 uniform items of 1 s, the run
   takes about 4 s, so throughput approaches N times the single-worker rate.
   Each file path is processed exactly once.

**Work items.**
- Introduce the schema version (`docs/RAGNART-design.md` §4.1) and add the
  tables (`projects`, `project_tags`, `project_links`, `source_links`,
  `grants`) and the per-item metadata of design §4.3 (`source_type`,
  `project_id`, `size_bytes`, `content_hash`, `hashed_at`, `first_seen_at`,
  `modified_at`, `pipeline_version` (the version that produced the item's
  chunks; every item is `p1`, and the column records the version for a future
  major change, §2),
  `chunk_count`, `missing_since`, origin details) to `ragnart/state.py`.
- Add the payload fields and indexes of design §4.4 (`project_id`, `owner`,
  `source_type`, `file_type`, `content_hash` as keywords; `modified_at`,
  `ingested_at` as datetimes) — online and additive; `documents` keeps serving
  search while they build.
- Re-key Claude Code sessions from their `.jsonl` path to
  `claude-session://<uuid>` (design §4.2), in the state database and the
  payload together; no re-embedding.
- Write a backfill script modelled on `utils/rag_sparse_migrate.py`, but
  doing `set_payload` by `source_path` filter on the live
  `documents` alias — no new collection, no alias swap. Dry-run first;
  resumable (checkpoint by last `source_path` processed); batched. It must
  derive `project_id` and the new metadata the same way the extended scanner
  does, and also backfill session attribution (Claude Code `cwd`, OpenCode
  `location.directory`) read fresh at parse time — this data is currently
  parse-time-only and is not persisted.
- Make each NFS root a top-level project and create a per-owner `sessions`
  project, so every file and every session has a `project_id` even with no
  `.ragnart/project.toml` anywhere above it (design §6.1).
- Extend the scanner and `rag_sync_nfs.py` with marker discovery
  (`.ragnart/project.toml`: `name`, `id`, `inherit_access`, `mode`, tags),
  move detection by `content_hash`, tombstones with a grace period, and
  plan-then-apply reconciliation (design §4.2, §10.2), on top of the mount
  guard and missing-sources threshold of fix-first item 1.
- Add `ragnart/scope.py` (`resolve_scope`, `authorize`, per §5), the `scope`
  parameter on `GET /v1/search` (project path, selection by paths and tags,
  `all`; resolved scope echoed in the response; design §8.8), and
  `GET /v1/projects`.
- Attach sessions to projects by launch directory (design §7.4).
- Search: `max_hits_per_source` cap and collapse of identical copies by
  `content_hash` (design §9.1, §8.7), with the cap as a knob in `ragnart.yaml`
  and per request (echoed).
- Reconciliation: a scheduled check of `chunk_count` against Qdrant and of
  Qdrant keys against rows; repairs under the deletion rules and fails loudly
  in the log (design §10.3). Its first run must also resolve these current
  findings:
  - **Zombie paths.** Two paths under `Gin-AI/dot.claude-Linux` (an AYA session
    `.jsonl` and `memory/aya-v06a-phased.md`, 307 points) have points but no
    queue row. Rationale: the check compares Qdrant keys with rows, so it must
    report and resolve points without a row.
  - **Completed rows without points.** About 1,270 `completed` rows have zero
    Qdrant points. Rationale: a `completed` row whose points are missing is the
    divergence this check exists to find.
  - **Crash in the purge.** A crash between the Qdrant delete and the row update
    in `utils/rag_purge_rescan.py` leaves a `completed` row without points.
    Rationale: the same divergence, with a known cause.
- Nightly online backup of `opencode.db` to
  `RAGNART--databases/opencode-backups/<hostname>-<ip>/`, installed by
  `scripts/install-units.sh` (design §10.5).

**Data migration & rollback.**
1. Snapshot pair first: Qdrant snapshot of `documents_new` plus a
   `rag_state.db` copy in the data root of §3 item 7, taken on demand per
   `docs/RAG-strategy.md` §9.5.
2. Pause the ingestion timer (hold its NFS lock) for the backfill's
   duration — the backfill and an ingestion run must not touch the same
   rows concurrently.
3. Apply `_MIGRATIONS` (tables and columns).
4. Create the three payload indexes.
5. Run the backfill dry-run, inspect the plan, then run it for real,
   batched and resumable.
6. Resume the ingestion timer.

Rollback: restore from the snapshot pair per `docs/RAG-strategy.md` §9.3
(full) or §9.4 (state-DB-only). Because the
backfill never rewrites vectors, a SQLite-only rollback is expected to
suffice; the extra payload fields and indexes left in Qdrant are inert to
the old code and are left in place by design, not removed. A SQLite-only
rollback also reverts the backfill's checkpoint, so a retried backfill
restarts from zero rather than resuming (it is idempotent, so this costs
time, not correctness).

**Definition of done.**
- Point count in `documents` is unchanged before/after the backfill.
- No vectors are rewritten (verified on a sample of points: vectors read
  before and after are identical).
- The 14 bench queries return the same results with no `project` given.
- A scoped search (`scope=<project path>`) returns only that project's
  sources, and the response names the projects searched.
- Moving a project directory changes one `projects` row and zero Qdrant
  points.
- Fix-first item 0 verified; items 1–5 pass their tests.
- Reconciliation reports zero unexplained differences on the clean corpus.
- A result list never contains more than `max_hits_per_source` passages from
  one item.

**Risks.** Backfill runtime against the clean corpus (§3 item 1: 348,163 points) needs measuring, not assuming. Re-deriving session `cwd`/`location.directory` at
backfill time may disagree with what a later ingestion run derives if
sessions moved in the meantime. The 10% missing-threshold default may be
wrong for this corpus's real churn rate and needs a dry run before being
trusted.

**Decisions to confirm at kickoff.** Scheduled sync scope: all configured
roots in one run (recommended) or `la-familia` added to `RAG_SYNC_USERS`;
missing-sources threshold (10% suggested); grace period before purge (24h suggested); backfill batch size;
whether to pilot the backfill on one owner's data before the full corpus;
the default `max_hits_per_source`; how many OpenCode backup copies to keep.

**Estimated sessions.** 2 (the migration script and the reconciliation/
scoping work are each large enough to warrant a session).

### v0.3 — Project mode

**Goal.** Make projects usable as notebooks (`docs/RAGNART-design.md` §7.3,
§7.4, §8): the `ragnart` CLI, the `/ragnart` skill and commands with scope and
grounding, the project memory, distill, and `add --wait`. The Transient tier is
an ephemeral cache of files; nothing in this version creates a second vector
index.

**Entry criteria.** v0.2's Definition of done met. The Transient lifecycle
("cache dynamics": admission, states, transitions, eviction, invalidation,
distill rules, actors) is defined and recorded in `docs/RAGNART-design.md`
§7.5 (design §13.3).

**Work items.**
- `ragnart` CLI, built on the git mapping (`docs/RAGNART-design.md` §1.2):
  `init` (writes `.ragnart/project.toml` with `name`, `id`,
  `mode = "curated"`), `add`, `add --wait`, `rm` (appends a pattern to the
  nearest `.ragignore`; never deletes a file), `mv`, `diff`, `fsck`, `gc`,
  `distill`, and `where`, `inbox`, `expect`, `calibrate`, `grant`, `revoke`,
  `transfer`, which have no git equivalent. Plus the metadata listing (`ls`
  with type and time filters, answered from the state database).
  `status`, `ls-files` and `check-ignore` are not v0.3 work: they were
  delivered with v0.2 item 0.
- `.ragnart/inbox/` as the Transient cache: files only, project-scoped, with
  the expiry and admission rules defined at kickoff.
- Distill: deterministic selection (rules in `project.toml` and explicit
  `add`/`rm`, recorded) and file placement into the project's references; the
  agent updates `.ragnart/MEMORY.md` (an index of pointers and hooks, design
  §7.4). Distill and `add --wait` take the ingestion lock.
- Curated ingestion mode: the static crawl skips `curated` projects except
  their references and `MEMORY.md`; `crawl` projects are indexed in full.
- `/ragnart` skill and commands for Claude Code and OpenCode: scope and
  grounding knobs, defaults `project + strict` inside a project and
  `all + strict` outside, the status line, the "no good hits in this scope"
  stop, one-off `ask`, verbose translation of natural-language scope, loading
  the project's `MEMORY.md` on entry (design §8). `/rag` becomes an alias
  (design §8.9).
- `scope` becomes required on `GET /v1/search` once both clients use
  `/ragnart` (§4).
- Add `.ragnart/` to the global git-ignore file (`core.excludesFile`); add
  `.ragnart/inbox/`, `.opencode/` and `.agents/` to `config/ragignore` (or to a
  `.ragignore`), so the NAS crawl excludes them (`docs/RAGNART-design.md` §1.2).
- Retrieval calibration (design §9.2, §9.3): `min_relevance`, `type_weights`
  and the fetch sizes as knobs at three levels; the labelled set with
  `/ragnart expect`; `ragnart calibrate`.

**Data migration & rollback.** None to `documents`. The cache is plain files;
references enter through the normal ingestion pipeline.

**Definition of done.**
- In a test project created with `ragnart init`: gather (add pages and files),
  `rm` some, distill; after `add --wait` the distilled references and
  `MEMORY.md` are returned by a `project + strict` search with citations, and
  items removed with `rm` are excluded from the index after the next sync while
  their files stay on disk.
- The same flow works in Claude Code and OpenCode.
- Every answer shows the status line; scope changes happen only through the
  commands or a stated natural-language translation.
- A `curated` project's non-reference files are not in the index.

**Risks.** Capture and curation depend on the skill being followed; nothing
enforces it. Distill's generated hooks need review by the operator. Whether
OpenCode loads skills from `~/.claude/skills` is unverified (§8).

**Decisions to confirm at kickoff.** The Transient lifecycle rules; expiry
period; default distill rules for a new project.

**Estimated sessions.** 1–2.

### v0.4 — Kotaemon

**Goal.** Give RAGNART a browser UI that reads it through its own index
plugin, for asking across a project's sources with citations, without
driving the CLI.

**Entry criteria.** None strict for the stock-Kotaemon spike, which can be
done any time. The plugin needs v0.2's `scope` parameter and
`GET /v1/projects`, and v0.3's `add --wait`.

**Work items.**
- Spike (any time before this version, if useful): run stock Kotaemon
  against SoHoAI's `/proxy/v1`, confirm one chat turn works; change the
  default `admin` login immediately — LAN-only is not a reason to skip this.
- Write the index plugin: retriever over `GET /v1/search`, project picker
  from `GET /v1/projects` as the scope, project and `source_type` shown on
  each citation, librarian (strict) prompt by default (design §8.8). Registers
  through `KH_INDEX_TYPES`/`KH_INDICES` in `flowsettings.py`, modelled on
  Kotaemon's bundled GraphRAG index (`libs/ktem/ktem/index/file/graph`).
- Uploads become references of the selected project (a deliberate act),
  followed by `add --wait` (design §7.4).
- Map the Kotaemon login to a principal and pass it on every call (§5, v0.4
  row). RAGNART still trusts the passed value until v0.5.
- Pin the Kotaemon version used; deploy as one container on Server 1, port
  7860 (NAS mounted read-only if citation previews need source files).

**Data migration & rollback.** None — this version adds a client, not
state.

**Definition of done.**
- A question asked with a project selected cites only that project's
  sources.
- The top sources match `ragnart search` run with the same query and scope.
- A search issued from a given Kotaemon login returns only projects granted
  to that principal.

**Risks.** Kotaemon's base-class interface for custom index types is
thinly documented (unverified) — the plugin may need more reverse-engineering
than estimated. Version drift on every
Kotaemon upgrade, since the plugin depends on internal classes. Whether
Docker is installed on Server 1 is unverified and blocks the container step
until checked.

**Decisions to confirm at kickoff.** Which Kotaemon version to pin; whether
the NAS read-only mount for citation previews is needed from day one or can
be deferred.

**Estimated sessions.** 1.

### v0.5 — Tenancy & authentication

**Goal.** Make the principal non-spoofable, enforce grants, add grant
management, and validate the two desktop clients end to end.

**Entry criteria.** v0.2's `grants` table and `authorize()` seam exist;
v0.3/v0.4 give something worth protecting.

**Work items.**
- Per-client API tokens first; OIDC retained as an option, not required for
  this version's Definition of done — SoHoAI `docs/TODO.md` already lists
  Google OIDC as a plan with no code, and it can be picked up here instead
  of tokens if the operator prefers (decision to confirm at kickoff, not
  pre-decided).
- `authorize()` starts consulting the `grants` table instead of only the
  owner rule; `resolve_scope()`'s shape is unchanged, its `authorize()`
  calls now have teeth.
- `user`/principal is derived from the validated token, never from a
  request parameter the caller sets.
- Grant management (`read`, `admin`) through the CLI and API; groups from
  the token's groups claim (design §6.2–§6.4).
- Validate OpenCode Desktop against the Server 2 OpenCode server
  (`192.168.1.95:4096`) — confirm the skill and commands load there (not
  stated in OpenCode's docs).
- Validate Claude Desktop Code sessions, local and over SSH; confirm where
  their transcripts land relative to `claude_chats.roots` (currently
  `/home/florian/.claude/projects`) — unverified.

**Data migration & rollback.** None to vector data. This version is
additive application logic over tables that already exist from v0.2.
Rollback is a feature flag / config revert (disable token enforcement, fall
back to the owner-only rule), not a data rollback.

**Definition of done.**
- A request without a valid token is rejected, or mapped to an anonymous
  principal with no project access (operator's choice at kickoff, see §8).
- A second principal (for example, a `la-familia`-only token) sees only
  content it has been granted, not the full corpus.
- A group grant gives every member of the group access, and only them.
- OpenCode Desktop sessions and Claude Desktop Code sessions (local and
  SSH) both appear correctly in the Runtime tier after the next ingestion
  run.

**Risks.** This version turns an until-now-open system into an enforced
one — a bug here fails closed (locks someone out) or fails open (leaks
data); test both directions before calling it done. Desktop-app transcript
locations are unverified and may require a
short side investigation before the Runtime-tier check can be written.

**Decisions to confirm at kickoff.** Tokens vs OIDC (or both, tokens
first); whether an invalid/missing token means "rejected" (HTTP 401) or
"anonymous, no grants" — these are different failure modes.

**Estimated sessions.** 1.

## 7. Cross-cutting

### Operations

- Data root: `/mnt/nfs/__Backups/RAGNART--databases/` (§3 item 7;
  `docs/RAG-strategy.md` §1.3).
- Snapshots: `scripts/sqlite-qdrant-snapshot.sh`, run by the ingestion
  wrapper once per day, writes paired `documents_new-*.snapshot` and
  `rag_state-*.db` files with the same timestamp to
  `qdrant-snapshots/documents/`; 7 pairs are kept. Qdrant's own
  `snapshots_path` is Server 1's local NVMe and holds a snapshot only while it
  is being downloaded. Policy and pair consistency: `docs/RAG-strategy.md`
  §9.1.
- Recovery: `docs/RAG-strategy.md` §9.3 (full) and §9.4 (state-DB-only).
- The collection name/alias (`documents` → `documents_new`) is unchanged
  across every version (§4).

### Docs & git discipline

- Each version's session updates RAGNART's docs before its commit, per the
  operator's global git-discipline rule.
- `docs/v0--Implementation-log.md` is the session record: one entry per
  version session, naming the commit(s) and the Definition-of-done checks
  that passed.
- This document is updated at the end of each version: the version's row in
  §9, and any affected text rewritten in place to the current plan.
- Descriptive docs (this plan, `README.md`, `docs/RAG-strategy.md`,
  `docs/RAG-troubleshoot.md`) describe the current state only; history lives
  in git, the implementation log and `docs/design-history.md`.
- `docs/RAG-strategy.md`, `docs/RAG-troubleshoot.md` and
  `docs/design-history.md` are the operator's own documents: ask before any
  content edit.

### Terms

The vocabulary is defined in `docs/RAGNART-design.md`: tiers (§7.1),
ingestion policies (§7.2), project mode and distill (§7.3), scope and
grounding (§8.1). The project marker directory is `.ragnart/`; inside it,
`inbox/` is the Transient cache and `sources/`, `notes/` and `MEMORY.md` are
the project's references.

### Future work (unscheduled)

Pipeline and tooling items with no version assigned. Version rule (§2): a
results-changing pipeline change is a new pipeline version; fixes and
improvements stay `p1`.

- **Size chunks in bge-m3 tokens.** Removes the residual rejection of
  over-length chunks by Ollama (`docs/RAGNART-design.md` §5.6). Changes chunk
  boundaries and therefore search results: candidate for the next pipeline
  version, not `p1`.
- **Real page, slide and cell numbers.** `page` is always `0` (`ragnart/schema.py`
  `FIELD_PAGE`; `docs/RAGNART-design.md` §4.4, §5.4), so it carries no
  information. `p1` improvement; applies to newly ingested items.
- **Secret and credential filter before embedding.** Credentials in files and
  sessions are embedded and returned today (`docs/RAGNART-design.md` §13.4).
  `p1` improvement: it changes which items are indexed, not how they are
  embedded or ranked.
- **`rag.hybrid.rrf_k: 60` in `ragnart.yaml` is read by no code.** Fusion uses
  Qdrant's built-in RRF constant, which the fused scores show is k = 2
  (`docs/RAG-strategy.md` §6.1), so the file's 60 does not describe the
  behaviour. Remove the key: `p1` improvement, no result change. Wiring it at
  any value other than the built-in one changes fused ranking: candidate for
  the next pipeline version.
- **Consolidate `utils/rag_purge_corrupt.py` into `utils/rag_purge_rescan.py`.**
  The two tools overlap in purpose. Tooling only; not a pipeline change.
- **Configurable docling device.** `rag.docling.device: cpu | cuda` in
  `ragnart.yaml` (default `cpu`), optionally overridden by an environment
  variable for a single manual run. `_build_converter()` in `ragnart/ingest.py`
  hard-codes `AcceleratorDevice.CPU` for PDF, DOCX and PPTX. The default stays
  CPU: on Server 2 the RTX 5070 has about 2 GB free beside the chat LLM (8.4 GB),
  the reranker (0.6 GB) and Ollama bge-m3 (1.1 GB). A docling converter needs
  about 1–3 GB per worker, and docling holding GPU memory can push an Ollama model
  reload onto CPU or make it fail. `cuda` is for an operator-chosen backlog run
  at a quiet time, for example after unloading the chat model. No automatic
  GPU-first with CPU fallback: considered and not planned (complexity, VRAM
  contention with Ollama, device-dependent parse differences). This is a `p1`
  improvement, not a version change: same models, speed only. GPU and CPU
  inference can differ slightly, so before adopting it, time one book on GPU and
  on CPU.

## 8. Open decisions and parameters to confirm

| # | Item | Suggested default | Confirm at |
| --- | --- | --- | --- |
| 1 | Transient expiry | 30 days | v0.3 kickoff |
| 2 | Grace period before purging missing Archive sources | 24 hours | v0.2 kickoff |
| 3 | Missing-sources threshold that aborts a sync | 10% | v0.2 kickoff |
| 4 | RAGNART service port on Server 2 | 8100 | settled |
| 5 | Token vs OIDC detail (format, issuer, storage) | per-client tokens first, OIDC optional later | v0.5 kickoff |
| 6 | Invalid/missing-token behaviour | not pre-decided — rejected (401) vs anonymous-no-grants | v0.5 kickoff |
| 7 | Docker on Server 1 (for Kotaemon's container) | unverified — check before v0.4 kickoff | v0.4 kickoff |
| 8 | OpenCode skill loading from `~/.claude/skills` | unverified per OpenCode docs — test early | v0.3 kickoff |
| 9 | Claude Desktop Code session transcript location | unverified — needed before the Runtime-tier check can be written | v0.5 kickoff |
| 10 | Backfill batch size / pilot scope (one owner first?) | not pre-decided | v0.2 kickoff |
| 11 | Notifications (channel, events: failed run, guard tripped, snapshot failed, reconciliation problems) | not scheduled; ntfy on Server 1 available | open |
| 12 | Corpus-hygiene policy: CSV and data files, size and chunk caps, mirrored trees | decided in fix-first item 0 | bridge session |

## 9. Version status table

| Version | Status | Session(s) | Commit(s) |
| --- | --- | --- | --- |
| v0.1 Spin-off | done | 1 | RAGNART: `e4de8a3` (import), `1d702fe` (adaptation), `796635f` (comparison tool, pins); SoHoAI: `95d5d0d` (removal, 307 stub) |
| v0.2 Nested | planned | — | — |
| v0.3 Project mode | planned | — | — |
| v0.4 Kotaemon | planned | — | — |
| v0.5 Tenancy & authentication | planned | — | — |

Each version's session updates its own row (`planned` → `in progress` →
`done`) and fills in the session count and commit hash(es) when it finishes.
