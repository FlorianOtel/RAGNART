---
title: "RAGNART — Implementation plan"
created_at: 2026-11-13--08-30
created_by: Claude Code (Claude Opus 5.5)
updated_by: Claude Code (Claude Haiku 5.5, /brain pipeline -- v0.1 close: §6 v0.1 dated note, §7 changelog note, §9 status row, v0.2 entry criterion); Claude Code (Claude Opus 5.5, 2026-11-13 -- post-v0.1 ops: Qdrant local config, daily snapshot retention, data root RAGNART--databases); Claude Code (Claude Opus 5.5, 2026-11-13 -- as-is pass: current-state only); Claude Code (Claude Opus 5.5, 2026-11-13 -- v0.2 fix-first items); Claude Code (Claude Opus 5.5, 2026-11-14 -- aligned with docs/RAGNART-design.md; v0.3 rewritten as project mode); Claude Code (Claude Opus 5.5, 2026-11-14 -- v0.2 prerequisites: corpus hygiene item 0, unscheduled design items assigned); Claude Code (Claude Haiku 5.5, 2026-11-14 -- p1 version rule, ragnart-git parallel decision, items 0-4 delivered, v0.3 CLI verbs on the git mapping); Claude Code (Claude Haiku 5.5, 2026-11-14 -- deviations A–G reference, Ollama context length 2048, v0.2 fix-first item 5 and reconciliation findings, future-work subsection); Claude Code (Claude Haiku 5.5); Claude Code (Claude Haiku 5.5, 2026-11-15 -- §3 item 1 measured baseline, fix-first list order 0-6); Claude Code (Claude Fable 5.2, 2026-11-15 -- v0.3 aligned with the project workflow of docs/RAGNART-design.md §7.3 to §7.5); Claude Code (Claude Haiku 5.5, 2026-11-15 -- v0.2 fix-first item 5 delivered for every index writer; project.toml reserved keys per design §4.5; session attribution cites design §4.3); Claude Code (Claude Haiku 5.5, v0.2 session A -- fix-first item 6 delivered; search cap and OpenCode backup delivered; scope required on /v1/search; settled kickoff decisions; subagent and session attribution rules); Claude Code (Claude Haiku 5.5, v0.2 session B -- baseline data model and kickoff decisions, work items 1-4 delivered, items 5 and 8 rewritten, migration steps 3-5 and DoD status; the session-uuid collision rule (`docs/RAGNART-design.md` §4.2, §14 D11) and the measured DoD point-count outcome; DoD bench result, backfill runtime and restore pair size; decision labels removed from the plan, `docs/RAGNART-design.md` §14 is the only register; point-count arithmetic and access/attribution rows aligned with design §4.3, §4.6, §6); Claude Code (Claude Haiku 5.5, v0.2 session C -- scope required and §4 invariants, §6 v0.2 work items and Definition-of-done results, §9 v0.2 row done; §3 item 1 consistent empty rows, §1 and §2 v0.2 session count, §6 v0.2 project_id item delivered; §6 v0.2 estimated-sessions line keeps the estimate only; the Actual sentence removed); Claude Code (Claude Fable 5.2, 2026-11-16 -- multi-source design of docs/RAGNART-design.md §2.3, §4.7, §4.8: new v0.3 Sources, later versions renumbered (project mode v0.4, Kotaemon v0.5, tenancy v0.6); §2 decisions, §4 invariants, §7 terms, §8 parameters, §9 status); Claude (Claude Fable 5.2, claude.ai design-review session with the operator, 2026-11-16 -- design D27 to D31 and the amended D8, D10, D15, D19: §2 decisions, §4 invariants, v0.3 and v0.4 work items and definitions of done, §7 terms and future work, §8 open decisions); Claude Code (Claude Opus 5.5, 2026-11-17 -- design discussion with the operator, design D32 to D37 and the amended D2, D2b, D3, D9, D10, D12, D15, D19, D21, D27, D28, D29: §2 decisions, §5 access model, v0.3 registry item, v0.4 rewritten, v0.5 uploads, §7 terms, §8 open decisions; then D38 to D41, references as resources, the crawl and project folders, no implicit commit, D42: central renamed the hub; projects addressed by unique name)
updated_at: 2026-11-17--14-32
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

This plan implements the design in `docs/RAGNART-design.md` in six versions,
each sized to one Claude Code `/brain` session, except v0.2, which takes five
(the bridge session, the item 5 session, and v0.2 sessions A, B and C), and
v0.3, which takes two (sessions A and B). The design
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
| Item identity | The pair (source, natural key); the key's format is its connector's; `content_hash` (MD5) as attribute; no surrogate item id | design D1, D18 (§4.2, §4.7) |
| Versions of data | Schema version (data model) and per-item pipeline version (`p1`) | design D1b (§4.1, §5) |
| Pipeline version | `p1` is the working name of the current pipeline: what the hub does to a submitted item, from parsing to embedding. Retrieval is not part of it. A version change is a major event: a change of parsing, admission or chunking that is incompatible or changes results significantly, or a different embedding model. Fixes and improvements stay `p1`. The hub stamps the version on every chunk. A change of parsing or chunking is applied per item as sources re-submit; a change of embedding model is applied to the whole collection by the hub and is never mixed | operator; design D8, D30 (§4.1) |
| ragnart ↔ git parallel | RAGNART mimics git as closely as possible for ingestion, exclusion and lifecycle: git command names where the mapping is one-to-one, RAGNART terms otherwise; deviations from git are named (A–I). Full mapping and deviations: `docs/RAGNART-design.md` §1.2 | operator; design §1.2 |
| Access model | Every item belongs to exactly one project: a ragnart project, or the reserved `global` project. Grants apply on ragnart projects; the owner rule applies to `global` items | design §4.6, §6.1, §6.5 |
| Projects | A ragnart project is a notebook: a folder created by `ragnart init <name>`, whose managed folders `inbox/`, `resources/` and `collateral/` sit beside `.ragnart/`; its material, indexed by commits, is `resources/` and `collateral/`. Every other file of the folder is not indexed: the global crawl does not enter a project folder. `.ragnart/` holds state only. Nesting has no meaning. Addressed by its `name`, unique across the hub registry; the folder's path is metadata; `project.toml` with required `name`, immutable `id`, free tags, and no `mode` | design D2b, D27, D32, D33 (§4.5) |
| Tiers | Transient = ephemeral cache (not indexed); Archive; Runtime; one store, one pipeline, two policies that do not overlap: curated in a ragnart project's managed folders, static everywhere else | design D3, D27 (§7) |
| Retrieval | Scope × grounding, defaults `project+strict` / `all+strict`, changes only by the operator; retrieval-only | design D4, D4b (§8) |
| Number of versions / sessions | Six versions; v0.2 takes five sessions (bridge, item 5, A, B, C), v0.3 takes two (A, B), the others one each | operator |
| Version scope | v0.1 spin-off · v0.2 nested tree and data model · v0.3 sources (connectors, the hub and the ingestion side, hub registry, ingestion API) · v0.4 project mode (CLI, skill, session-start hook, capture, collateral) · v0.5 Kotaemon · v0.6 tenancy & authentication | operator |
| Package name, dot-directory | `ragnart`, `.ragnart/` | operator |
| Service host | Server 2 (192.168.1.95), beside ingestion | operator |
| Symbolic links | A filesystem construct, ingested by the filesystem connector's rules as any file is. A symbolic link never creates a reference | design D28 (§4.5, §13.4) |
| Sessions in ragnart projects | Every session is a `global` item of its owner, keyed by its id, ingested as before. A ragnart project includes it by a reference to its id: the session-start hook references a session started in a project folder, and any session can be referenced explicitly. `launch_dir` is recorded verbatim and is metadata only | design D15, D34 (§4.3, §4.5) |
| v0.2 migration mechanism | Payload-only `set_payload` backfill on the live collection; no new collection, no re-embedding | operator; §3 items 1 and 4 |
| Baseline data model | Items outside ragnart projects (today: all items) follow the single-user RAG data model and retrieval semantics, now and later; the ragnart model applies only inside a ragnart project | operator, v0.2 session B; `docs/RAGNART-design.md` §4.6 |
| `global` project_id | Every item carries `project_id`; items outside ragnart projects carry the reserved `global` (one, not per owner); a session enters a ragnart project's scope by reference and keeps `global`; a file enters a ragnart project as a copy | operator, v0.2 session B; `docs/RAGNART-design.md` §4.6 |
| Two access rules | Grants govern items inside ragnart projects (v0.6); the owner rule (owner may read; anyone may read `la-familia` items) governs `global` items, permanently | operator, v0.2 session B; `docs/RAGNART-design.md` §4.6 |
| No root or sessions projects | No root-level projects, no per-owner `sessions` project, no common-ancestor attachment; a crawl root is only a traversal starting point plus the owner mapping | operator, v0.2 session B; `docs/RAGNART-design.md` §4.6 |
| Path selection outside ragnart projects | Not scheduled: a path term in a `<selection>` names ragnart projects only, resolved through `projects.path` | operator, v0.2 session B; `docs/RAGNART-design.md` §4.6, §8.1 |
| Workflow journals | Excluded as orchestration logs, not conversation; subagent and workflow-agent transcripts are keyed `claude-session://<uuid>/agent-<id>` | operator, v0.2 session B; `docs/RAGNART-design.md` §4.2 |
| Duplicate session copies | One session uuid is one item. When several transcript files carry it, the newest copy by modification time (an exact tie goes to the greatest path) is the item's origin; the other copies are duplicate locations and are not indexed | operator, v0.2 session B; `docs/RAGNART-design.md` §4.2, §14 D11 |
| Connectors and sources | A connector is a type; a source is a configured instance of one. Key, locator and display formats are the connector's own, and a key is unique within its source only | operator; design D18 (§4.2, §4.7) |
| The hub and the ingestion side | Each ingestion side owns its bookkeeping and the lifecycle of its items. The hub parses, chunks, embeds, stores and searches; it is the only writer of the index and keeps nothing of an item beyond its chunks. An ingestion side makes five requests per item: upsert, delete, change of key, change of metadata and change of source; the last three rewrite payload fields and re-embed nothing | operator; design D19, D37 (§2.3) |
| Three stores | The Qdrant payload and the hub registry in the hub; a state database on each ingestion host | operator; design D20 (§4.8) |
| Sharing | A ragnart project has one home, on a source RAGNART can write to. Sharing is a grant on a ragnart project homed on a hub source, never a replica. A project id published by a second source is refused and reported | operator; design D21 (§4.5) |
| Write-time gates | The hub checks that a source is known, presents its secret, stays within its own items, and publishes no project id homed elsewhere. One secret per source. Removing a dead source's items is a break-glass operator tool | operator; design D22 (§10.2, §12) |
| Submission form | Bytes for a general document format, parsed by the hub; text for a connector's native format, rendered by the connector | operator; design D23 (§4.7, §5.1) |
| Scope path terms | `<source>:<path>`; the API has no default source; the client's default source is explicit, visible and changeable | operator; design D24 (§8.2, §8.8) |
| Result origin | A hit names `source_id`, key and display label, with no promise that the caller can open the item | operator; design D25 (§8.7) |
| The NAS as a source | One source; its crawl roots are traversal starting points inside it; `global` stays one value | operator; design D26 (§4.6, §4.7) |
| Sequencing of sources | The multi-source data model (v0.3) is built before project mode (v0.4), so that project mode is not built on tables and scope terms that then change | operator |
| `multi_query` / `rag_mmr_bench.py` | Not part of RAGNART | operator |
| Add and references | References by stable id are the rule: a ragnart project references other ragnart projects and sessions; a reference widens scope, never access, and is not transitive between ragnart projects. Files are the exception: an add always copies the file into `resources/`. Both are resources: a reference is a `.reference` file in `resources/`, and `add` is the one verb for both. "Reference" is the only term | operator; design D28, D34 (§4.5, §7.3) |
| Crawl, commits and markers | The global crawl does not enter a project folder. A project's index changes only by `add`, `note`, `commit` and `drop`: there is no implicit commit, and uncommitted changes are shown. No marker changes what is indexed: a marker `init` did not make is refused in a folder with indexed content. A code repository is not a ragnart project | operator; design D39, D40, D41 (§1.2, §4.5, §7.3) |
| Sessions and creation | A ragnart project is created from inside a session by the `/ragnart` skill: `ragnart init`, then the new project becomes the session's current project and the session is added to it. A session works in at most one ragnart project for its whole life; every `ragnart` command acts on the current project only | operator; design D38 (§4.5, §7.3) |
| Marker guards | A registered ragnart project whose marker is malformed keeps its last good record; one whose marker is missing is tombstoned for 24 hours before removal; within one source the registered path keeps a duplicated id | operator; design D29 (§4.5) |
| `init` | `ragnart init <name>` creates the folder `<name>`; the current directory has no meaning. It never changes what is indexed, by construction. Every new ragnart project starts `strict` with capture on. It refuses a folder that already exists, and registers the new one | operator; design D10, D32 (§4.5) |
| Exclusion and projects | No exclusion rule applies inside a ragnart project's managed folders; admission rules do | operator; design D35 (§1.2) |
| `add` and `commit` | Membership by location in `resources/` and `collateral/`. `add` copies and commits, `add --no-commit` only copies, `commit` indexes what is there; commits coalesce per ragnart project; every file gets an outcome | operator; design D36 (§7.3, §10.6) |
| Registration and moves | `init` registers the project folder; `repair` re-registers one moved by hand; managed files are keyed relative to the project; `mv` changes the home source with no re-embedding | operator; design D37 (§4.2, §4.5) |
| Librarian guarantee | The search API returns the evidence and its verdict: hit id, rerank score, count of good hits, reranker status. The client's command prints the status line and the "no good hits" message. Staying within the evidence in free conversation is best-effort. `min_relevance` is applied by the API, with one global value | operator; design D31 (§8.1, §8.7, §9.2) |
| Scope of the design review of 2026-11-16 | It settled D27 to D31 and amended D8, D10, D15 and D19. The order and the scope of the versions in §6 are unchanged | operator |
| Scope of the design discussion of 2026-11-17 | It settled D32 to D42 and amended D2, D2b, D3, D9, D10, D12, D15, D19, D21, D27, D28 and D29. The order of the versions is unchanged; v0.4 grows by references to sessions, registration, `commit`, `mv` and `repair` | operator |

## 3. Current baseline

These facts hold for every version below. They are checked against the
repository and the live services; operational detail is in
`docs/RAG-strategy.md`.

1. **Corpus.** Measured 2026-11-15, after the first sync with the new scanner (which ingested new and changed transcripts): **347,812 points** in **13,393 items**, in one
   collection: dense vector (1024-dim, Cosine, unnamed) plus sparse `sparse_text` (IDF), `on_disk_payload: true`.
   The largest item has 4,559 points; none is above the 5,000-chunk cap. Composition (points / items):

   | Type | Points | Items |
   | --- | ---: | ---: |
   | Claude Code sessions | 100,436 | 4,258 |
   | `.md` | 72,160 | 3,198 |
   | `.html` | 56,249 | 523 |
   | `.pdf` | 53,308 | 1,002 |
   | `.txt` | 18,521 | 1,428 |
   | `.ipynb` | 15,721 | 910 |
   | OpenCode sessions | 14,439 | 433 |
   | `.docx` | 10,173 | 403 |
   | `.pptx` | 3,408 | 438 |
   | `.sh` | 2,648 | 464 |
   | `.jsonl` | 117 | 100 |
   | `.yaml` | 338 | 81 |
   | `.yml` | 294 | 155 |

   Queue: 14,660 `completed`, 268 `ignored`, 0 `pending`. Completed rows without points are consistent:
   they have `chunk_count` 0 (empty documents, no chunks), and reconciliation treats them as consistent. The pre-purge history is in `docs/v0--Implementation-log.md`.
   No migration step may require re-embedding or rewriting vectors.
2. **`documents` is an alias.** `documents` → `documents_new`. Every
   reference to "the `documents` collection" below means the alias; the
   underlying name `documents_new` matters only for snapshot restore
   (`docs/RAG-strategy.md` §4.2 and §9.3).
3. **Eight payload fields are Qdrant-indexed.** Keyword indexes on `source_path`, `project_id`,
   `owner`, `source_type`, `file_type` and `content_hash`; datetime indexes on `modified_at` and
   `ingested_at`. `ensure_payload_indexes()` (`ragnart/collection.py:135`) registers them. The index
   list is `docs/RAGNART-design.md` §4.4.
4. **Point IDs are random; replace deletes by `source_path`.**
   `ragnart/ingest.py:1033` uses `uuid.uuid4()`; a file replace deletes by a
   `source_path` filter (`ragnart/ingest.py:783-835`,
   `utils/rag_sync_nfs.py:90-100`). So `set_payload` filtered by
   `source_path` can add or rewrite payload fields (including the key itself)
   without touching vectors, and a later full re-ingest still replaces cleanly by
   filter.
5. **Owner comes from config, per NFS root.** The scanner assigns each
   configured root's `owner` literal (`ragnart/scanner.py:93-112`);
   `derive_owner()` in `ragnart/schema.py:42` is not called. Search filters
   `owner IN [user, 'la-familia']` only when `user` is supplied
   (`ragnart/search.py:155-160`).
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
   (`documents_new-*.snapshot`, plus `rag_state-*.db`; about 3.4 GB on 2026-11-15: 3.1 GB Qdrant
   snapshot and 258 MB `rag_state.db`) is taken per
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
  never a new collection, never a vector rewrite. A change of embedding model is
  not a migration of this kind: it re-embeds by definition, and its mechanism is
  open (`docs/RAGNART-design.md` §4.1, §13.3).
- **SQLite and Qdrant move together.** `ingestion_queue.file_path` is the
  SQLite primary key. A project or source move updates the SQLite row and
  the Qdrant payload in the same operation (plan-then-apply). Updating only
  one side makes the next scan see delete+new and re-embed — wasteful, not
  corrupting, but it defeats the point of move detection.
- **Schema changes are additive-only, through the ordered `_MIGRATIONS` in `ragnart/state.py`.** Each
  entry is a schema version with its SQL statements. The version is SQLite's `PRAGMA user_version`;
  version 2 is the v0.2 session B model (per-item columns including `origin_path` and `launch_dir`, and
  the `projects`, `project_tags`, `project_links`, `source_links` and `grants` tables). A migration runs
  once, in ascending order; a statement that fails with an `OperationalError` is skipped, so a run that
  crashed part-way can be repeated. Never rename or drop a column, and never change
  `ingestion_queue.file_path`'s role as primary key. One exception, in v0.3: the ragnart project
  tables move to the hub registry (`docs/RAGNART-design.md` §4.8) and are removed from
  `rag_state.db` by a numbered migration, after the registry copy is verified and with a restore
  pair taken first.
- **Search outside ragnart projects keeps today's results.** `GET /v1/search`
  requires `scope`, with no server-side default: a request without it answers
  HTTP 422 (`docs/RAGNART-design.md` §8.8). The `/rag` command copies send
  `scope=all`. A query whose matches are all outside ragnart projects returns
  today's results, except that identical copies now collapse into one hit that
  lists every location (§6 v0.2). The regression
  test is the 14 queries in `utils/rag_bench_queries.txt`, run before and
  after every version's data migration (tie-aware, as search ordering is not
  fully deterministic; `docs/RAG-strategy.md` §6.5).
- **Collection name/alias and `rag_state.db`'s name do not change.**
  `documents` → `documents_new` and the file name `rag_state.db` stay the
  same at every version. The recovery procedures in `docs/RAG-strategy.md`
  §9 refer to both by name.
- **One writer of the index, one host per state database.** The hub is the
  only writer of the index (`docs/RAGNART-design.md` §2.3). A state database is
  opened by its own ingestion host only: `rag_state.db` is opened from Server 2
  only. On Server 2 one writer runs at a time, enforced by the NFS
  `lockf` lock that every writer takes (`docs/RAG-strategy.md` §5.6). The search service opens
  `rag_state.db` read-only, for scope resolution and the project listing, until
  it reads the hub registry (v0.3).
- **Snapshot before scale.** A matching Qdrant snapshot and `rag_state.db`
  copy is taken before any step that touches payload or schema at scale
  (v0.2's backfill, in particular), in the data root of §3 item 7.
- **A source id and a pipeline version are metadata, never a re-key.** Existing
  rows and points receive `source_id`, and existing points `pipeline_version`
  (`p1`), by an additive column and a `set_payload` backfill. No key changes and
  nothing is re-embedded (`docs/RAGNART-design.md` §4.4, §14 D26, D30).

## 5. Access-control model

The model is `docs/RAGNART-design.md` §6: everything is a project; access is
granted on one project at a time to persons and groups, with no inheritance,
because nesting has no meaning; `owner` is the accountable person
(metadata and default grantee), not a filter; grants live in the hub
registry (the state database until v0.3). Filesystem permissions carry no tenancy (§3 item 6).

**The single seam:** `resolve_scope()` in `ragnart/scope.py` is the only
function that turns a request into a Qdrant filter; `authorize(principal,
action, project_id)` is the only function that decides whether a principal may
touch a project.

**What each version adds:**

| Version | Addition |
| --- | --- |
| v0.1 | No seam. `GET /v1/search` applies the owner-only filter (§3 item 5). |
| v0.2 | Projects, `project_tags`, `grants` (shaped for persons and groups, roles `read`/`admin`) and the seam. Items outside ragnart projects (`project_id` `global`) keep the owner rule; the result equals today's owner filter. |
| v0.3 | No change to access: the owner rule and the seam are unchanged. A source's secret gates writes to the index; it is not a principal and gives no read access (`docs/RAGNART-design.md` §12). |
| v0.4 | Project-mode actions (capture, add, commit, drop, note, rm, gc) go through the CLI and the skill, and take the ingestion lock on the ingestion side while they write the state database (`docs/RAGNART-design.md` §10.3, §10.6). A reference widens scope, never access. |
| v0.5 | Kotaemon's login is mapped to a principal; still no authentication. |
| v0.6 | Authentication (per-client tokens first, OAuth with group claims optional); `authorize()` enforces grants; grant management through CLI and API. |

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
   a run that scans only one owner. No live impact while `/mnt/nfs/La-Familia` is
   empty.
   Delivered by the bridge session (owner restriction) and by v0.2 session C
   (the scheduled sync is one call over every configured root, La-Familia
   included; there is no per-owner loop).
   Fix:
   - `find_deleted(existing_paths, owners)` restricts to `WHERE owner IN (...)`
     (the `owner` column already exists); `rag_sync_nfs.py` passes the owners
     whose sources were actually scanned.
   - The scheduled sync covers every configured root in one call (settled at
     kickoff; `scripts/ragnart-ingest-run.sh`).
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

5. **Writers of the index can overlap.** Only the ingestion daemon took
   `rag.ingest_lock`, so a manual sync, a purge, a reset or the snapshot could
   change `rag_state.db` or Qdrant while the daemon or another run wrote
   (`docs/RAGNART-design.md` §10.3, §13.2).
   Delivered by the v0.2 item 5 session (2026-11-15).
   Fix:
   - `ragnart/lock.py` is the one lock for every writer. The sync, the daemon,
     `utils/rag_purge_rescan.py --apply`, `utils/rag_purge_corrupt.py --confirm`,
     `utils/rag_reset.py` without `--dry-run` and `scripts/sqlite-qdrant-snapshot.sh`
     take it before their first write. `scripts/ragnart-ingest-run.sh` holds it
     for the whole run (sync, daemon, WAL checkpoint, snapshot).
   - Contention fails fast: a second writer exits 75 (`EXIT_LOCK_HELD`, sysexits
     `EX_TEMPFAIL`) at once and prints the holder's pid, host, command and start
     time, read from the lock file. It never waits.
   - `rag.ingest_lock` is required in `ragnart.yaml`; there is no host-local fallback.
   Test: `tests/test_lock.py` (contention between two processes, holder line,
   no truncation by a contender, run-wide hold) and the per-writer lock tests.
   Live: with a holder running, the sync, the daemon and the snapshot each exited
   75 naming it; a timer run under the run-wide hold ended `Result=success`.

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
   Delivered by v0.2 session A.
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
  `chunk_count`, `missing_since`, origin details) to `ragnart/state.py`. Delivered by v0.2 session B.
- Add the payload fields and indexes of design §4.4 (`project_id`, `owner`,
  `source_type`, `file_type`, `content_hash` as keywords; `modified_at`,
  `ingested_at` as datetimes) — online and additive; `documents` keeps serving
  search while they build. Delivered by v0.2 session B.
- Re-key Claude Code sessions from their `.jsonl` path to
  `claude-session://<uuid>` (design §4.2), in the state database and the
  payload together; no re-embedding. Claude Code subagent transcripts are separate items keyed
  `claude-session://<session-uuid>/agent-<id>`, with the parent session's `project_id` (design §4.2).
  Delivered by v0.2 session B, including workflow-depth subagent transcripts and exclusion of
  workflow orchestration journals.
- Write a backfill script modelled on `utils/rag_sparse_migrate.py`, but
  doing `set_payload` by `source_path` filter on the live
  `documents` alias — no new collection, no alias swap. Dry-run first;
  resumable (checkpoint by last `source_path` processed); batched. It must
  derive `project_id` and the new metadata the same way the extended scanner
  does. Launch directories are recorded verbatim in `launch_dir` (Claude Code
  `cwd`, OpenCode `location.directory`), never normalised; title and start time
  stay in the payload (`session_title`) and in `first_seen_at`. The backfill also
  seeds `size_bytes` and `content_hash` (MD5) by reading every NAS file and every
  Claude Code transcript once; after that a file is hashed only when its size or
  modification time changes (design §4.2). Delivered by v0.2 session B as
  `utils/rag_metadata_backfill.py`.
- Every item carries a `project_id`: the id of the closest enclosing ragnart
  project, or the reserved `global` value for an item outside any ragnart
  project (`docs/RAGNART-design.md` §4.6, §6.1). There is no root-level project
  and no per-owner `sessions` project; a crawl root is only a traversal starting
  point plus the owner mapping. Delivered by v0.2 session B (items outside ragnart
  projects carry `global`) and v0.2 session C (attachment to ragnart projects).
- Extend the scanner and `rag_sync_nfs.py` with marker discovery
  (`.ragnart/project.toml`: reserved keys `name`, `id`, `inherit_access`, `mode`,
  `scope`, `grounding`, `capture`, `inbox_expiry_days`; every other key is a tag,
  `docs/RAGNART-design.md` §4.5),
  move detection by `content_hash`, tombstones with a grace period, and
  plan-then-apply reconciliation (design §10.2, §10.3), on top of the mount
  guard and missing-sources threshold of fix-first item 1. Delivered by v0.2
  session C, including attachment of every item to its closest enclosing
  ragnart project, re-evaluated on every sync.
- Add `ragnart/scope.py` (`resolve_scope`, `authorize`, per §5), the `scope`
  parameter on `GET /v1/search` (project path, selection by paths and tags,
  `all`; resolved scope echoed in the response; design §8.8), and
  `GET /v1/projects`. `scope` is required on `GET /v1/search`, with no server-side
  default (design §8.8). The two `/rag` command copies (`~/.claude/commands/rag.md`,
  `~/.config/opencode/commands/rag.md`) are updated in the same change.
  Delivered by v0.2 session C. The `scope` terms are `all`, a ragnart project
  path, a path with a trailing `/` (that project and its subtree), and
  `tag:<key>=<value>`; the terms are repeatable and form a union.
- Record each session's launch directory verbatim in `launch_dir`, including a
  stale or deleted directory: never normalised or resolved. A session launched
  outside a ragnart project is an ordinary RAG item
  (`docs/RAGNART-design.md` §4.6). Delivered by v0.2 session C: a session
  attaches to the closest ragnart project whose path is a verbatim prefix of its
  `launch_dir` (`docs/RAGNART-design.md` §4.5). A launch directory reached
  through a symlink does not attach.
- Search: `max_hits_per_source` cap and collapse of identical copies by
  `content_hash` (design §9.1, §8.7), with the cap as a knob in `ragnart.yaml`
  and per request (echoed). Delivered by v0.2 session A (the cap) and v0.2
  session C (collapse: after rerank, before the cap and `top_k`; a collapsed hit
  lists every location; empty-file and NULL hashes never collapse).
- Reconciliation: a scheduled check of `chunk_count` against Qdrant and of
  Qdrant keys against rows; repairs under the deletion rules and fails loudly
  in the log (design §10.3). Delivered by v0.2 session C (`utils/rag_reconcile.py`,
  run at the end of every scheduled ingestion run, under the run-wide lock).
  It resolves these findings (measured results in the Definition of done):
  - **Zombie paths.** Two paths under `Gin-AI/dot.claude-Linux` (an AYA session
    `.jsonl` and `memory/aya-v06a-phased.md`, 307 points) have points but no
    queue row. Rationale: the check compares Qdrant keys with rows, so it must
    report and resolve points without a row.
  - **Completed rows without points.** 1,269 `completed` rows have zero Qdrant
    points and `chunk_count` 0; the check treats that as consistent (empty
    documents, no chunks) and leaves them alone. Rationale: a `completed` row
    with a non-zero `chunk_count` whose points are missing is the divergence
    this check exists to find.
  - **Crash in the purge.** A crash between the Qdrant delete and the row update
    in `utils/rag_purge_rescan.py` leaves a `completed` row without points.
    Rationale: the same divergence, with a known cause.
- Nightly online backup of `opencode.db` to
  `RAGNART--databases/opencode-backups/<hostname>-<ip>/`, installed by
  `scripts/install-units.sh` (design §10.5). Delivered by v0.2 session A.

**Data migration & rollback.**
1. Snapshot pair first: Qdrant snapshot of `documents_new` plus a
   `rag_state.db` copy in the data root of §3 item 7, taken on demand per
   `docs/RAG-strategy.md` §9.5.
2. Pause the ingestion timer (hold its NFS lock) for the backfill's
   duration — the backfill and an ingestion run must not touch the same
   rows concurrently.
3. Apply `_MIGRATIONS` (tables and columns). Delivered by v0.2 session B.
4. Create the payload indexes of `docs/RAGNART-design.md` §4.4 (eight fields).
   Delivered by v0.2 session B.
5. Run the backfill dry-run, inspect the plan, then run it for real,
   batched and resumable. Delivered by v0.2 session B
   (`utils/rag_metadata_backfill.py`).
6. Resume the ingestion timer.

Rollback after the backfill: restore the snapshot pair together, the Qdrant snapshot and the matching
`rag_state.db` copy, per `docs/RAG-strategy.md` §9.3. A SQLite-only rollback (`docs/RAG-strategy.md` §9.4)
is not sufficient: the backfill re-keyed Qdrant payloads (the `source_path` of Claude Code transcripts
moved to their `claude-session://` keys, and the collision repair deleted duplicate points), so an older
`rag_state.db` would no longer match them. The extra payload fields and indexes left in Qdrant are inert
to the old code and are left in place by design, not removed.

**Definition of done.**
- Point count in `documents`. The backfill's payload pass left it unchanged (348,715 before and after).
  Resolving the 46 duplicate-copy collisions (session-uuid rule, `docs/RAGNART-design.md` §14 D11) deleted the colliding keys' points and
  re-ingested 38 sessions once from their newest copy: 348,715 to 347,314 (net -1,401). The first sync with the new scanner then
  added new and changed transcripts: live count 347,812 on 2026-11-15 (§3 item 1). Results in
  `docs/v0--Implementation-log.md`.
- No vectors are rewritten (verified on a sample of points: vectors read
  before and after are identical). Checked by v0.2 session B's live backfill;
  results in `docs/v0--Implementation-log.md`.
- The 14 bench queries return the same results with `scope=all`. Checked by
  v0.2 session B's live backfill; results in `docs/v0--Implementation-log.md`. Measured against a
  temporary restore of the pre-migration snapshot: 12 of 14 queries had identical top-5 source sets;
  the 2 differences were identical-content copies. The pre-migration collection itself varied between
  two runs of the same query (known non-determinism, `docs/RAG-strategy.md` §6.5, which covers rank
  ties and reranker shifts, not set-level variation between runs). Verified
  (session C): against a pre-change baseline captured twice, 12 of 14 queries
  are identical with `scope=all`; the 2 differ only because identical copies
  now collapse into one hit with all locations.
- A scoped search (`scope=<ragnart project path>`) returns only that ragnart
  project's sources, and the response names the ragnart projects searched.
  Verified (session C): `scope=/mnt/nfs/Florian/Gin-AI/projects/RAGNART`
  returned only RAGNART files and sessions (10/10) and echoed the project.
- Moving a ragnart project directory changes one `projects` row and zero Qdrant
  points. Verified (session C, live, on a throwaway ragnart project that was
  renamed): one `projects` row changed (path), item `project_id` unchanged, both
  files detected as moves (`source_path` rewritten, same point ids, no
  re-embedding), total points unchanged; deleting the project removed it and
  tombstoned its 2 items.
- Fix-first item 0 verified; items 1–5 pass their tests.
- Reconciliation reports zero unexplained differences on the clean corpus.
  Verified (session C): the first run filled 433 chunk counts and deleted the 2
  orphaned sources (307 points); after that, zero differences (13,415 ok, 1,269
  ok with zero chunks, 0 anomalies, 0 orphans).
- A result list never contains more than `max_hits_per_source` passages from
  one item. Verified (session C, after collapse): a scoped search returned 3 hits
  from each of two sessions.

**Risks.** Backfill runtime measured: 14,681 rows, 13.5 GB hashed, about 20 minutes for the full pass on Server 2. Re-deriving session `cwd`/`location.directory` at
backfill time may disagree with what a later ingestion run derives if
sessions moved in the meantime. The 10% missing-threshold default may be
wrong for this corpus's real churn rate and needs a dry run before being
trusted.

**Decisions (settled at kickoff).** Scheduled sync covers all configured roots in
one run. Missing-sources threshold: 10% (`rag.sync.max_delete_fraction`). Grace
period before purging a missing source's chunks: 24 hours. Backfill: checkpoint
every 100 items, one `set_payload` per `source_path`, pilot on one subtree first.
Default `max_hits_per_source`: 3. OpenCode backup copies kept: 7.

Kickoff decisions of v0.2 session B (operator, 2026-11-15), each documented in `docs/RAGNART-design.md`:
- Baseline data model: an item outside ragnart projects follows the single-user RAG data model and retrieval semantics, now and later (`docs/RAGNART-design.md` §4.6).
- `project_id` is always present; items outside ragnart projects carry the reserved `global`, with one reserved `projects` row (`docs/RAGNART-design.md` §4.6).
- Two access rules by design: grants govern items inside ragnart projects; the owner rule governs `global` items, permanently (`docs/RAGNART-design.md` §4.6).
- No root-level projects, no per-owner `sessions` project; a crawl root is only a traversal starting point plus the owner mapping; launch directories are recorded verbatim (`docs/RAGNART-design.md` §4.6).
- An item belongs to the closest enclosing ragnart project; a ragnart project's parent is the closest enclosing ragnart project (`docs/RAGNART-design.md` §4.5).
- Path selection outside ragnart projects is not scheduled; a path term in a `<selection>` names ragnart projects only (`docs/RAGNART-design.md` §4.6).
- `origin_path` records where an item's bytes are read: the key for a NAS file, the `.jsonl` path for a Claude Code transcript, `NULL` for OpenCode (`docs/RAGNART-design.md` §4.3).
- Claude Code main transcripts are keyed `claude-session://<uuid>`, subagent and workflow-agent transcripts `claude-session://<uuid>/agent-<id>`; workflow journals are excluded (`docs/RAGNART-design.md` §4.2).
- Session-uuid rule (operator, 2026-11-15, found during the live backfill): one session uuid is one item; the newest copy by modification time wins (an exact tie goes to the greatest path); the other copies are not indexed. The backfill resolves collisions automatically (`docs/RAG-strategy.md` §5.7; `docs/RAGNART-design.md` §4.2, §14 D11).

**Estimated sessions.** 2 (the migration script and the reconciliation/
scoping work are each large enough to warrant a session).
### v0.3 — Sources

**Goal.** Make ingestion multi-point (`docs/RAGNART-design.md` §2.3, §4.7,
§4.8): name the connectors and sources that already exist, give every item and
chunk its source, separate the hub from the ingestion side behind an ingestion
API, and bring up one source on another host. Retrieval for existing callers is
unchanged.

**Entry criteria.** v0.2's Definition of done met. `ragnart-ingest.timer` is
stopped for each migration step.

**Why before project mode.** v0.4 writes project state. Built first, it would
rest on the ragnart project tables in `rag_state.db`, which this version moves
to the hub registry, and on bare path terms, which this version qualifies
by source.

**Work items, session A: one host, no change in behaviour.**
- `source_id` on every queue row (an additive column) and on every point (a
  `set_payload` backfill, with a keyword index), for the three existing
  sources. A restore pair is taken first (§4).
- `pipeline_version` on every point, in the same backfill and with a keyword
  index: every existing point is `p1`. Ingestion stamps it on every chunk it
  writes (design §4.1, §4.4).
- Register the three existing sources: the NAS files (filesystem connector),
  the Claude Code sessions and the OpenCode sessions. Their ids are an operator
  decision (§8 item 16).
- Restate the three scanners and the two session parsers as connectors behind
  the contract of design §4.7: key, locator, display, enumeration with its
  completeness signal, fetch, capabilities and duplicate rule.
- The hub registry: a database of its own, holding the sources and the
  ragnart project tables, where `projects` records each ragnart project's home
  source. Which tables and columns move is a kickoff decision (§8 item 19): the
  design has no use for `source_links` or for the `parent` and `mode` columns
  of `projects`, and the published references cover sessions as well as
  ragnart projects (design §4.5). The sync publishes project records to it. `ragnart/scope.py` and
  `ragnart/service.py` read it. The project tables leave `rag_state.db` (the one
  exception of §4).
- Scope grammar (design §8.8): a path term is `<source>:<path>` or
  `<source>:<path>/`; a path term without a source is HTTP 422; `tag` and
  `name` are reserved and are never source ids; the echo names each project's source. The `/rag` command copies and
  the `utils/` callers send `scope=all` and are unaffected. Any caller that
  sends a path term is updated in the same change.
- Hits carry `source_id` (design §8.7).
- Deleting and replacing an item's points filters on `source_id` and
  `source_path` (design §4.4, §5.4). Reconciliation and the purge tools follow.
- The restore pair includes the registry (design §10.4). The recovery
  procedures of `docs/RAG-strategy.md` §9 follow; that document is the
  operator's, so the edit is proposed, not made.

**Work items, session B: the ingestion API and a source on another host.**
- Ingestion API on the hub (design §12): upsert an item, as bytes with a file
  type or as text; delete an item; change an item's key (a move) and change an
  item's metadata (`project_id`), both as payload rewrites with no content and
  no re-embedding; list the source's own items (key, content hash, chunk count,
  pipeline version); read the status of a submitted item; publish ragnart
  project records; read the current pipeline version, the accepted file types
  and the admission limits.
- Write-time gates and one secret per source (design §14 D22): the source is
  known, a request touches its own items only, and a project id is not homed on
  another source.
- Content in transit is held until its item is ingested, then deleted (design
  §2.3).
- Admission results are returned to the source, which records `ignored` and the
  reason (design §5.2). The source applies the size limit before it submits,
  with the limit the hub publishes.
- The ingestion side as an installable part without docling and without the
  embedder: scanners, `.ragignore`, hashing, sync and plan, state database, and
  the API client.
- The NAS ingestion on Server 2 submits through the same API, so that there is
  one write path.
- A source re-submits its items after a rendering version change, and after a
  change of parsing, admission or chunking: those whose chunks carry an older
  pipeline version in the hub's listing (design §4.1). A change of embedding
  model involves no source.
- A source on another host reconciles against the hub's listing of its items,
  and rebuilds a lost state database from it (design §2.3, §10.3).
- An operator tool on the hub that removes every item of one `source_id`
  (design §10.2): dry-run by default, taking the ingestion lock in write mode,
  as the existing purge tools do.
- One endpoint source on a second host, end to end: a filesystem source and a
  Claude Code source of one owner.

**Data migration & rollback.** Session A: one additive column and one payload
backfill (`source_id`), then the move of the ragnart project tables. Each step
is preceded by a restore pair and can be re-run. The backfill needs no
rollback, because an unread payload field changes no result. The rollback of
the table move is the restore pair. Session B adds no migration to `documents`:
the items of a source on another host are new points, and the per-source tool
removes them.

**Definition of done.**
- Session A:
  - The 14 bench queries return the same results before and after (tie-aware,
    §4).
  - Every point and every completed row has a `source_id`, and for each source
    the point count equals the sum of its rows' `chunk_count`.
  - Every point has `pipeline_version` `p1`.
  - `GET /v1/projects`, and a search with a `<source>:<path>` term, return what
    the bare path returned before. A bare path term is HTTP 422.
  - A scheduled ingestion run completes, including reconciliation and a restore
    pair that holds the registry.
- Session B:
  - An item of the endpoint source is found by a `scope=all` search, with its
    `source_id`. An edit replaces its chunks. A deletion tombstones it and then
    purges it.
  - A file moved on the endpoint source keeps its points: its key is rewritten
    by a request to the hub, and nothing is re-embedded.
  - With the endpoint host switched off, its items are still returned, and no
    run on the hub touches them.
  - A request with a wrong secret, and a request for another source's item, are
    refused.
  - A `project.toml` copied from the NAS to the endpoint is refused and
    reported, and the files under it are indexed as `global` items.
  - The per-source removal tool, in dry-run, lists exactly that source's items.
  - With the NAS ingestion on the API, the 14 bench queries return the same
    results.

**Risks.** Session B moves the NAS ingestion onto a new write path. A
regression there affects the whole corpus, so the bench comparison and one full
scheduled run gate it. Large files are uploaded over whatever link the endpoint
has. The registry and `rag_state.db` could disagree during the table move, so
the move runs with the timer stopped. Until tenancy (v0.6), the secret per
source is the only protection of the write path.

**Decisions to confirm at kickoff.** The ids of the three existing sources
(§8 item 16). The registry's file name and place (§8 item 17). How the
ingestion side is installed and scheduled on the endpoint host. Whether the NAS
ingestion moves onto the API in session B or in a follow-up session, if
session B runs long.

**Estimated sessions.** 2.

### v0.4 — Project mode

**Goal.** Make projects usable as notebooks (`docs/RAGNART-design.md` §4.5,
§7.3 to §7.5, §8): `ragnart init <name>` and the managed folders, references
to ragnart projects and sessions, `add` and `commit`, the `ragnart` CLI as
plumbing, the `/ragnart` skill with the action vocabulary, scope and grounding,
the session-start hook and the status line, ambient capture into the inbox,
keep and drop, collateral, registration, `mv` and `repair`. The Transient tier
is an ephemeral cache of files; nothing in this version creates a second
vector index.

**Entry criteria.** v0.3's Definition of done met.

**Model.** A ragnart project is a notebook: a folder created by
`ragnart init <name>`, whose material is the content of its managed folders
beside `.ragnart/`, and which includes sessions and other ragnart projects by
reference (`docs/RAGNART-design.md` §4.5, §14 D32 to D37). The v0.2 code was
built to an earlier model, so this version changes that behaviour before it
adds to it.

**Work items.**
- Project content and membership (design §4.5), which change v0.2 behaviour:
  - A file belongs to a ragnart project only when it is in that project's
    `resources/` or `collateral/`; `inbox/` holds working material and is
    never indexed. Every other file of the project folder is not indexed. A nested ragnart project has no
    `parent`, and its files are not the outer one's. `mode` and
    `inherit_access` are reserved and ignored.
  - Managed files are keyed `ragnart://<project id>/<relative path>`, with
    their full path as locator (design §4.2).
  - Sessions are `global` items; no rule reads `launch_dir`. The v0.2
    assignment of sessions by `launch_dir` is removed.
  - A registered ragnart project whose marker is malformed keeps its last good
    record. One whose marker is missing is tombstoned (`missing_since`) for the
    grace period. The registered path keeps a duplicated id.
  - The global crawl reads a `.ragnart/` marker and does not enter its
    folder (design §1.2, deviation D). A commit applies the admission rules
    and no exclusion rule. `check-ignore` names the project folder.
  - A marker with an unknown `id` in a folder that holds indexed `global`
    items is refused, logged at ERROR on every run, and walked past as if it
    were not there (design §4.5).
- Registration (design §4.5): `init` registers the project folder with its
  ingestion host; the sync reads the registered folders as well as walking
  the crawl roots; `ragnart repair <path>` re-registers a folder moved by
  hand. A move on one ingestion host rewrites locators only.
- References (design §4.5): a `.reference` file in `resources/` per
  reference, written by `ragnart add` for a session or a ragnart project,
  removed as any resource is; read on each commit and published; `GET /v1/projects` returns each ragnart project's references; a term
  naming a ragnart project includes its referenced sessions, matched by
  `session_id` and resolved before the search (design §8.8); the client
  resolves scope `project` to the current ragnart project and those it
  references. A missing target is reported.
- Names (design §4.5, §8.8): `name` unique across the hub registry without
  regard to case, checked at `init` and at every publication; the scope term
  `name:<name>`, which the client completes from a bare name; `add` accepts a
  ragnart project by name. The status line shows the project's name.
- `.ragnart/provenance/`, one file per copy placed by `add` or `capture`,
  with its origin and time (design §4.5).
- The session's current project (design §4.5, §7.3): session state on the
  client host, keyed by the session id, holding the current project's `id`,
  scope and grounding. Set once, by the session-start hook or by creating a
  ragnart project; never changed.
- Creation from inside a session (design §7.3): the skill checks the gate (no
  current project), runs `ragnart init`, makes the new project current with
  scope `project` and the project's default grounding, and adds the session.
  Every `ragnart` command runs in a subshell in the current project's folder;
  the CLI has no option naming another project. Prerequisite to verify first:
  whether the agent can read its own session id, in Claude Code and OpenCode.
- `add` and `commit` (design §7.3): `add` copies or captures, records the
  provenance, commits and waits; `add --no-commit` only copies; `commit`
  reconciles `resources/` and `collateral/` with the index and waits; `note`
  and `drop` commit.
  There is no implicit commit: the scheduled run commits no ragnart project,
  and the hook and the status line count uncommitted changes. Adding a file
  from the project's own `inbox/` or `collateral/` moves it into
  `resources/`, with its provenance. A commit removes what has left the
  `resources/` and `collateral/` at once, with no tombstone, and runs no deletion phase when
  a folder cannot be read or the `id` does not match. A commit and
  `ragnart status` report stale collateral. Commits coalesce per ragnart project (a
  generation number), and every file gets an outcome: indexed, refused,
  failed or pending. Non-zero exit when a file was refused or failed.
- The concurrency rules R1 to R14 (design §10.6).
- `ragnart` CLI, built on the git mapping (`docs/RAGNART-design.md` §1.2):
  `init`, `init --from`, `add`, `add --no-commit`, `commit`, `rm`
  (appends a pattern to the nearest `.ragignore`; never deletes a file),
  `mv` (a file, or a ragnart project), `repair`, `diff`, `fsck`, `gc`, and
  `capture`, `drop`, `note`, `where`, `expect`, `calibrate`, `grant`,
  `revoke`, `transfer`, which have no git equivalent. Plus the metadata
  listing (`ls` with type and time filters, answered from the state
  database). `status`, `ls-files` and `check-ignore` are not v0.4 work: they
  were delivered with v0.2 item 0. `ls-files` gains the project-scoped
  listing and the inbox listing under `--others`, and `status` the
  project-scoped list of files not indexed (design §7.3, §7.4).
- `init` (design §4.5): it creates the folder `<name>` with `.ragnart/`, the
  managed folders and a `project.toml` with `name`, a new `id`,
  `grounding = "strict"` and `capture = true`; prints them; registers the
  folder; publishes the project record to the hub at once, so the new project is
  known to search immediately. It commits nothing. When the hub cannot be
  reached it still succeeds, says so, and the next commit or sync publishes
  the record. It refuses a folder `<name>` that already exists. It never
  changes what is indexed.
- `ragnart mv` of a ragnart project to another source (design §4.5): copy,
  verify, release the old home, claim the new one; the hub rewrites the
  `source_id` of the managed files' chunks, the fifth request of the
  ingestion API (design §2.3), added to the v0.3 API in this version.
- `inbox/` as the Transient cache (design §7.5): files only, not indexed,
  30-day expiry (`inbox_expiry_days`), `drop` into `.ragnart/dropped/` with a
  grace period, deletion by `gc` in the ingestion run.
- Capture (design §7.3): `ragnart capture <url>` fetches the page itself and
  writes Markdown with front matter to the inbox, with a provenance entry.
- Operator add, keep and note (design §7.3): a file or a folder is always
  copied into `resources/`, wherever the original is, then committed.
- Collateral (design §7.3): written to `collateral/`, indexed with
  `source_type = collateral`; the inbox items it lists are kept.
- `/ragnart` skill for Claude Code and OpenCode: the action vocabulary and the
  capture policy (design §7.3), scope and grounding knobs with defaults at
  three levels (design §8.2), the status line and the "no good hits in this
  scope" stop, both printed by the command from the API's response and not
  composed by the model (design §8.1), one-off `ask`, verbose translation of
  natural-language requests.
  `/rag` becomes an alias (design §8.9).
- Session-start hook and harness status line (design §7.4): the session's
  current project from session state, or, for a session started in a project
  folder, that project made current and the session added to it; and the
  project name, scope, grounding and a capped listing in every session.
- The client's default source (design §8.2): a setting of the `/ragnart`
  skill, shown by `/ragnart status` and changed by `/ragnart source`. The
  skill completes a bare path term with it and shows the completed term.
- Add `.ragnart/` to the global git-ignore file (`core.excludesFile`); add
  `.ragnart/`, `.opencode/` and `.agents/` to `config/ragignore`, so the NAS
  crawl never ingests them (`docs/RAGNART-design.md` §1.2, §4.5).
- The relevance verdict in the search response (design §8.7): a hit id and the
  rerank score on each hit, the count of good hits, and whether reranking ran.
  `min_relevance` is applied by the API to the raw rerank score, with one
  global value. This item depends on neither v0.3 nor the rest of v0.4, and may
  be delivered before them.
- Retrieval calibration (design §9.2, §9.3): `type_weights` (including
  `collateral` and the session types; they change order only) and the fetch
  sizes as knobs at three levels; the labelled set, a plain file that includes
  questions which should end in "no good hits", started from real use and grown
  with `/ragnart expect`; `ragnart calibrate`, which finds `min_relevance` and
  sweeps nothing else.

**Data migration & rollback.** Payload only, after a restore pair (§4).
- The files that today carry a ragnart project's `project_id` because they
  sit under its holding directory return to `global`, by `set_payload` and
  with no re-embedding (today: the files of the RAGNART repository).
- The sessions that today carry a ragnart project's `project_id` return to
  `global` the same way. No reference is written for them: no ragnart project
  is left to hold them.
- The RAGNART repository's hand-written marker is removed, and with it the
  RAGNART ragnart project: a code repository is not a ragnart project (design
  §4.5, D41). The repository's files stay `global` and indexed. A notebook
  about RAGNART, if wanted, is created later with `ragnart init`, and the
  sessions wanted in it are added there.
- No managed files exist yet, so no item is re-keyed to `ragnart://`.
- The cache is plain files; resources and collateral enter through the normal
  ingestion pipeline.

**Definition of done.**
- `ragnart init ACME-Q2`, run in a folder with indexed content, creates
  `ACME-Q2/` with `.ragnart/` and the managed folders, writes `strict` and
  capture on, and leaves the index unchanged. Run again, it refuses.
- In a test project: pages fetched by the agent appear in the inbox listing
  with an expiry date and are not in the index; after "keep" and an operator
  "add", those items are returned by a `project + strict` search with
  citations; a dropped item is restored by a keep within the grace period and
  deleted by `gc` after it.
- A file placed into `resources/` by hand is counted as an uncommitted change
  by the status line, is not indexed by the scheduled run, and is indexed by
  `ragnart commit`. A file that an exclusion rule would match is indexed there
  all the same.
- A marker placed by hand in a folder with indexed content is refused with an
  ERROR, and nothing leaves the index; a marker placed in an empty folder
  makes a ragnart project.
- `ragnart add collateral/<file>` moves a piece of collateral into
  `resources/`.
- Ten adds in quick succession cost at most two commits, and each reports the
  outcome of its own files; a refused file is reported with its reason and
  the command exits non-zero.
- A piece of collateral is returned as derived material, and the inbox items
  it lists are kept.
- The same flow works in Claude Code and OpenCode.
- Every answer shows the status line; scope changes happen only through the
  commands or a stated natural-language translation.
- The files of a project folder outside its managed folders, and its inbox,
  are not indexed: no search returns them.
- A ragnart project nested in another's folder is not covered by a search at
  the outer project's scope.
- A file added to a ragnart project is a copy under its `resources/`, with a
  provenance entry, and the original is untouched.
- A session that creates a ragnart project is added to it and has it as its
  current project; asked to create a second one, the skill refuses. A session
  started in a project folder is added to it and found at its scope; a session referenced explicitly from elsewhere is found too; neither
  is re-ingested.
- After the project folder is renamed, or moved by hand and found with
  `ragnart repair`, its sessions and its references still belong to it, and
  nothing is re-embedded. After `ragnart mv` to another source, the same holds.
- A `project.toml` made invalid leaves the ragnart project, its tags and its
  references unchanged and logs an error. A `project.toml` removed tombstones
  the ragnart project, and restoring it within the grace period clears the
  tombstone.
- A search at scope `project` covers a referenced ragnart project, and does not
  follow that project's own references.
- A question whose answer is not in the ragnart project ends in "no good hits",
  reported by the API.

**Risks.** Capture, the action vocabulary and staying within the evidence under
`strict` depend on the skill being followed; nothing enforces them (design
§8.1). The change of project content alters built v0.2 behaviour: a scoped
search of a ragnart project no longer returns the other files of its folder.
A commit that waits needs the v0.3 split of the hub and the ingestion side;
without it, a commit meets the lock of a scheduled run that parses for hours
(design §7.3). Whether OpenCode loads skills from `~/.claude/skills`, and
whether it has an equivalent of the session-start hook, is unverified (§8).

**Decisions to confirm at kickoff.** The open items of
`docs/RAGNART-design.md` §13.3 that this version depends on: the write path
for project actions from a host other than the ragnart project's ingestion
host; where a session's scope, grounding and default source are stored;
where a session's state lives, which is specific to each harness (design
§13.3).

**Estimated sessions.** 1–2.

### v0.5 — Kotaemon

**Goal.** Give RAGNART a browser UI that reads it through its own index
plugin, for asking across a project's sources with citations, without
driving the CLI.

**Entry criteria.** None strict for the stock-Kotaemon spike, which can be
done any time. The plugin needs v0.2's `scope` parameter and
`GET /v1/projects`, and v0.4's `add`.

**Work items.**
- Spike (any time before this version, if useful): run stock Kotaemon
  against SoHoAI's `/proxy/v1`, confirm one chat turn works; change the
  default `admin` login immediately — LAN-only is not a reason to skip this.
- Write the index plugin: retriever over `GET /v1/search`, project picker
  from `GET /v1/projects` as the scope (sent as source-qualified path terms,
  design §8.8), project and `source_type` shown on
  each citation, librarian (strict) prompt by default (design §8.8). Registers
  through `KH_INDEX_TYPES`/`KH_INDICES` in `flowsettings.py`, modelled on
  Kotaemon's bundled GraphRAG index (`libs/ktem/ktem/index/file/graph`).
- Uploads become resources of the selected project: an operator add, which
  copies into `resources/` and commits (design §7.3, §7.4).
- Map the Kotaemon login to a principal and pass it on every call (§5, v0.5
  row). RAGNART still trusts the passed value until v0.6.
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

### v0.6 — Tenancy & authentication

**Goal.** Make the principal non-spoofable, enforce grants, add grant
management, and validate the two desktop clients end to end.

**Entry criteria.** The `grants` table (in the hub registry since v0.3)
and the `authorize()` seam exist;
v0.4/v0.5 give something worth protecting.

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
- Decide how sources enter the access model (design §13.3): whether access is
  also scoped by source, and how a source's secret relates to a person's
  token. A client token authenticates a reader; a source's secret authorises
  writes for one source. The two stay distinct.
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
"anonymous, no grants" — these are different failure modes; whether access
is scoped by source.

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
ingestion policies (§7.2), the project workflow (§7.3), scope and
grounding (§8.1). A ragnart project is a **project folder**, created by
`ragnart init <name>`. Its **managed folders** sit beside `.ragnart/`:
`inbox/` is the Transient cache, `resources/` holds the project's kept and
added material, and `collateral/` holds the documents produced from it.
`.ragnart/` holds state and metadata only: `project.toml`, `provenance/` and
`dropped/`. A **resource** is a copy or a reference, both in `resources/`; a
reference is a `.reference` file. A session's **current project** is the one
ragnart project it works in (`docs/RAGNART-design.md` §4.5).

Every other file of a project folder is external to the project and is
not indexed: the global crawl does not enter a project folder. "Reference" is the only word for a pointer from a ragnart
project to another ragnart project or to a session: "link" is not used for
it, and a symbolic link is a filesystem construct. A **commit** indexes the
content of a project's `resources/` and `collateral/` (`docs/RAGNART-design.md` §7.3).

A **connector** is a type of store RAGNART can read. A **source** is one
configured instance of a connector, identified by `source_id`. **The hub** is
the pipeline, the index and the search API. The **ingestion side** is an
ingestion host with its sources and its state database. The **hub
registry** holds the sources and the published ragnart project records
(`docs/RAGNART-design.md` §2.3, §4.7, §4.8). In the names `source_path`,
`source_type`, `source_links` and `max_hits_per_source`, "source" refers to an
item: `source_path` is the item's key within its source.

### Future work (unscheduled)

Pipeline and tooling items with no version assigned. Version rule (§2): a
results-changing change of parsing, admission or chunking, or a different
embedding model, is a new pipeline version; fixes and improvements stay `p1`.
A change to retrieval is not a pipeline change: it is gated by the labelled
set (`docs/RAGNART-design.md` §5.5, §9.3).

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
  any value other than the built-in one changes fused ranking: a change to
  retrieval, gated by the labelled set.
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
| 1 | Transient expiry | 30 days | settled |
| 2 | Grace period before purging missing Archive sources | 24 hours | settled |
| 3 | Missing-sources threshold that aborts a sync | 10% | settled |
| 4 | RAGNART service port on Server 2 | 8100 | settled |
| 5 | Token vs OIDC detail (format, issuer, storage) | per-client tokens first, OIDC optional later | v0.6 kickoff |
| 6 | Invalid/missing-token behaviour | not pre-decided — rejected (401) vs anonymous-no-grants | v0.6 kickoff |
| 7 | Docker on Server 1 (for Kotaemon's container) | unverified — check before v0.5 kickoff | v0.5 kickoff |
| 8 | OpenCode skill loading from `~/.claude/skills` | unverified per OpenCode docs — test early | v0.4 kickoff |
| 9 | Claude Desktop Code session transcript location | unverified — needed before the Runtime-tier check can be written | v0.6 kickoff |
| 10 | Backfill batch size / pilot scope (one owner first?) | checkpoint every 100 items; pilot on one subtree first | settled |
| 11 | Notifications (channel, events: failed run, guard tripped, snapshot failed, reconciliation problems) | not scheduled; ntfy on Server 1 available | open |
| 12 | Corpus-hygiene policy: CSV and data files, size and chunk caps, mirrored trees | decided in fix-first item 0 | bridge session |
| 13 | Default `max_hits_per_source` | 3 | settled |
| 14 | OpenCode backup copies kept | 7 | settled |
| 15 | Scheduled sync scope | all configured roots in one run | settled |
| 16 | Ids of the three existing sources (NAS files, Claude Code sessions, OpenCode sessions) | short names chosen by the operator | v0.3 kickoff |
| 17 | Hub registry: file name and place | its own SQLite file in the data root, beside `rag_state.db` | v0.3 kickoff |
| 18 | Whether access is scoped by source | open; part of the tenancy design | v0.6 kickoff |
| 19 | The ragnart project tables when they move to the hub registry: rename `project_links` for references to ragnart projects and sessions, drop `source_links` and the `parent` and `mode` columns of `projects` | rename and drop; the tables are empty and the columns have no use | v0.3 kickoff |
| 20 | Mechanism of an embedding model change: a second vector or a second collection, and the switch between them | open; needed only when a second embedding model is chosen | when needed |

## 9. Version status table

| Version | Status | Session(s) | Commit(s) |
| --- | --- | --- | --- |
| v0.1 Spin-off | done | 1 | RAGNART: `e4de8a3` (import), `1d702fe` (adaptation), `796635f` (comparison tool, pins); SoHoAI: `95d5d0d` (removal, 307 stub) |
| v0.2 Nested | done | 5 | `c41c08c` (items 0-4), `b88d4a4` (item 5), `c4e5616` (session A), `8a0f6b3` (session B), `64472db` (session C phase 1), `c7c4999` (session C phase 2), `49e03de` (session C phase 3) |
| v0.3 Sources | planned | — | — |
| v0.4 Project mode | planned | — | — |
| v0.5 Kotaemon | planned | — | — |
| v0.6 Tenancy & authentication | planned | — | — |

Each version's session updates its own row (`planned` → `in progress` →
`done`) and fills in the session count and commit hash(es) when it finishes.
