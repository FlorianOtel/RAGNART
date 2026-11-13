---
title: "RAGNART — Implementation plan"
created_at: 2026-11-13--08-30
created_by: Claude Code (Claude Opus 5.5)
updated_by: Claude Code (Claude Haiku 5.5, /brain pipeline -- v0.1 close: §6 v0.1 dated note, §7 changelog note, §9 status row, v0.2 entry criterion); Claude Code (Claude Opus 5.5, 2026-11-13 -- post-v0.1 ops: Qdrant local config, daily snapshot retention, data root RAGNART--databases)
updated_at: 2026-11-13--16-15
context: >
  Produced by a /brain session on 2026-11-13, tasked with writing a versioned,
  staged implementation plan for RAGNART — SoHoAI's RAG spun off into its own
  repo and service, then extended into a nested, three-tier (Archive / Runtime
  / Transient) RAG with a self-hosted notebook UI (Kotaemon). Constraints set
  by the operator: stay backward compatible with the existing SoHoAI corpus
  and ingestion pipeline (no re-embedding migration; renames and payload-only
  changes are fine); keep access control open-ended for multi-tenancy from the
  start, enforced through one logical-ACL seam rather than filesystem
  permissions; and keep the work to a few, clearly demarcated sessions — five
  versions, one working session each except v0.2, which may take two. Inputs:
  the handover document
  `/home/florian/Gin-AI/tmp/RAGNART handover a nested, three-tier RAG system
  spun off from SoHoAI.md` (546 lines, checked against SoHoAI commit 5f4f1ed
  on 2026-11-12); the SoHoAI repository itself at the same commit
  (`rag_engine/`, `utils/`, `docs/RAG-strategy.md`, `docs/design-history.md`);
  and a live read of the Qdrant `documents` collection on Server 1
  (192.168.1.93:6333) taken on 2026-11-13, which corrected several of the
  handover's numbers. This document is the sole deliverable of that /brain
  session; no code was moved or written while producing it.
---

# RAGNART — Implementation plan

## 1. Purpose and how to use this plan

This plan turns the handover's build order into five versions, each sized to
one Claude Code `/brain` session (v0.2 may take two). Each version section is
meant to be self-contained: a future session should be able to read this
document and its own version section and start work without re-reading the
handover document in full.

Rules for a version session:

1. Re-read this document in full, especially §2 (Decisions already taken),
   §3 (Corrections to the handover), §4 (Backward-compatibility invariants)
   and §5 (Access-control model) — these apply to every version, not just
   one.
2. Read its own version section in §6 in full.
3. Do its own `/brain` planning for the detailed design. This document gives
   goals, work items and acceptance checks, not file-by-file diffs — that
   level of detail belongs to each version's own planning session.
4. At the end, update its row in §9 (Version status table) and add one dated
   entry to the project changelog (see §7, Docs & git discipline).

This document is not rewritten version to version. Each session appends to
§9 and to the changelog, and may add a dated note to a version's own section
if something planned here turned out to be wrong — reopening a decision gets
a note, not a silent rewrite of history.

## 2. Decisions already taken

| Decision | Choice | Source |
| --- | --- | --- |
| Spin off or evolve SoHoAI in place | Spin off: new repo, new service; all work happens in the RAGNART repo | the handover §Spin-off, not evolution; operator, this session |
| History carried into the new repo | Plain copy ("import from SoHoAI@5f4f1ed" as one commit), not `git filter-repo` | operator, this session |
| Access-control model | Logical ACL (principal / owner / project / grants), enforced only in `resolve_scope()`/`authorize()`; filesystem permissions recorded as hints, never consulted | operator, this session (corroborated by evidence in §3 below) |
| Number of versions / sessions | Five versions, one `/brain` session each; v0.2 may take two | operator, this session |
| Version scope | v0.1 spin-off · v0.2 nested tree · v0.3 transient (merges the handover's stages 2 and 4) · v0.4 Kotaemon · v0.5 tenancy & authentication | operator, this session, reordered from the handover's six-stage build order |
| Package name | `rag_engine` → `ragnart` | the handover §Names |
| Dot-directory name | `.ragnart/` | the handover §`.ragnart/` or `.rag/` |
| Service host | Server 2 (192.168.1.95), beside ingestion | the handover §Where the service runs |
| Symlink rule | Real path decides the project (accepted) | the handover, decision #4 |
| Session attribution | By launch directory (accepted) | the handover, decision #5 |
| Transient visibility | Shared with the project; `created_by` stored for a later private-until-kept switch | the handover, decision #6 |
| v0.2 migration mechanism | Payload-only `set_payload` backfill on the live collection; no new collection, no re-embedding | operator + corrections in §3 |
| `multi_query` / `rag_mmr_bench.py` | Dropped in v0.1, not ported | the handover §Cutover step 6; no-go verdict in SoHoAI `docs/RAG-strategy.md` §8.3 |

## 3. Corrections to the handover

The handover was checked against SoHoAI at commit `5f4f1ed` by reading code
only; it states plainly that "the live NAS data and the running services were
not inspected." This `/brain` session did read the live Qdrant collection and
re-checked several code claims. The following corrections hold for every
version below.

1. **Corpus size.** The handover gives 98,737 points "at Phase 2 completion."
   A live read of `GET /collections/documents` on 192.168.1.93:6333 on
   2026-11-13 shows **2,987,299 points** — dense (1024-dim, Cosine, unnamed
   vector) plus sparse `sparse_text` (IDF), `on_disk_payload: true`. Every
   migration step in this plan is sized for millions of points, not tens of
   thousands: no step may require re-embedding or rewriting vectors.
2. **`documents` is an alias, not a plain collection.** `GET /aliases` on
   2026-11-13 shows `documents` → `documents_new` (also recorded in SoHoAI
   `docs/RAG-strategy.md` §14.4, the sparse-vector migration that created the
   alias). Every reference to "the `documents` collection" below means the
   alias; the underlying name `documents_new` matters only for snapshot
   restore, per SoHoAI `docs/RAG-strategy.md` §11.
3. **`owner` is not Qdrant-indexed today.** `payload_schema` on the live
   collection has only `source_path` (keyword) indexed; `rag_engine/
   collection.py:121` is the only `create_payload_index()` call in the
   codebase. v0.2 is the first version to add indexes, on `owner`,
   `project_id` and `source_id`.
4. **Point IDs are random, not path-derived.** `rag_engine/ingest.py:846`
   uses `uuid.uuid4()`; a file replace deletes by a `source_path` filter
   (`ingest.py:722-735`, `utils/rag_sync_nfs.py:45-67`). The favourable
   consequence: `set_payload` filtered by `source_path` can add or rewrite
   `project_id`/`source_path` without touching vectors, and a later full
   re-ingest still replaces cleanly by filter.
5. **`derive_owner()` is dead code.** Defined in `rag_engine/schema.py:33-61`
   but never called. The real owner assignment is a per-NFS-root config
   literal read in `scanner.py:119-132`; search filters
   `owner IN [user, 'la-familia']` only when `user` is supplied
   (`search.py:104-118`). Treat the handover's references to
   `derive_owner()` as describing intent, not shipped behaviour.
6. **Filesystem permissions cannot carry tenancy.** Both NFS roots
   (`/mnt/nfs/Florian`, `/mnt/nfs/La-Familia`) are `florian:florian`, mode
   `777`; nearly every file is owned by uid 555; there are no per-person Unix
   accounts; the NFS mount is `sec=sys` with no `all_squash`/`anonuid` on the
   client side. This is why §5 below is a logical ACL rather than a mirror of
   `uid`/`gid`/`mode` — those three columns in the handover's `projects`
   table are recorded as hints only and are never consulted by `authorize()`.
7. **Snapshot backup path.** The correct, confirmed path is
   `/mnt/nfs/__Backups/RAGNART--databases/` (`qdrant-snapshots/documents/`,
   paired `documents_new-*.snapshot` files of roughly 21 GB each and
   `rag_state-*.db` copies of roughly 258 MB; written by `scripts/
   sqlite-qdrant-snapshot.sh`). The script's default is `KEEP=3`, but 24
   database copies were present at read time — retention is not being
   applied as documented. Every snapshot reference in this plan uses this
   corrected path.
8. **`main.py`'s RAG surface is larger than "handler plus client."** Besides
   the `/v1/rag/search` handler and the Qdrant client set-up, `main.py:34-36`,
   `100-135` and `185-228` carry config plumbing and the multi-query
   expansion LLM variant. All of it is dropped or moved in v0.1, not only the
   search handler.
9. **Two independent `/rag` client copies exist, not one.**
   `~/.claude/commands/rag.md` is a symlink into
   `/mnt/nfs/Florian/Gin-AI/dot.claude-Linux/commands/rag.md`, and
   `~/.config/opencode/commands/rag.md` is a separate, textually identical
   file. v0.1 must repoint both, plus the `rag` skill entry listed in Claude
   Code ("Search local SoHoAI RAG").

## 4. Backward-compatibility invariants

These are hard rules that every version from v0.2 onward obeys. v0.1 obeys
them trivially, since it moves code and changes no data.

- **No migration re-embeds.** Every schema or layout change after v0.1 is
  additive metadata, applied through `set_payload` (filtered by
  `source_path`, or later by `project_id`/`source_id` once those exist) —
  never a new collection, never a vector rewrite.
- **SQLite and Qdrant move together.** `ingestion_queue.file_path` is the
  SQLite primary key. A project or source move updates the SQLite row and
  the Qdrant payload in the same operation (plan-then-apply). Updating only
  one side makes the next scan see delete+new and re-embed — wasteful, not
  corrupting, but it defeats the point of move detection.
- **Schema changes are additive-only, through `_MIGRATIONS`.** New columns
  and tables go through the existing `_MIGRATIONS` list pattern in
  `rag_engine/state.py` (currently a list of `ALTER TABLE` statements applied
  idempotently, with failures swallowed because the column already exists;
  there is no schema-version table). Never rename or drop a column, and
  never change `ingestion_queue.file_path`'s role as primary key.
- **Search without new parameters is unchanged.** `GET /v1/search` (and its
  SoHoAI-era counterpart `/v1/rag/search`, during the v0.1 transition period)
  called with no `project`, `tiers`, `include_children` or `include_links`
  parameter must return the same ranking it returns today. The regression
  test is the 14 queries in `utils/rag_bench_queries.txt`, run before and
  after every version's data migration.
- **Collection name/alias and `rag_state.db`'s name do not change.**
  `documents` → `documents_new` and the file name `rag_state.db` are
  unchanged at every version. SoHoAI `docs/RAG-strategy.md` §11's recovery
  procedures refer to both by name; that section itself moves (copied, then
  the SoHoAI copy marked superseded) into RAGNART's docs in v0.1.
- **One database host, one ingestion daemon.** `rag_state.db` is opened from
  Server 2 only, and one ingestion daemon runs at a time, enforced by an NFS
  `lockf` lock — both already true in SoHoAI (`docs/RAG-strategy.md` §10.6)
  and carried over unchanged. If the search service ever needs the SQLite
  file directly rather than just Qdrant, it opens it read-only.
- **Snapshot before scale.** A matching Qdrant snapshot and `rag_state.db`
  copy is taken before any step that touches payload or schema at scale
  (v0.2's backfill, in particular), using the corrected path in §3 item 7.

## 5. Access-control model

RAGNART's access control is a **logical ACL**, stored in `rag_state.db` and
enforced at exactly one seam. It is never derived from filesystem ownership
or mode bits.

**Why not filesystem permissions:** both production NFS roots are
`florian:florian`, mode `777`, almost entirely uid 555, with no per-person
Unix accounts (§3 item 6). There is no uid diversity to read a tenant
boundary from.

**Model:**

- **Principal** — whoever is making the request. A `user` value today; a
  validated client identity from v0.5 on.
- **Owner** — the existing, coarser label already in production (`florian`,
  `la-familia`, …), assigned per NFS root at ingest time
  (`scanner.py:119-132`). It stays the outermost filter on every search — a
  principal never sees across owners without a grant.
- **Project** — a node in the project tree (the handover §Nested: the
  project tree), identified by a UUID that never changes; the tree is read
  from `.ragnart/project.toml` markers on disk.
- **Grant** — a `(principal, project_id, role)` row in the `grants` table
  (the handover §Where the tree is stored). Empty through v0.2 and v0.3;
  starts being written in v0.4/v0.5.

**The single seam:** `resolve_scope(principal, project_id=None,
children=True, links=True)` in `ragnart/scope.py` is the only function that
turns a request into a Qdrant filter; `authorize(principal, action,
project_id)` is the only function that decides whether a principal may touch
a project. No other code path builds a scope filter or an allow/deny
decision (the handover §How a search is scoped carries the reference code
sketch).

**What each version adds to the seam:**

| Version | Addition |
| --- | --- |
| v0.1 | No seam yet. Search behaves exactly as `/v1/rag/search` does today (owner-only filter). |
| v0.2 | Seam introduced; `authorize()` implements only today's rule (project owner equals caller owner, or the shared owner). `grants` exists but stays empty. |
| v0.3 | Path confinement on the one write path (`POST /v1/transient`): refuses targets outside `<project>/.ragnart/inbox/` under a configured root. `created_by` starts being recorded on every Transient point. |
| v0.4 | Kotaemon's login name is mapped to an owner, the same way `user` is today. Still no stronger authentication. |
| v0.5 | `user`/principal is no longer self-asserted — per-client API tokens first (OIDC optional). `grants` rows are enforced by `authorize()`. Transient's shared-by-default visibility becomes switchable to private-until-kept, keyed off `created_by`. |

**Hints, not authority:** `uid`, `gid` and `mode` are recorded per project
(the handover's `projects` table) for a possible future permissions-mirroring
mode. No version in this plan reads them inside `authorize()`. Mirroring
filesystem permissions later changes `authorize()` alone — nothing else in
the system needs to know.

## 6. Versions

### v0.1 — Spin-off

**Goal.** Move the already-independent RAG code and docs out of SoHoAI into
the RAGNART repo and a new service, with no semantic change to search. From
here on, all RAG work happens in the RAGNART repo, never in SoHoAI.

**Entry criteria.** This plan approved; SoHoAI confirmed at commit `5f4f1ed`
or later with no further RAG-affecting changes since (check `git log`).

**Work items.**
- Plain-copy `rag_engine/` (renamed to the package `ragnart`), `utils/
  rag_*.py`, `utils/qdrant_status.py`, `utils/rag_bench_queries.txt`,
  `scripts/rag-ingest*`, `scripts/qdrant/`, `scripts/
  sqlite-qdrant-snapshot.sh` into RAGNART as one "import from
  SoHoAI@5f4f1ed" commit. No `git filter-repo`.
- Carry `docs/RAG-strategy.md`, `docs/RAG-troubleshoot.md` and the
  RAG-relevant entries of `docs/design-history.md` into RAGNART's `docs/`,
  completing the operator's metadata frontmatter on arrival. These remain
  the operator's own documents moving home, not new documents — ask before
  any content edit beyond the frontmatter and the path corrections this
  version requires.
- Derive `pyproject.toml` from actual imports, not SoHoAI's flat dependency
  list: docling, tiktoken, fastembed, numpy, qdrant-client, httpx, pyyaml,
  fastapi, uvicorn, python-pptx (implicit).
- Split `ragnart.yaml` out of `SoHoAI-config.yaml` (the `rag`, `users`,
  `shared`, `claude_chats`, `opencode` blocks and `db_base_path`). Switch the
  roughly ten `utils/` config-path-resolution sites (the `Path(__file__)
  .resolve().parent.parent / "SoHoAI-config.yaml"` pattern) to read
  `ragnart.yaml` from the RAGNART repo root.
- Fix the hardcoded SoHoAI path in `rag-ingest.service:10` and
  `rag-ingest-run.sh:17` (and the hardcoded `DB_PATH` at
  `rag-ingest-run.sh:49`) to point at the RAGNART repo.
- Drop `multi_query` and `utils/rag_mmr_bench.py` — already dead per SoHoAI
  `docs/RAG-strategy.md` §8.3's no-go verdict (2026-04-22). Do not port them.
- Write the new FastAPI service: `GET /v1/search` (the handler moved from
  SoHoAI `main.py`) and `GET /health`, under systemd on Server 2, on a free
  port chosen at kickoff (port 8000 belongs to llama-swap there).
- Move the ingestion timer: disable SoHoAI's systemd timer, enable
  RAGNART's. Never both active — the NFS `lockf` guard prevents concurrent
  *runs*, but two active timers still means two schedules writing
  retry/backoff state against one queue.
- Repoint both `/rag` command copies (§3 item 9) at the new service URL.
- SoHoAI-side changes, limited to exactly these three, each in a SoHoAI
  session/commit under SoHoAI's own git discipline: (a) turn
  `/v1/rag/search` in SoHoAI's `main.py` into a redirect to the new service
  for a transition period; (b) mark SoHoAI's copies of the moved RAG docs as
  superseded; (c) at the end of the transition, remove SoHoAI's RAG code
  (the redirect, the leftover Qdrant client set-up, `rag_engine/` and the
  RAG `utils/`/`scripts/`). Step (c) waits for this version's Definition of
  done.

**Data migration & rollback.** None — this version touches no data, only
code location and process ownership. Rollback is re-enabling SoHoAI's timer
and `/v1/rag/search` handler; SoHoAI's RAG code stays in place, untouched,
until the Definition of done is met.

**Definition of done.**
- `utils/rag_smoke_test.py` (ported) passes against the new RAGNART service.
- The 14 queries in `utils/rag_bench_queries.txt` return identical results
  from the old SoHoAI endpoint and the new RAGNART endpoint.
- The ingestion timer runs from RAGNART; SoHoAI's is disabled.
- SoHoAI has zero `rag_engine` imports left.

**Risks.** Dependency drift between SoHoAI's flat list and what RAGNART
actually imports is flagged unclear in the research for this plan and needs
a real import-graph check, not just the list above. Hardcoded paths may
exist beyond the two already found. Doc-only mentions of the old endpoint
exist in octmux and AYA project docs; not executable clients, but check them
too.

**Decisions to confirm at kickoff.** The free port for the new service;
whether the carried follow-ups from SoHoAI `docs/design-history.md` (stale
`ollama_url` Server-1 defaults, unused models in `schemas.py`) are cleaned
up now or in a follow-up.

**Dated note, 2026-11-13.** `docs/Changelog.md` (per Section 7) was not created;
`docs/v0--Implementation-log.md` (AYA format) serves that purpose from v0.1 onward.
The work-item list above named `numpy`; it was dropped after verifying that no direct
import remains once `multi_query.py` and `rag_mmr_bench.py` are dropped. Port chosen: 8100.
The old-vs-new bench check uses a tie-aware gate (operator-approved), because search is
non-deterministic on the old endpoint alone; see `docs/v0--Implementation-log.md`, v0.1
Deviation 6. The SoHoAI `/v1/rag/search` 307 stub is kept until 2026-11-16 (operator
date); check `SoHoAI-gateway.log` for 307 hits before removing it.

**Estimated sessions.** 1.

### v0.2 — Nested

**Goal.** Land the project tree in `rag_state.db` and in the Qdrant payload
by payload-only backfill, with no vectors touched, and give search project
scoping.

**Entry criteria.** v0.1's Definition of done met (RAGNART's service and
timer are live and are the only ones running). **Also:** SoHoAI's
`/v1/rag/search` 307 stub removed (on or after 2026-11-16, after checking
`SoHoAI-gateway.log` for 307 hits), per `docs/v0--Implementation-log.md`, v0.1
Open items.

**Work items.**
- Add tables (`projects`, `project_links`, `source_links`, `grants`) and
  queue columns (`source_id`, `project_id`, `content_hash`, `size_bytes`,
  `missing_since`) to `ragnart/state.py` through the existing `_MIGRATIONS`
  list pattern.
- Add Qdrant keyword payload indexes on `owner`, `project_id`, `source_id`
  (online and additive — `documents` keeps serving search while they build).
- Write a backfill script modelled on SoHoAI's `rag_sparse_migrate.py`
  precedent, but doing `set_payload` by `source_path` filter on the live
  `documents` alias — no new collection, no alias swap. Dry-run first;
  resumable (checkpoint by last `source_path` processed); batched. It must
  re-derive `project_id`/`source_id` the same way the scanner now does, and
  also backfill session attribution (Claude Code `cwd`, OpenCode
  `location.directory`) read fresh at parse time — this data is currently
  parse-time-only and is not persisted (the handover §Runtime tier: one
  change).
- Create an implicit root project per owner, and a per-owner `sessions`
  project, so every file and every session has a `project_id` even with no
  `.ragnart/project.toml` anywhere above it.
- Extend the scanner and `rag_sync_nfs.py` with marker discovery
  (`.ragnart/project.toml`), the plan-then-apply reconciliation (the
  handover §Keeping the tree in sync with the disk, steps 1–5), the mount
  guard, and the missing-sources-threshold abort.
- Add `ragnart/scope.py` (`resolve_scope`, `authorize`, per §5), the
  `project`/`include_children`/`include_links` parameters on `GET
  /v1/search`, and `GET /v1/projects`.
- Attach sessions to projects by launch directory (the handover, decision
  #5, accepted).

**Data migration & rollback.**
1. Snapshot pair first: Qdrant snapshot of `documents_new` plus a
   `rag_state.db` copy, written to `/mnt/nfs/__Backups/RAGNART--databases/`
   (corrected path, §3 item 7), following SoHoAI `docs/RAG-strategy.md`
   §11's pairing method.
2. Pause the ingestion timer (hold its NFS lock) for the backfill's
   duration — the backfill and an ingestion run must not touch the same
   rows concurrently.
3. Apply `_MIGRATIONS` (tables and columns).
4. Create the three payload indexes.
5. Run the backfill dry-run, inspect the plan, then run it for real,
   batched and resumable.
6. Resume the ingestion timer.

Rollback: restore from the snapshot pair per SoHoAI
`docs/RAG-strategy.md` §11.5 (full) or §11.6 (SQLite-only). Because the
backfill never rewrites vectors, a SQLite-only rollback is expected to
suffice; the extra payload fields and indexes left in Qdrant are inert to
the old code and are left in place by design, not removed. A SQLite-only
rollback also reverts the backfill's checkpoint, so a retried backfill
restarts from zero rather than resuming (it is idempotent, so this costs
time, not correctness).

**Definition of done** (the handover §Build order Stage 3, plus the checks
this plan's corrections make necessary).
- Point count in `documents` is unchanged before/after the backfill.
- No vectors are rewritten (verified on a sample of points: vectors read
  before and after are identical).
- The 14 bench queries return the same results with no `project` given.
- A scoped search (`project=<id>`) returns only that project's sources.
- Moving a project directory changes one `projects` row and zero Qdrant
  points.
- The mount guard aborts the sync when a configured root is unmounted.

**Risks.** Backfill runtime against ~2.99M points (§3 item 1) needs
measuring, not assuming. Re-deriving session `cwd`/`location.directory` at
backfill time may disagree with what a later ingestion run derives if
sessions moved in the meantime. The 10% missing-threshold default may be
wrong for this corpus's real churn rate and needs a dry run before being
trusted.

**Decisions to confirm at kickoff.** Missing-sources threshold (10%
suggested); grace period before purge (24h suggested); backfill batch size;
whether to pilot the backfill on one owner's data before the full corpus.

**Estimated sessions.** 2 (the migration script and the reconciliation/
scoping work are each large enough to warrant a session).

### v0.3 — Transient

**Goal.** Ship the `ragnart` CLI, the shared skill, and the real-time
`transient` collection. Merges the handover's stages 2 and 4, because
`ragnart init` needs the project tree v0.2 built in order to attach captures
to a project.

**Entry criteria.** v0.2's Definition of done met.

**Work items.**
- `ragnart` CLI: `where`, `init`, `capture`, `add`, `inbox`, `grep`, `keep`,
  `drop`, `search`, `sweep`, `reindex`.
- Shared skill at `~/.claude/skills/ragnart/SKILL.md` (OpenCode's docs list
  `~/.claude/skills` among its search directories — unverified, see §8)
  plus `/research`, `/inbox`, `/curate` command files for Claude Code
  (`~/.claude/commands/`) and OpenCode (`~/.config/opencode/commands/`).
- Add `.ragnart/` to the global git-ignore file (`core.excludesFile`); add
  `".ragnart/inbox/"`, `".opencode/"`, `".agents/"` to `rag.scanner
  .exclude_dir_names` (multi-component entries are supported per
  `scanner.py:44-51`, but this is code-read-only and unverified at runtime —
  verify it in this version).
- Create the `transient` Qdrant collection with the same vector
  configuration as `documents` (bge-m3 dense plus `sparse_text` sparse).
- `POST`/`DELETE /v1/transient`: synchronous write, no queue, no ingestion
  lock; path-confined to `<project>/.ragnart/inbox/` under a configured
  root; markdown/text indexed immediately, PDF/Office parsed in a
  background task.
- `tiers` parameter and `tier` field on `GET /v1/search`; default
  `archive,runtime`; `transient` included by default once a `project` is
  given.
- Expiry sweep (default 30 days, confirm at kickoff) and promotion removal
  — a kept item's Transient points are removed once the Archive has
  ingested it, so it is never missing from both.

**Data migration & rollback.** None to `documents`. `transient` is a
brand-new, additive collection — rollback is dropping it; it is explicitly
rebuildable from the inbox files, which are the authority.

**Definition of done** (the handover §Build order Stages 2 and 4 combined).
- The same `/research`, `/inbox`, `/curate` flow works in Claude Code and
  OpenCode on one test project.
- A captured page is returned by a scoped search within seconds.
- A dropped item is gone from both disk and the index.
- A kept item is returned from `transient` until the next Archive ingestion
  run has it, and from `documents` afterwards — never from neither.

**Risks.** `POST /v1/transient` is an unauthenticated write path until
v0.5, mitigated only by path confinement and LAN-only deployment — the
handover's own risk list flags this explicitly. Capture depends on the
skill being followed; nothing enforces it. Server 2's 16 GB RAM is shared
with GPU models, now under added PDF-parsing load.

**Decisions to confirm at kickoff.** The 30-day expiry value; whether
`grep`'s ripgrep dependency needs vendoring or can assume ripgrep is already
on every host's `PATH`.

**Estimated sessions.** 1.

### v0.4 — Kotaemon

**Goal.** Give RAGNART a browser UI that reads it through its own index
plugin, for asking across a project's sources with citations, without
driving the CLI.

**Entry criteria.** None strict — the handover marks the stock-Kotaemon
spike as doable any time. The plugin itself needs v0.3's `tiers` parameter
on `GET /v1/search` and v0.2's `GET /v1/projects` to be meaningful.

**Work items.**
- Spike (any time before this version, if useful): run stock Kotaemon
  against SoHoAI's `/proxy/v1`, confirm one chat turn works; change the
  default `admin` login immediately — LAN-only is not a reason to skip this.
- Write the index plugin: retriever over `GET /v1/search`, project picker
  from `GET /v1/projects`, `tier` surfaced on each citation. Registers
  through `KH_INDEX_TYPES`/`KH_INDICES` in `flowsettings.py`, modelled on
  Kotaemon's bundled GraphRAG index (`libs/ktem/ktem/index/file/graph`).
- Route file uploads to `POST /v1/transient`.
- Map the Kotaemon login to an `owner`, the same way `user` is derived
  today, and pass it on every `GET /v1/search` and `GET /v1/projects` call
  (§5, v0.4 row). Kotaemon's login is its own; RAGNART still trusts the
  passed value until v0.5.
- Pin the Kotaemon version used; deploy as one container on Server 1, port
  7860 (NAS mounted read-only if citation previews need source files).

**Data migration & rollback.** None — this version adds a client, not
state.

**Definition of done** (the handover §Build order Stage 5).
- A question asked with a project selected cites only that project's
  sources.
- The top sources match `ragnart search` run with the same query and scope.
- A search issued from a given Kotaemon login is scoped to that login's
  owner (plus the shared owner), and never returns another owner's sources.

**Risks.** Kotaemon's base-class interface for custom index types is
thinly documented, per the handover's own "Not verified" list — the plugin
may need more reverse-engineering than estimated. Version drift on every
Kotaemon upgrade, since the plugin depends on internal classes. Whether
Docker is installed on Server 1 is unverified and blocks the container step
until checked.

**Decisions to confirm at kickoff.** Which Kotaemon version to pin; whether
the NAS read-only mount for citation previews is needed from day one or can
be deferred.

**Estimated sessions.** 1.

### v0.5 — Tenancy & authentication

**Goal.** Make `user`/principal non-spoofable, enforce grants, make
Transient's visibility switchable, and validate the two desktop clients end
to end.

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
- Transient's shared-by-default visibility becomes switchable to
  private-until-kept, keyed off the `created_by` field already recorded
  since v0.3.
- Validate OpenCode Desktop against the Server 2 OpenCode server
  (`192.168.1.95:4096`) — confirm the skill and commands load there (the
  handover flags this as not stated in OpenCode's docs).
- Validate Claude Desktop Code sessions, local and over SSH; confirm where
  their transcripts land relative to `claude_chats.roots` (currently
  `/home/florian/.claude/projects`) — the handover flags this as unverified
  too.

**Data migration & rollback.** None to vector data. This version is
additive application logic over tables that already exist from v0.2.
Rollback is a feature flag / config revert (disable token enforcement, fall
back to the owner-only rule), not a data rollback.

**Definition of done** (the handover §Build order Stage 6, plus the
tenancy checks this version exists for).
- A request without a valid token is rejected, or mapped to an anonymous
  principal with no project access (operator's choice at kickoff, see §8).
- A second principal (for example, a `la-familia`-only token) sees only
  content it has been granted, not the full corpus.
- Transient private-until-kept works once switched on.
- OpenCode Desktop sessions and Claude Desktop Code sessions (local and
  SSH) both appear correctly in the Runtime tier after the next ingestion
  run.

**Risks.** This version turns an until-now-open system into an enforced
one — a bug here fails closed (locks someone out) or fails open (leaks
data); test both directions before calling it done. Desktop-app transcript
locations are unverified per the handover's own list and may require a
short side investigation before the Runtime-tier check can be written.

**Decisions to confirm at kickoff.** Tokens vs OIDC (or both, tokens
first); whether an invalid/missing token means "rejected" (HTTP 401) or
"anonymous, no grants" — these are different failure modes and the handover
does not pick one.

**Estimated sessions.** 1.

## 7. Cross-cutting

### Operations

- **Dated note, 2026-11-13 (after v0.1 close):** the RAG data root moved from
  `/mnt/nfs/__Backups/SoHoAI--databases/` to `/mnt/nfs/__Backups/RAGNART--databases/`
  (`sqlite/`, `logs/`, `qdrant-snapshots/`, `rag-ingest.lock`); only SoHoAI's own
  `archive/` and gateway log stay in the old directory. Snapshot retention is now one
  pair per day, 7 kept on the NAS; Qdrant's snapshots_path is Server 1's local NVMe.
  Paths below are updated to the new root. Details: `docs/RAG-strategy.md` §11.2a and
  `docs/v0--Implementation-log.md`, v0.1 ops entry of 2026-11-13.
- Snapshot script: `scripts/sqlite-qdrant-snapshot.sh` (moves to RAGNART in
  v0.1), writing to `/mnt/nfs/__Backups/RAGNART--databases/
  qdrant-snapshots/documents/` (corrected path, §3 item 7) — paired
  `documents_new-*.snapshot` and `rag_state-*.db` files. The SQLite copy is
  taken after the snapshot download, not atomically with it; a "matching"
  pair is nearest-timestamp, not guaranteed-identical content, same as
  today.
- Recovery: SoHoAI `docs/RAG-strategy.md` §11's full (§11.5) and
  SQLite-only (§11.6) rollback procedures move into RAGNART's docs in
  v0.1 (copied, then SoHoAI's copy marked superseded, not deleted, until
  v0.1's SoHoAI-side cleanup commit).
- The collection name/alias (`documents` → `documents_new`) is unchanged
  across every version (§4).

### Docs & git discipline

- Each version's session updates RAGNART's docs before its commit, per the
  operator's global git-discipline rule.
- A changelog document (`docs/Changelog.md`, created in v0.1 alongside the
  carried-over RAG-strategy and RAG-troubleshoot docs) gets one dated entry
  per version session, naming the commit(s) and the Definition-of-done
  checks that passed.
  **2026-11-13:** `docs/Changelog.md` not created — see the dated note under §6 v0.1; `docs/v0--Implementation-log.md` is the changelog from here on.
- This document is itself updated at the end of each version: the
  version's row in §9, and a dated note in §6 for anything that changed
  during the work.
- Carried-over docs (`RAG-strategy.md`, `RAG-troubleshoot.md`, the RAG
  entries of `design-history.md`) get the operator's metadata frontmatter
  completed on arrival in v0.1, and an `updated_by`/`updated_at` pair once
  their content is edited for the new location (path corrections,
  SoHoAI-specific framing removed). Ask before any edit beyond what v0.1's
  work items require — these remain the operator's own documents even as
  they change repos.

### Naming (adapted from the handover §Names)

| Was (SoHoAI) | Becomes (RAGNART) | Changes in |
| --- | --- | --- |
| `.sohoai/` (earlier SoHoAI handovers' marker name) | `.ragnart/` | v0.2 |
| `rag_engine` package | `ragnart` package | v0.1 |
| RAG blocks of `SoHoAI-config.yaml` | `ragnart.yaml` | v0.1 |
| `GET /v1/rag/search` | `GET /v1/search` | v0.1 |
| `workbench` collection (the handover's working name) | `transient` collection | v0.3 |
| `documents` collection/alias | unchanged | — |
| `rag_state.db` | unchanged | — |
| `inbox/`, `sources/`, `notes/` | unchanged in name | introduced under `.ragnart/` in v0.3 |
| Corpus tier / Session tier / Workbench tier (the handover's working names) | Archive / Runtime / Transient | v0.1 (naming only — Archive and Runtime exist already) |

## 8. Open decisions and parameters to confirm

| # | Item | Suggested default | Confirm at |
| --- | --- | --- | --- |
| 1 | Transient expiry | 30 days | v0.3 kickoff |
| 2 | Grace period before purging missing Archive sources | 24 hours | v0.2 kickoff |
| 3 | Missing-sources threshold that aborts a sync | 10% | v0.2 kickoff |
| 4 | RAGNART service port on Server 2 | any free port, not 8000 | v0.1 kickoff |
| 5 | Token vs OIDC detail (format, issuer, storage) | per-client tokens first, OIDC optional later | v0.5 kickoff |
| 6 | Invalid/missing-token behaviour | not pre-decided — rejected (401) vs anonymous-no-grants | v0.5 kickoff |
| 7 | Docker on Server 1 (for Kotaemon's container) | unverified — check before v0.4 kickoff | v0.4 kickoff |
| 8 | OpenCode skill loading from `~/.claude/skills` | unverified per OpenCode docs — test early | v0.3 kickoff |
| 9 | Claude Desktop Code session transcript location | unverified — needed before the Runtime-tier check can be written | v0.5 kickoff |
| 10 | Backfill batch size / pilot scope (one owner first?) | not pre-decided | v0.2 kickoff |

## 9. Version status table

| Version | Status | Session(s) | Commit(s) |
| --- | --- | --- | --- |
| v0.1 Spin-off | done | 1 | RAGNART: `e4de8a3` (import), `1d702fe` (adaptation), `796635f` (comparison tool, pins); SoHoAI: `95d5d0d` (removal, 307 stub) |
| v0.2 Nested | planned | — | — |
| v0.3 Transient | planned | — | — |
| v0.4 Kotaemon | planned | — | — |
| v0.5 Tenancy & authentication | planned | — | — |

Each version's session updates its own row (`planned` → `in progress` →
`done`) and fills in the session count and commit hash(es) when it finishes.
