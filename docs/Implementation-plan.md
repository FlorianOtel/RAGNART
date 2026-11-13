---
title: "RAGNART — Implementation plan"
created_at: 2026-11-13--08-30
created_by: Claude Code (Claude Opus 5.5)
updated_by: Claude Code (Claude Haiku 5.5, /brain pipeline -- v0.1 close: §6 v0.1 dated note, §7 changelog note, §9 status row, v0.2 entry criterion); Claude Code (Claude Opus 5.5, 2026-11-13 -- post-v0.1 ops: Qdrant local config, daily snapshot retention, data root RAGNART--databases); Claude Code (Claude Opus 5.5, 2026-11-13 -- as-is pass: current-state only); Claude Code (Claude Opus 5.5, 2026-11-13 -- v0.2 fix-first items)
updated_at: 2026-11-13--17-16
context: >
  Versioned, staged implementation plan for RAGNART, a self-hosted RAG
  retrieval service, built out into a nested, three-tier (Archive / Runtime /
  Transient) RAG with a self-hosted notebook UI (Kotaemon). Operator
  constraints: backward compatible with the existing corpus and ingestion
  pipeline (no re-embedding migration; renames and payload-only changes are
  fine); access control open-ended for multi-tenancy, enforced through one
  logical-ACL seam rather than filesystem permissions; the work kept to five
  versions, one working session each except v0.2, which may take two. The
  design input is the handover document
  `/home/florian/Gin-AI/tmp/RAGNART handover a nested, three-tier RAG system
  spun off from SoHoAI.md`, cited below as "the handover". The current
  baseline (§3) is checked against the repository and the live Qdrant
  collection on Server 1 (192.168.1.93:6333); operational detail lives in
  docs/RAG-strategy.md. This document states the plan as it currently stands.
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
| Where RAG work happens | The RAGNART repo and service only; SoHoAI carries no RAG code | the handover §Spin-off, not evolution; operator |
| Access-control model | Logical ACL (principal / owner / project / grants), enforced only in `resolve_scope()`/`authorize()`; filesystem permissions recorded as hints, never consulted | operator (corroborated by §3 item 6) |
| Number of versions / sessions | Five versions, one `/brain` session each; v0.2 may take two | operator |
| Version scope | v0.1 spin-off · v0.2 nested tree · v0.3 transient (merges the handover's stages 2 and 4) · v0.4 Kotaemon · v0.5 tenancy & authentication | operator, reordered from the handover's six-stage build order |
| Package name | `ragnart` | the handover §Names |
| Dot-directory name | `.ragnart/` | the handover §`.ragnart/` or `.rag/` |
| Service host | Server 2 (192.168.1.95), beside ingestion | the handover §Where the service runs |
| Symlink rule | Real path decides the project (accepted) | the handover, decision #4 |
| Session attribution | By launch directory (accepted) | the handover, decision #5 |
| Transient visibility | Shared with the project; `created_by` stored for a later private-until-kept switch | the handover, decision #6 |
| v0.2 migration mechanism | Payload-only `set_payload` backfill on the live collection; no new collection, no re-embedding | operator; §3 items 1 and 4 |
| `multi_query` / `rag_mmr_bench.py` | Not part of RAGNART | the handover §Cutover step 6 |

## 3. Current baseline

These facts hold for every version below. They are checked against the
repository and the live services; operational detail is in
`docs/RAG-strategy.md`.

1. **Corpus.** About **2.99M points** in one collection: dense vector
   (1024-dim, Cosine, unnamed) plus sparse `sparse_text` (IDF),
   `on_disk_payload: true`. Every migration step in this plan is sized for
   millions of points: no step may require re-embedding or rewriting vectors.
2. **`documents` is an alias.** `documents` → `documents_new`. Every
   reference to "the `documents` collection" below means the alias; the
   underlying name `documents_new` matters only for snapshot restore
   (`docs/RAG-strategy.md` §4.2 and §9.3).
3. **Only `source_path` is Qdrant-indexed.** The live `payload_schema` has
   one keyword index, `source_path`; `ragnart/collection.py:121` is the only
   `create_payload_index()` call. v0.2 adds indexes on `owner`, `project_id`
   and `source_id`.
4. **Point IDs are random; replace deletes by `source_path`.**
   `ragnart/ingest.py:846` uses `uuid.uuid4()`; a file replace deletes by a
   `source_path` filter (`ragnart/ingest.py:722-735`,
   `utils/rag_sync_nfs.py:45-67`). So `set_payload` filtered by
   `source_path` can add or rewrite `project_id`/`source_id` without touching
   vectors, and a later full re-ingest still replaces cleanly by filter.
5. **Owner comes from config, per NFS root.** The scanner assigns each
   configured root's `owner` literal (`ragnart/scanner.py:119-132`);
   `derive_owner()` in `ragnart/schema.py:33` is not called. Search filters
   `owner IN [user, 'la-familia']` only when `user` is supplied
   (`ragnart/search.py:104-110`). The handover's references to
   `derive_owner()` describe intent, not behaviour.
6. **Filesystem permissions cannot carry tenancy.** Both NFS roots
   (`/mnt/nfs/Florian`, `/mnt/nfs/La-Familia`) are `florian:florian`, mode
   `777`; nearly every file is owned by uid 555; there are no per-person Unix
   accounts; the NFS mount is `sec=sys` with no `all_squash`/`anonuid` on the
   client side. Hence §5 is a logical ACL rather than a mirror of
   `uid`/`gid`/`mode` — those three columns in the handover's `projects`
   table are recorded as hints only and never consulted by `authorize()`.
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
  `source_path`, or later by `project_id`/`source_id` once those exist) —
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
- **Search without new parameters is unchanged.** `GET /v1/search` called
  with no `project`, `tiers`, `include_children` or `include_links`
  parameter must return the same ranking it returns today. The regression
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
  (`ragnart/scanner.py:119-132`). It stays the outermost filter on every search — a
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
| v0.1 | No seam. `GET /v1/search` applies the owner-only filter (§3 item 5). |
| v0.2 | Seam introduced; `authorize()` implements only today's rule (project owner equals caller owner, or the shared owner). `grants` exists but stays empty. |
| v0.3 | Path confinement on the one write path (`POST /v1/transient`): refuses targets outside `<project>/.ragnart/inbox/` under a configured root. `created_by` starts being recorded on every Transient point. |
| v0.4 | Kotaemon's login name is mapped to an owner, the same way `user` is today. Still no stronger authentication. |
| v0.5 | `user`/principal comes from a validated per-client API token (OIDC optional), not from the caller's own parameter. `grants` rows are enforced by `authorize()`. Transient's shared-by-default visibility becomes switchable to private-until-kept, keyed off `created_by`. |

**Hints, not authority:** `uid`, `gid` and `mode` are recorded per project
(the handover's `projects` table) for a possible future permissions-mirroring
mode. No version in this plan reads them inside `authorize()`. Mirroring
filesystem permissions later changes `authorize()` alone — nothing else in
the system needs to know.

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
hits and repointing any caller found.

**Fix first (before any schema or backfill work).** The backfill and the
move detection below rely on the sync never deleting wrongly, so these four
defects are fixed, tested and run once by hand (log inspected) before the
timer picks up the new sync. None of them changes Qdrant data or the schema.

1. **An unreachable NFS root deletes its whole tree from the index (critical).**
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
   Fix:
   - The client is always created at start-up; failures surface per request.
   - `/health` checks Qdrant's `/healthz` with a 2 s timeout and answers
     `{"status":"degraded","qdrant":false}` immediately when it fails.
   Test: Qdrant stopped -> `/health` answers `degraded` within 3 s; Qdrant started
   -> `/v1/search` answers 200 without restarting `ragnart-search.service`.

**Work items.**
- Add tables (`projects`, `project_links`, `source_links`, `grants`) and
  queue columns (`source_id`, `project_id`, `content_hash`, `size_bytes`,
  `missing_since`) to `ragnart/state.py` through the existing `_MIGRATIONS`
  list pattern.
- Add Qdrant keyword payload indexes on `owner`, `project_id`, `source_id`
  (online and additive — `documents` keeps serving search while they build).
- Write a backfill script modelled on `utils/rag_sparse_migrate.py`, but
  doing `set_payload` by `source_path` filter on the live
  `documents` alias — no new collection, no alias swap. Dry-run first;
  resumable (checkpoint by last `source_path` processed); batched. It must
  re-derive `project_id`/`source_id` the same way the extended scanner does, and
  also backfill session attribution (Claude Code `cwd`, OpenCode
  `location.directory`) read fresh at parse time — this data is currently
  parse-time-only and is not persisted (the handover §Runtime tier: one
  change).
- Create an implicit root project per owner, and a per-owner `sessions`
  project, so every file and every session has a `project_id` even with no
  `.ragnart/project.toml` anywhere above it.
- Extend the scanner and `rag_sync_nfs.py` with marker discovery
  (`.ragnart/project.toml`), the plan-then-apply reconciliation (the
  handover §Keeping the tree in sync with the disk, steps 1–5), on top of the
  mount guard and missing-sources threshold of fix-first item 1.
- Add `ragnart/scope.py` (`resolve_scope`, `authorize`, per §5), the
  `project`/`include_children`/`include_links` parameters on `GET
  /v1/search`, and `GET /v1/projects`.
- Attach sessions to projects by launch directory (the handover, decision
  #5, accepted).

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

**Definition of done** (the handover §Build order Stage 3, plus the checks
the baseline in §3 makes necessary).
- Point count in `documents` is unchanged before/after the backfill.
- No vectors are rewritten (verified on a sample of points: vectors read
  before and after are identical).
- The 14 bench queries return the same results with no `project` given.
- A scoped search (`project=<id>`) returns only that project's sources.
- Moving a project directory changes one `projects` row and zero Qdrant
  points.
- Fix-first items 1–4 pass their tests.

**Risks.** Backfill runtime against ~2.99M points (§3 item 1) needs
measuring, not assuming. Re-deriving session `cwd`/`location.directory` at
backfill time may disagree with what a later ingestion run derives if
sessions moved in the meantime. The 10% missing-threshold default may be
wrong for this corpus's real churn rate and needs a dry run before being
trusted.

**Decisions to confirm at kickoff.** Scheduled sync scope: all configured
roots in one run (recommended) or `la-familia` added to `RAG_SYNC_USERS`;
missing-sources threshold (10% suggested); grace period before purge (24h suggested); backfill batch size;
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
  `ragnart/scanner.py:44-51`, but this is code-read-only and unverified at runtime —
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

### Naming (adapted from the handover §Names)

| Handover term | RAGNART name | Introduced in |
| --- | --- | --- |
| `.sohoai/` (marker name in earlier handovers) | `.ragnart/` | v0.2 |
| `workbench` collection | `transient` collection | v0.3 |
| `inbox/`, `sources/`, `notes/` | same names, under `.ragnart/` | v0.3 |
| Corpus tier / Session tier / Workbench tier | Archive / Runtime / Transient | Archive and Runtime exist; Transient in v0.3 |

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
