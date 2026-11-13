---
title: "RAGNART -- v0 implementation log"
created_at: 2026-11-13--14-04
created_by: Claude Code (/brain pipeline -- Claude Opus 5.5 orchestrating, Claude Haiku 5.5 implementing)
updated_by: Claude Code (Claude Haiku 5.5, /brain pipeline -- v0.1 post-review doc fixes); Claude Code (Claude Opus 5.5, 2026-11-13 -- post-v0.1 ops: Qdrant local config, daily snapshot retention, data root RAGNART--databases); Claude Code (Claude Opus 5.5, 2026-11-13 -- section pointers updated after the as-is rewrite); Claude Code (Claude Opus 5.5, 2026-11-14 -- design review session entry, v0.2 handover)
updated_at: 2026-11-14--00-27
context: >
  Per-session log for RAGNART's v0 train (v0.1 spin-off through v0.5 tenancy),
  mirroring AYA's docs/v0--Implementation-log.md format. The plan is
  docs/Implementation-plan.md; this log is the narrative record of how each
  version was actually reached.
---

# RAGNART -- v0 implementation log

## 0. How this document works

**Reverse chronological throughout. Newest on top, at every level.**

- One `##` section per version (`v0.1`, `v0.2`, ...). The newest version sits above
  the older ones.
- One `###` subsection per coding session, titled with the session timestamp in
  `YYYY-MM-DD--HH-MM` format.
- Each session subsection carries **What was done** (with commit hashes),
  **Deviations from the plan**, and -- for a session that closes a version --
  **Decisions**, **Evidence**, **Open items** and a **Handover** block.

**Citation convention.** A bare "Section x" means this document. `Plan Section x` is
`docs/Implementation-plan.md`. The v0.1 session that wrote this entry is the only
session of v0.1; `Plan Section 6 v0.1` means the v0.1 work-item list in that document.

**Log, not status.** The status table is `docs/Implementation-plan.md` Section 9. This
log records what happened, including what diverged.

---

## v0.1 -- Spin-off

Plan: `docs/Implementation-plan.md` Section 6, v0.1. Status: **done** (see Plan Section 9).

### 2026-11-14--00-30 -- design review, design document, as-is docs, corpus analysis, v0.2 handover

Intermediate session between v0.1 and v0.2, run directly by Claude Opus 5.5 (with subagents
for the doc rewrites and independent reviews). Operator-led design review of RAGNART before
v0.2, producing `docs/RAGNART-design.md`.

#### What was done

| Commit | Repo | Subject |
| --- | --- | --- |
| `fd07805` | RAGNART | docs: describe the current state only (as-is rewrite) |
| `938fe7e` | RAGNART | plan: v0.2 fix-first items (NFS root guard, --user scope, alias, health) |
| `d44cc41` | RAGNART | docs: RAGNART design document; plan aligned with it |
| `f384b20` | RAGNART | README: roadmap and tiers aligned with the design document |

The commit carrying this entry also adds v0.2 fix-first item 0 and the version assignments
below to `docs/Implementation-plan.md` and the matching status markers to
`docs/RAGNART-design.md`.

1. **As-is documentation rule** (operator): descriptive docs describe only the current state;
   history lives in git and in this log. `docs/RAG-strategy.md` rewritten as a current-state
   reference (3,119 -> ~1,010 lines), `docs/RAG-troubleshoot.md` as a symptom/check/fix guide
   (1,661 -> ~712), plan and README given an as-is pass. Independent review: FIX, one item, fixed.
   Rule recorded in project memory; README keeps one origin reference; log files are exempt.
2. **Defects found in the code** while rewriting, verified and written into the plan as v0.2
   fix-first items 1-4: an unreachable NFS root deletes its tree; a `--user` sync deletes other
   owners' rows; `rag_status.py`/`rag_reset.py` ignore the `documents` alias; `/health` blocks
   and search stays 503 after Qdrant was down at start.
3. **Design review** (topics worked in order, decisions by the operator): identity (natural
   key + MD5 attribute, no surrogate id); schema version vs pipeline version, `p1` defined
   explicitly; payload metadata and filters, collapse of identical copies; everything is a
   project, grants to persons and groups, inheritance opt-out, roles read/admin, sharing TBD;
   projects addressed by path with immutable `id` and free tags; tiers by lifecycle with
   Transient as an ephemeral, non-indexed cache; project mode (gather -> compact -> references,
   `MEMORY.md` index); retrieval as scope x grounding with explicit-only changes; retrieval-only;
   predictable-not-rigid quality with calibration knobs; failure and consistency rules;
   `opencode.db` nightly backup. Full register: `docs/RAGNART-design.md` §14.
4. **Corpus analysis** (operator suspected pollution): facet over `source_path` plus file sizes.
   Of 2,988,440 points, 1,243,155 (41.6%) come from 211 PDF/DOCX read as raw bytes, 987,376
   (33.0%) from 15 very large text files (P4OC R8 mapping files, VPN logs), 362,643 (12.1%) from
   23 CSV exports; the real corpus is ~395k points. Root causes in `ragnart/ingest.py`: raw-read
   fallback on docling failure; quality gate defeated by U+FFFD counting as printable; no size or
   chunk caps; gate applied only at ingestion; static policy takes every txt/csv. Cleanup and
   `p2` handed to a separate operator session (the bridge to v0.2).
5. **Design document** `docs/RAGNART-design.md` written, independently reviewed (FIX: four
   present-tense statements about unbuilt parts, a table residual; all fixed with a status
   table and per-section status lines). Plan and README aligned with it; the handover in
   `~/Gin-AI/tmp` is no longer referenced.

#### Deviations from the plan

1. v0.3 changed from "Transient" (real-time `transient` collection, `/v1/transient`) to
   "Project mode" (CLI, `/ragnart` skill, compact, `MEMORY.md`, ingest-now; no second vector
   index), per design decision D3.
2. v0.2 drops `source_id` and gains: schema version, design §4.3/§4.4 metadata and indexes,
   re-keying of Claude Code sessions to `claude-session://<uuid>`, the `scope` parameter,
   `max_hits_per_source` and collapse, reconciliation, the `opencode.db` backup, and fix-first
   item 0 (corpus hygiene, delivered by the bridge session).
3. v0.3 gains retrieval calibration (`ragnart calibrate`, labelled set).

#### Evidence

- 19:00 ingestion run (2026-11-13): sync 1.5 min, 15 files ingested, 0 failed; snapshot
  correctly skipped ("newest pair is younger than 20 h"). The deleted
  `OpenFlow-Book-Preview.pdf` (105,116 points) was removed from disk at 00:09 on 2026-11-14,
  after that run; its points are removed by the 01:00 run.
- Corpus at the end of the session: 2,988,440 points (cleanup not yet started).

#### Handover to v0.2

- **Tree state:** RAGNART HEAD = the commit carrying this entry (parent `f384b20`); SoHoAI HEAD
  `95d5d0d`. Clean tree.
- **Read first:** `docs/RAGNART-design.md` §1.1 (what is built vs planned) and §14 (decision
  register); then `docs/Implementation-plan.md` §2–§5 and §6 v0.2 in full.
- **Prerequisites (entry criteria):** fix-first item 0 delivered by the bridge session and
  verified (point count and composition, `p2` in code and described in design §5, plan §3
  refreshed); SoHoAI `/v1/rag/search` 307 stub removed on or after 2026-11-16 after checking
  `SoHoAI-gateway.log` for 307 hits.
- **Decisions to take at kickoff:** plan §6 v0.2 "Decisions to confirm at kickoff" (sync scope,
  missing-sources threshold, grace period, backfill batch size and pilot, `max_hits_per_source`
  default, OpenCode backup retention).
- **Open design topics not needed for v0.2:** Transient lifecycle (before v0.3), interfaces
  (API versioning, CLI/skill/API split, Kotaemon fit), notifications, sharing and ownership
  transfer, Qdrant placement after the cleanup.
- **Environment:** Qdrant on `.93` (config `/etc/qdrant/config.yaml`, wired 1 Gb/s), search and
  ingestion on `.95`, data root `/mnt/nfs/__Backups/RAGNART--databases/`; privileged commands via
  `sudo -n` or `ssh root@localhost` (`.95`) and `ssh 192.168.1.93` + `sudo -n` (`.93`).

### 2026-11-13--16-15 -- post-close ops: Qdrant config, snapshot retention, data root, Server 1 back on wired

Intermediate session between v0.1 and v0.2 (operator-requested re-evaluation). Run directly by
Claude Opus 5.5, not as a `/brain` pipeline. Not a change to v0.1's scope; it fixes defects found
after the close and sets the snapshot policy.

#### What was done

| Commit | Repo | Subject |
| --- | --- | --- |
| `6c30d2a` | RAGNART | ops: Qdrant local config, daily snapshot retention, data root RAGNART--databases |

1. **Server 1 (`.93`) back on wired Ethernet.** Root cause of the snapshot slowdown, confirmed from
   three sources:
   - `.93` journal: on 2026-11-08 at 09:27:44 CET the Wi-Fi profile `inet5` was created, and at
     09:28:14 NetworkManager made it the default route. `eno1` had no carrier from the ~09:31
     reboot until 2026-11-13 15:13:32.
   - AYA `docs/v2--Implementation-log.md`, entry 2026-11-08--13-30: "`.93` moved to Wi-Fi on the
     operator's desk" (um690 became the Windows 11 VM host for AYA v2.4).
   - Snapshot log: about 530 runs (May to 2026-11-08 07:11 CET) downloaded in 1-4 min; every run
     from 2026-11-08 13:09 CET took 32-59 min. Three 07:00-slot runs exceeded the 3600 s curl
     limit (11-09 twice, 11-13 once), leaving the ~103 GB of orphans cleaned in v0.1.
   Before power-off: VM `um690-g1` shut down; `btusb` blacklist removed (backup
   `/root/aya-btusb-blacklist.conf.2026-11-13` on `.93`, initramfs rebuilt); `Wired connection 1`
   set to static `192.168.1.93/24`, `inet5` autoconnect off (profile backup
   `/root/nm-system-connections.2026-11-13`). GRUB default verified: plain Debian 7.3.3, no vfio.
   After boot: `eno1` 1000 Mb/s full duplex, 680M on `amdgpu`, BT up, 102 MB/s measured.
2. **Qdrant was running on built-in defaults.** `qdrant.service` on `.93` still pointed at
   `…/SoHoAI/scripts/qdrant/qdrant-config.yaml`, deleted by SoHoAI `95d5d0d`. At the 15:13 boot
   Qdrant logged "Config file via --config-path is not found" and started on defaults; it worked
   only because the default `./storage` under `WorkingDirectory=/var/lib/qdrant` is the same path.
   Fixed: config installed locally as `/etc/qdrant/config.yaml` by `scripts/qdrant/install-qdrant.sh`
   (never from NFS, which may not be mounted when Qdrant starts). Restarted 15:54 CET: no config
   error, 2,988,227 points, green.
3. **Snapshot location and retention** (`docs/RAG-strategy.md` §9.1):
   - `snapshots_path` is now `/var/lib/qdrant/snapshots` (Server 1 NVMe, transient). Before, it was
     the NAS, so every run wrote ~20 GiB to the NAS twice (Qdrant's archive, then the download).
   - One restore pair per day (snapshot when the newest NAS pair is older than 20 h; normally the
     01:00 run), 7 kept (~150 GB). Ingestion stays 4x/day. Safety nets: stale Qdrant-local
     snapshots and NAS `.tmp` deleted before each run, 60 GB NAS free-space guard, EXIT-trap
     cleanup, unpaired `rag_state` copies pruned.
4. **Clean transition.** Deleted: 109 GB of May 10-12 snapshots of the pre-sparse `documents`
   collection on `.93`'s NVMe (unrestorable); on the NAS, 9 of 12 current snapshot pairs (kept the
   newest per day: 11-11 18:07, 11-12 18:10, 11-13 12:10 UTC), 3 April/May `.checksum` files, 12
   May `rag_state` copies, the 2026-11-04 `rag_state-pre-rerun` backup, empty `qdrant/`,
   `qdrant-snapshots/documents_new`, `qdrant-snapshots/tmp`. NAS snapshot area 240 GB -> 61 GB.
5. **Data root split.** `/mnt/nfs/__Backups/SoHoAI--databases/{sqlite,logs,qdrant-snapshots,rag-ingest.lock}`
   moved (rename on the same NFS filesystem) to `/mnt/nfs/__Backups/RAGNART--databases/`.
   `SoHoAI--databases` keeps only SoHoAI's own `archive/` (old chat/telemetry DBs) and
   `SoHoAI-gateway.log` (written by `start-sohoai.sh`). All scripts, `ragnart.yaml`, utils defaults,
   logrotate and docs repointed.
6. **systemd / logrotate on Server 2.** `scripts/install-units.sh` (re-run as root): installs
   `/etc/logrotate.d/ragnart-ingest` with `su florian florian` (the old file had failed every night
   since 2026-05-06: "parent directory has insecure permissions"; log had grown to 228 MB, now
   rotated to 9 MB `.gz`), removes `/etc/logrotate.d/rag-ingest` and SoHoAI's
   `rag-ingest.service`/`.timer`.

#### Evidence

Verification run with `RAG_FORCE_SNAPSHOT=1`, 15:57:51-16:13:41 CET, exit 0, new data root:

| Step | Before (Wi-Fi, snapshots_path on NAS) | 2026-11-13 16:09 CET |
| --- | --- | --- |
| Ingest | -- | 28 files, 0 failed |
| Snapshot creation | 5-6 min | 44 s |
| 20 GiB download to NAS | 32-59 min | 3 min 21 s |
| Snapshot step total | ~40 min | 4 min 16 s |
| `.93` `/var/lib/qdrant/snapshots` after | -- | 0 bytes; Qdrant lists no snapshots |
| NAS pairs | 12 (240 GB) | 4/7 (data root 81 GB) |

#### Decisions

- Snapshots daily, 7 kept, instead of every run with 12 kept (operator: "balance availability with
  space"). Recovery point at most ~24 h; ingestion is idempotent.
- Data root split rather than a plain rename, so SoHoAI's gateway log path and archive stay valid.
- Qdrant config is a local file on Server 1, installed from the repo; the NFS copy is the source only.

#### Open items

- 19:00 timer run should skip the snapshot (newest pair 16:13, younger than 20 h); first scheduled
  daily snapshot on the 01:00 run of 2026-11-14.
- `redis-server` package still installed (disabled) on `.93`; unused, can be removed.
- Qdrant on `.93` vs `.95`: undecided; measure `.95` peak RAM first (Qdrant RSS is 15 GB).
- RAGNART `/health` hangs when Qdrant is down (no short client timeout) -- v0.2.

### 2026-11-13--14-04 -- v0.1 close

Single session, closing v0.1. It moved RAG code, config, service and ingestion from SoHoAI
into this repo, retired SoHoAI's `/v1/rag/search` handler to a 307 redirect stub, and ran
the two Definition-of-done checks. The session was run as a `/brain` pipeline with
Claude Opus 5.5 orchestrating and Claude Haiku 5.5 executing the steps.

#### What was done

| Commit | Repo | Subject |
| --- | --- | --- |
| `e4de8a3` | RAGNART | import: RAG code and docs from SoHoAI@5f4f1ed (plain copy, 32 files, sha256-identical to the source) |
| `1d702fe` | RAGNART | ragnart: adapt SoHoAI's RAG spin-off (package rename, ragnart.yaml, service module, systemd units, pyproject) |
| `796635f` | RAGNART | ragnart: add tie-aware old-vs-new endpoint comparison; align pins with venv |
| `95d5d0d` | SoHoAI | rag: spin off into RAGNART -- redirect stub, remove rag_engine/RAG utils/scripts, freeze RAG docs |

The docs commit that carries this entry cites only the code hashes above; its own hash is
recorded in `git log`, not here. Two RAGNART files that were not part of this session's
work were folded into the same docs commit: `README.md` (rewritten in a separate session)
and the avatar assets (`assets/ragnart-avatar.png`, `assets/ragnart-social-preview.png`,
and `assets/RAGNART-avatar.jpeg`, moved from the repo root).

Work by phase:

- **P1 -- import.** Byte-identical copy of `rag_engine/`, the RAG `utils/`, the RAG `scripts/`,
  and the two RAG docs from SoHoAI@5f4f1ed. Checked per file by sha256.
- **P2 -- adaptation.** `rag_engine` renamed to `ragnart`; `multi_query` and
  `utils/rag_mmr_bench.py` dropped; `ragnart.yaml` split out of `SoHoAI-config.yaml`
  (the RAG blocks only, no YAML anchors involved); about ten config-path sites switched
  to `ragnart.yaml`; `ragnart/service.py` added (`GET /v1/search`, `GET /health`);
  ingestion scripts and units renamed to `ragnart-*`; `scripts/install-units.sh` added;
  snapshot script given a 7200 s download timeout and a failure-cleanup trap;
  `pyproject.toml` and `.gitignore` added. Stale `ollama_url` fallbacks corrected to `.95`.
- **P3 -- live verification before cutover.** Manual uvicorn on `:8100`; `/health`
  returned `{"status":"ok","qdrant":true}`; `utils/rag_smoke_test.py` passed; the
  comparison tool was added and run (see Evidence). The first exact-ordered comparison
  failed; see Deviation 6.
- **P5 -- snapshot cleanup and cron.** Three orphaned Qdrant-local snapshots and three
  orphaned NAS `.tmp` files deleted (exact names only, re-listed and size-checked first).
  The 03:00 `sqlite-qdrant-snapshot.sh` crontab line removed; the crontab is now empty
  (backup kept in the session directory).
- **P4 -- cutover.** `scripts/install-units.sh` run as root at 13:00:23 CET. `ragnart-search.service`
  active on `:8100`; `ragnart-ingest.timer` enabled (next slot 19:00); `rag-ingest.timer`
  disabled. First RAGNART ingestion run 13:01-13:50 CET: NFS sync of 11773 files, 4263
  Claude sessions and 433 OpenCode sessions; 32 new files ingested, 0 failed; the
  20 GiB Qdrant snapshot downloaded in 34 min (13:16:13-13:50:40 CET), the Qdrant-local
  copy then deleted; Result=success; 12/12 snapshot pairs after rotation.
- **P6 -- client repoint.** Both `/rag` command files (`dot.claude-Linux/commands/rag.md`
  and `~/.config/opencode/commands/rag.md`) pointed at `http://192.168.1.95:8100/v1/search`
  (lines 16, 37, 76). Plain files, not under git.
- **P7 -- SoHoAI removal.** In SoHoAI: `main.py` gained a 307 stub for `/v1/rag/search`;
  the Qdrant client set-up, `rag_cfg` plumbing and `variant_llm_fn` removed from `main.py`;
  the RAG blocks removed from `SoHoAI-config.yaml`; 30 RAG tracked files removed
  (`rag_engine/`, `utils/rag_*`, RAG `scripts/`); doc banners on the two RAG docs;
  `CLAUDE.md`, `README.md` and `docs/design-history.md` updated. Single commit `95d5d0d`.
  The gateway reloader did not pick up the edits; the operator restarted it (Deviation 7).
- **P8 -- docs and close.** RAG docs corrected by name and path; `docs/design-history.md`,
  this log and the plan status updated; RAGNART `README.md` and avatar assets committed
  as described above. After review, two residual `~/Gin-AI/projects/SoHoAI` mentions in the carried-over RAG docs were fixed (RAG-strategy.md setup instruction repointed; RAG-troubleshoot.md gateway-reload note annotated).

#### Deviations from the plan

1. **`docs/Changelog.md` was not created.** Plan Section 7 planned it. This log, in the
   AYA format, replaces it from v0.1 onward. Recorded as a dated note in Plan Section 6
   v0.1 and Plan Section 7.
2. **`numpy` dropped from `pyproject.toml`.** Plan Section 6 v0.1 named it. After dropping
   `multi_query.py` and `utils/rag_mmr_bench.py`, nothing in the import graph imports
   `numpy` directly. Recorded in Plan Section 6 v0.1. All other pins in `pyproject.toml` were likewise aligned to the versions actually installed in the shared venv (`fastapi==0.135.1`, `qdrant-client==1.17.1`, `uvicorn==0.42.0`) instead of the plan's literal values; `python-pptx==1.0.2` was taken from the venv as planned.
3. **Snapshot work was not a named v0.1 item.** The download timeout fix (`--max-time`
   3600 to 7200 and a failure trap), the cleanup of six orphaned artefacts, and the
   removal of the 03:00 cron line were done here. They were required by this session's
   constraints: the 07:00 slot had failed with exit 28 on the 3600 s timeout, and the
   03:00 cron snapshot was redundant with the snapshot each ingestion run already takes.
4. **Cutover was run with passwordless sudo by the Actor, not by the operator.** Plan
   Section 6 v0.1 names the systemd move; the install script ran via `sudo -n` at 13:00:23
   CET, with `ssh root@localhost` as the fallback route. Passwordless sudo was
   timestamp-based and worked intermittently during the session.
5. **The SoHoAI `rag-ingest.timer` was stopped early by Brain.** At 12:49 CET Brain
   stopped it via root ssh, so the 13:00 SoHoAI run could not start while the cutover
   was pending. The timer stayed enabled until `install-units.sh` disabled it at 13:00:23.
6. **The exact-ordered old-vs-new gate was replaced by a tie-aware gate.** The first
   comparison failed on exact ordering (4/14, then 6/14 passing). The root cause is
   non-determinism on the old endpoint alone: Qdrant RRF breaks exact score ties
   inconsistently (OLD-vs-OLD, `rerank=false`: 5/14 identical orderings), and the
   reranker's `relevance_score` moves by about 1e-3 to 1e-2 with batch composition, which
   flips near-ties. The operator chose a tie-aware gate (see Decisions).
7. **The SoHoAI gateway was restarted by the operator, not reloaded.** The
   `uvicorn --reload` process on Server 1 did not reload on NFS edits, from Server 2 or
   from a local `touch`. The operator restarted it at 13:20 CET. Startup completed at
   13:24:56 CET, slowed by the concurrent 20 GiB snapshot write saturating the NAS. The
   307 stub was then verified live.
8. **The 307 stub stays in SoHoAI until 2026-11-16.** The plan's removal of the stub at
   the end of the transition was not done in this session. The operator set the date
   (originally 2026-11-27). Open item below.
9. **Extra SoHoAI config blocks removed.** Beyond the `rag:` block, the `users`, `shared`,
   `claude_chats`, `opencode` and `db_base_path` blocks were removed from `SoHoAI-config.yaml`,
   after a repo-wide grep found no remaining reader. The anchors used by `audio_models`
   and `proxy` (`server1_ip`, `server2_ip`) are kept.
10. **Commit trailers name the executing model.** The plan's trailers say Claude Opus 5.5.
    The commits carry `Co-Authored-By: Claude Haiku 5.5`, which is the model that actually
    ran the steps.
11. **README and avatar files folded into the docs commit.** A separate session rewrote
    `README.md` and converted the avatar. Its files were folded into this session's docs
    commit at the operator's direction, rather than a separate commit.
12. **Stale `ollama_url` fallbacks fixed.** `embeddings.py`, `search.py` and `ingest.py`
    had fallback literals pointing at Server 1 (`.93`). They now point at `.95`. The
    `qdrant_url` fallbacks correctly stay on `.93`. Plan Section 8 #4 noted the port choice.

#### Decisions

- **Service and unit naming.** `ragnart/service.py` (`uvicorn ragnart.service:app`);
  `ragnart-search.service` on port 8100; `ragnart-ingest.service` and `.timer`;
  `scripts/install-units.sh`. Port 8100 was free on Server 2 (8000 llama-swap, 4096
  OpenCode, 11434 Ollama, 8011/8012 llama-server).
- **Comparison tool kept.** `utils/rag_compare_endpoints.py` is committed, not a scratch
  file. It re-checks Plan Section 4's invariant ("no new params means unchanged ranking")
  and is reusable for every future version.
- **Tie-aware equivalence gate (operator-approved).** With `rerank=false`, each query must
  have the same multiset of scores (tolerance 1e-6), and within each tied-score group the
  same set of `source_path` values (the last group, if cut at `top_k`, compared on count
  and score only). With `rerank=true`, top-1 equality and set overlap are reported but do
  not gate. An OLD-vs-OLD control runs with the same checks, so the noise floor is visible.
- **Production non-determinism is not fixed here.** Adding a stable tie-break or pinning
  reranker batching changes search behaviour, so it is an open item for v0.2+, not v0.1.
- **Snapshot schedule.** One snapshot per ingestion run (`--keep 12`); the 03:00 cron
  entry removed. Scheduling of the 4x/day snapshots is an open item.

#### Evidence

Bench comparison, `utils/rag_compare_endpoints.py`, 14 queries from
`utils/rag_bench_queries.txt`, `top_k=5`, user `florian`. OLD is SoHoAI `:8000/v1/rag/search`;
NEW is RAGNART `:8100/v1/search`. The per-query PASS/FAIL lines are not kept in the session
directory; the aggregate results are below.

| Check | Scope | Result |
| --- | --- | --- |
| HARD | OLD vs NEW, `rerank=false`, tie-aware equivalence | **14/14 PASS** |
| CONTROL | OLD vs OLD, same checks | 14/14 PASS |
| INFO | OLD vs NEW, `rerank=true`, top-1 equal | 14/14 |
| INFO | mean top-k set overlap, OLD vs NEW | 0.79 (control: 0.79) |

- `/health` on NEW: `{"status":"ok","qdrant":true}` (P3 and after cutover).
- `utils/rag_smoke_test.py --query "AWS certifications" --user florian --expect "AWS-Certification"`:
  PASS (P3, and again after cutover).
- `systemctl list-timers`: `ragnart-ingest.timer` enabled, next run 19:00 CET;
  `rag-ingest.timer` disabled.
- Live 307 from SoHoAI `/v1/rag/search?q=...` to `http://192.168.1.95:8100/v1/search?...`: first verified at about 13:25 CET, right after the operator's restart of the Server 1 gateway (13:24:56 CET); re-verified after the final P7 edits at 13:59:38 and 14:01:28 CET (12:59:38Z / 13:01:28Z), and again by the Reviewer.
- Ingestion run 13:01-13:50 CET: 32 new files, 0 failed; WAL checkpoint logged; snapshot
  phase completed with `Result=success`.

#### Open items

- **Production search is non-deterministic.** No stable tie-break; reranker batch jitter
  can flip near-ties. Candidate fix for v0.2+; changes behaviour, so not in v0.1.
- **`utils/rag_status.py` reports a false "collection 'documents' does not exist".** It
  does not check aliases, and `get_collections` excludes them. Pre-existing; not fixed here.
- **SoHoAI residuals, out of scope for v0.1.** `schemas.py` orphaned RAG models; RAG
  dependencies (`qdrant-client`, `docling`) still in SoHoAI's `pyproject`; `CLAUDE.md`
  stale RAG mentions (servers table with the rag-ingest timer and 03:00 cron, storage
  paths, a code-style line about `rag_engine`/`state.py`, the architecture diagram);
  `.claude/settings.local.json` `rag-*` allowlist entries.
- **Stub removal, 2026-11-16 (operator date).** Remove SoHoAI's `/v1/rag/search` 307 stub
  on or after 2026-11-16, or at v0.2 kickoff if that is earlier. Before removal, check
  `SoHoAI-gateway.log` for `GET /v1/rag/search ... 307` hits and repoint any caller found.
- **`scripts/qdrant/qdrant.service` and `qdrant-config.yaml`** are carried over with the
  path fixed but **not reinstalled**. Qdrant keeps running from its existing unit.
- **Logrotate.** `ragnart-ingest-logrotate` is renamed but not installed to
  `/etc/logrotate.d/`. The log filename (`rag-ingest.log`) is unchanged, so an existing
  logrotate config should still match. To verify whether one exists.
- **Server 1 gateway reloader is unreliable on NFS.** `uvicorn --reload` did not pick up
  NFS edits. Changes to `main.py` need a manual restart on Server 1 until this is fixed.
- **Snapshot load on the NAS.** Four runs a day, each about 35 min, saturate the NAS and
  slowed the gateway start. Consider scheduling.

#### Handover to v0.2

- **Tree state.** RAGNART HEAD: the docs commit that carries this entry (its parent is
  `796635f`). SoHoAI HEAD: `95d5d0d`.
- **v0.2 entry criteria (Plan Section 6 v0.2).** RAGNART's service and timer are the only
  ones running -- confirmed by this session's P4 verification. **Also required:** the
  SoHoAI `/v1/rag/search` 307 stub is removed (on or after 2026-11-16), after the
  gateway-log check in Open items.
- **Must read first.** v0.2 must read Plan Sections 2-5 in full plus its own Plan Section 6
  v0.2, per Plan Section 1's rule.
- **Environment prerequisites.** Qdrant 1.17.1 at `192.168.1.93:6333`; Ollama bge-m3 at
  `192.168.1.95:11434`; reranker via llama-swap at `192.168.1.95:8000/v1/rerank`; the
  `ragnart-search.service` on `:8100` and `ragnart-ingest.timer` both running from this repo.
