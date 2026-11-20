---
title: "RAGNART — Design"
created_at: 2026-11-13--23-55
created_by: Claude Code (Claude Opus 5.5)
updated_by: Claude Code (Claude Fable 5.2); Claude Code (Claude Haiku 5.5, 2026-11-15 -- §10.3 single-writer lock for every index writer, holder and fail-fast exit 75; §13.2 sync-lock item removed); Claude Code (Claude Haiku 5.5, 2026-11-15 -- v0.2 session A invariants); Claude Code (Claude Haiku 5.5, 2026-11-15 -- v0.2 session B: baseline data model; one item per session uuid (§14 D11, newest copy wins); ragnart project spelled out; §4.2 sentence restored after the one-item paragraph insert); Claude Code (Claude Haiku 5.5, 2026-11-16 -- v0.2 session C: built state for markers, attachment, moves, tombstones, plan-then-apply, scope API, /v1/projects, collapse and reconciliation; §14 D12 to D17; §4.5 marker-discovery wording says ragnart project); Claude Code (Claude Fable 5.2, 2026-11-16 -- multi-source design: connectors and sources, central and the ingestion side, the three stores, source-qualified scope terms; new §2.3, §4.7, §4.8; §14 D18 to D26; later versions renumbered for the new v0.3); Claude (Claude Fable 5.2, claude.ai design-review session with the operator, 2026-11-16 -- pipeline scope and the two kinds of version change §4.1, §5; four ingestion requests §2.3, §12; the notebook model, membership, references and marker guards §4.5; an add always copies §7.3; no ingestion mode §7.2, §7.4; the librarian's guarantee and the relevance verdict §8.1, §8.7, §9; open items §13.3; §14 D8, D10, D15, D19 amended, D27 to D31 added); Claude Code (Claude Opus 5.5, 2026-11-17 -- design discussion with the operator, the notebook model worked back from what to how: `ragnart init <name>`, managed folders beside `.ragnart/`, `.ragnart/` as state only, references by stable id with files as the exception, sessions referenced by the hook, exclusion rules out of managed folders, `add` and `commit`, registration, project-relative keys, `mv` and `repair`, concurrency §10.6; §14 D2, D2b, D3, D9, D10, D12, D15, D19, D21, D27, D28, D29 amended, D32 to D41 added; references are resources, the session's current project, creation from inside a session; the crawl does not enter a project folder, no implicit commit, hand-made markers; central renamed the hub, D42; `init` publishes the project record; projects addressed by unique name; corrections from an independent consistency review); Claude Code (Claude Opus 5.5, 2026-11-18 -- after `init` the session moves its shell into the project folder; relocating the session itself is an open item; how the agent searches, D31 amended; no separate session state, the working reference; `session:current`, settled for Claude Code and OpenCode; `/ragnart open`; the capture hook; writing collateral; resume and compaction; the web is a scope, scope and grounding not fully orthogonal; relocation tested: spawn in the project folder, operator resumes it; fork verified, one turn one place; the switch stays the operator's instruction, worded per client); Claude Code (Claude Haiku 5.5, 2026-11-18 -- v0.3 session A: source ids and the connector contract, the hub registry and its reshaped project tables, `source_id` and `pipeline_version` on every item and chunk, source-qualified scope terms and the bare-path 422, the per-source reconciliation check, the restore triple; status lines of §1.1, §2.3, §4.1 to §4.5, §4.7, §4.8, §8.3, §8.7, §8.8, §10.1, §10.3, §10.4, §12; one turn, one place marked not built, v0.4); Claude Code (Claude Haiku 5.5, v0.3 session A fix-up -- migration 4 reserved in _MIGRATIONS; migrate tool decides by table presence; migration 4 wording in the schema bullet and §4.8 status); Claude Code (Claude Haiku 5.5, 2026-11-19 -- v0.3 session B1: the ingestion API, the intake table and worker, endpoint sources MBP and claude-MBP, the source id rule; status lines of §1.1, §2.3, §4.2, §4.5, §4.7, §4.8, §5.2, §10.2, §10.3, §12 and the host table §2.1); Claude Code (Claude Opus 5.5, v0.3 session B2: NAS ingestion on the ingestion API); Claude Code (Claude Opus 5.5, 2026-11-20 -- project support is built for the NAS ingestion only; D21 checked first); Claude Code (Claude Opus 5.5, 2026-11-20 -- RAGNART names, User-Agent, one-line hub-unreachable log); Claude (Claude Fable 5.2, claude.ai checkpoint evaluation with the operator, 2026-11-20 -- §15 future considerations: what would be hard to retrofit for use by a team or an organisation; pointers in §1.1 and §6.5; v0.4 delivered in two parts)
updated_at: 2026-11-20--20-30
context: >
  The design of RAGNART: a self-hosted retrieval system and knowledge-base manager for one
  household lab (two servers and a Synology NAS), covering ingestion, access control,
  lifecycle and retrieval. It records the decisions taken by the operator in a design review
  held before version v0.2, each with its rationale and its consequences for the code. The
  review worked through identity, the data model, ownership and access, tiers and lifecycle,
  retrieval scope and grounding, retrieval quality, and failure and consistency, and ended
  with a measured analysis of the corpus. Inputs: the code in this repository, the running
  services (Qdrant on Server 1, the search and ingestion services on Server 2), the ingestion
  queue and logs on the NAS, docs/RAG-strategy.md, docs/Implementation-plan.md, and the
  operator's answers. docs/Implementation-plan.md sequences the work; this document is what
  that work implements.
---

# RAGNART — Design

## 1. Purpose and principles

RAGNART turns everything the operator has (documents on the NAS, Claude Code and OpenCode
sessions, material gathered for a project) into a searchable knowledge base, and manages that
knowledge over time: what goes in, who may see it, how long it lives, and how it is found.

It has two jobs:

1. **Retrieval** — given a question and a scope, return the relevant passages, each with the item
   it comes from, and say whether they are enough to answer from (§8.7).
2. **Knowledge-base management** — ingestion, access control and lifecycle, per project.

Its origin is a self-hosted NotebookLM: a per-project set of sources that a conversation is
grounded in. A ragnart project is such a notebook (§4.5). Its context is available automatically,
and it is built up by capturing material as the work goes, keeping what matters, and producing
collateral from it (§7.3).

**Principles.** Every design decision below follows from these.

| Principle | Meaning |
|---|---|
| **Librarian answers** | Answers are facts, references and quotes from the material at hand. Nothing is extrapolated or guessed unless the operator explicitly opens the grounding (§8). RAGNART guarantees the evidence and its verdict on it; staying within the evidence is the client's part (§8.1). |
| **Retrieval-only** | RAGNART returns evidence and says whether it is enough; it never writes answers and contains no model gateway. Clients (Claude Code, OpenCode, Kotaemon) compose answers. |
| **The source is the authority** | The content of each source (§4.7) is the truth; today that is the files and session stores on the NAS. A state database is bookkeeping; the vector index is derived. |
| **Explicit over implicit** | The current scope and grounding are always visible; every widening or narrowing is a deliberate act of the operator, loud and visible, never silent. |
| **KISS** | Use what exists (paths, folders, files, one pipeline, one store) before adding mechanisms. Add primitives where tuning is needed, not cleverness. |
| **Predictable, not rigid** | Same input on the same data gives the same result; behaviour changes only when the data or the settings change, and the settings are knobs the operator controls. |
| **Git parallel** | RAGNART mirrors git in ingestion, exclusion and lifecycle, using git's command names where the mapping is exact (§1.2). |
| **Current-state documentation** | Documents describe how things are; history is in git and in the implementation logs. |

### 1.1 Status of this design

This document describes the **target design**. Parts of it run today; the rest is built in
the versions of `docs/Implementation-plan.md` §6. Sections whose content is not yet built carry
a **Status** line. Summary:

| Area | Status |
|---|---|
| Hosts, services, data root, snapshot policy (§2, §10.4) | Built |
| Pipeline `p1` (§5), admission rules (§5.2) | Built |
| Parallel with git (§1.2): ignore rules and the read-only `ragnart` commands `check-ignore`, `ls-files`, `status` | Built. `add`, `rm`, `mv`, `diff`, `fsck`, `gc`: v0.4 |
| Item identity for NAS files, `opencode://` and `claude-session://` keys, content hash (§4.2) | Built. Move detection: v0.2 |
| State-database metadata and payload fields and their indexes (§4.3, §4.4), schema version (§4.1), per-item pipeline version (§4.1) | Built, with `source_id` on every item and chunk and `pipeline_version` on every chunk. The hub's listing of a source's items, for every source including the NAS ones: built (v0.3, sessions B1 and B2, §12) |
| Projects data model: the `projects`, `project_tags`, `project_references`, `grants` and `sources` tables of the hub registry, with the reserved `global` project; baseline data model (§4.6) | Built, in the hub registry (§4.8): `projects` has `home_source_id`, and there are no `parent`, `mode` or `source_links` columns or tables. `project_references` is written from v0.4, and name uniqueness is v0.4. The RAGNART repository holds the first ragnart project |
| `project.toml`, marker discovery, tags, `resolve_scope()` (§4.5, §8.8); owner rule as the access rule (§6.5) | Built. `authorize()`, grants and authentication: v0.6 |
| Project content and membership (§4.5): `ragnart init <name>`, the managed folders beside `.ragnart/`, `.ragnart/` as state only, no meaning for nesting, marker guards | v0.4. Built today, to an earlier model: every file under the directory that holds `.ragnart/` carries its `project_id`, a nested ragnart project records a `parent`, and a missing marker removes its ragnart project at once |
| References by stable id (§4.5): to ragnart projects and to sessions, with the session-start hook referencing a session started in a project folder | v0.4. Built today, to an earlier model: every sync assigns each session to the ragnart project whose holding directory encloses its `launch_dir` |
| Registration of project folders, project-relative keys of managed files, `ragnart mv` and `ragnart repair` (§4.2, §4.5) | v0.4; a move to another source is the change-of-source request, not built (v0.4, §2.3). Built today: ragnart projects are found by the walk of the crawl roots only, and every file is keyed by its full path |
| Scope and grounding in the API and the `/ragnart` skill and commands (§8) | API scope and project listing: built. Skill and commands: v0.4 |
| Project workflow: `init`, capture, the inbox, keep and drop, collateral, `add` and `commit`, the `/ragnart` skill, the session-start hook and the status line (§7.3 to §7.5); concurrency rules (§10.6) | v0.4 |
| Retrieval-quality knobs and calibration (§9) | `max_hits_per_source` and collapse by `content_hash`: built. Other knobs and calibration: v0.4 |
| Relevance verdict in the search response: hit id, rerank score, good-hit count, reranker status (§8.7, §9.2) | Not built. It depends on neither v0.3 nor v0.4 |
| Deletion rules (§10.2), reconciliation (§10.3) | Built: all five rules of §10.2 (the purge record is the sync log line of rule 4), plan-then-apply, and the scheduled reconciliation of §10.3 |
| OpenCode database backup (§10.5) | v0.2 |
| Connectors and sources (§4.7), the hub and the ingestion side (§2.3), the three stores (§4.8), `source_id` on every item and chunk (§4.3, §4.4), source-qualified scope terms (§8.8) | Built: the connector contract as a code interface, the three hub sources and the two endpoint sources registered, the hub registry, `source_id` on every item and chunk, and source-qualified scope terms. Every source, the NAS ones included, submits through the ingestion API (v0.3, session B2). The NAS ingestion runs on Server 2 as an ingestion side, with `rag_state.db` as its state database and its own lock; the endpoint sources run on their own host, with that host's state database (§2.3) |
| Ingestion API, and sources on hosts other than Server 2 (§2.3, §12) | Built (v0.3, sessions B1 and B2): the ingestion API for every source, the three NAS-side sources on it as an ingestion side on Server 2, and two endpoint sources on the operator's Mac |
| Kotaemon (§12) | v0.5 |
| Future considerations: use by a team or an organisation (§15) | Considerations, not decisions. The provisions of §15.2 are confirmed or declined at the v0.4a kickoff |

v0.4 is delivered in two parts, v0.4a and v0.4b (`docs/Implementation-plan.md` §6). A status of
"v0.4" in this document does not say which part.

### 1.2 Parallel with git

RAGNART follows git as closely as it can in ingestion, exclusion and lifecycle. Where a RAGNART
command does exactly what the git command of the same name does, it takes that name. Where the
mapping is not exact, RAGNART uses its own terms: the Archive, Runtime and Transient tiers (§7.1),
the queue, the index, the ragnart project, and the static and curated policies (§7.2). The reasons for
each deviation are recorded here and in decision D7 (§14).

| git concept | RAGNART | Note |
|---|---|---|
| Working tree | The files of a filesystem source (§4.7); today the NAS, under the NFS roots | The authority. RAGNART does not modify them. It writes only inside ragnart projects, in `.ragnart/` and the managed folders (planned, §7.3), and appends lines to `.ragignore` files (by `rm`, planned, deviation B). |
| Repository, created by `git init <dir>` | Ragnart project, created by `ragnart init <name>` (planned) | `init` creates the folder `<name>` with `.ragnart/` inside it (§4.5). The folder can be anywhere, and its place has no meaning. Unlike a repository, a ragnart project knows only its managed folders (deviation I). |
| `.git/` | `.ragnart/` | State and metadata only: `project.toml`, provenance, dropped items (§4.5). The material is in `resources/` and `collateral/` beside it. |
| Tracked files | The files of a ragnart project's `resources/` and `collateral/` | Tracked by location: a file placed there by hand belongs to the project as one placed by `add` does (§4.5, §7.3). |
| Untracked files | The files of a ragnart project's folder outside its managed folders; and its inbox, `inbox/` | Neither is indexed: the global crawl does not enter a project folder (deviation D), and a commit skips the inbox (§7.1). |
| Staging area | Queue rows with status `pending`; in a ragnart project, the files of its `resources/` and `collateral/` that no commit has indexed yet | Queued or placed, not yet indexed. |
| Committed content | The index: Qdrant points and `completed` queue rows | Derived from the sources (§10.1). |
| `.gitignore` | `.ragignore`, one per directory | Same syntax and precedence (below). Not applied in a ragnart project's folder (deviation D). |
| `core.excludesFile` | `config/ragignore` | Global exclusion rules with the lowest precedence. There is no `.git/info/exclude` equivalent. |
| System, global and local config | `ragnart.yaml`, then `project.toml`, then the request | Each level overrides the one before it (§9.2). |
| `git status` | `ragnart status` (built) | Queue and Qdrant counts; `--ignored` lists ignored files with their reason. Planned: scoped to a ragnart project, it lists the files not indexed and why (§7.3). |
| `git ls-files`, with `--others`, `--ignored`, `--stage` | `ragnart ls-files` (built), same flags | Without a flag, lists indexed files. `--stage` lists every queue row with its status. Planned: scoped to a ragnart project, the listing is that ragnart project's catalogue, and `--others` lists its inbox (§7.4). |
| `git check-ignore -v` | `ragnart check-ignore -v` (built) | Prints the file, line and pattern that decide each path. |
| `git add` | `ragnart add --no-commit` (planned) | Copies a file into the ragnart project's `resources/` and records its origin. Nothing is indexed until a commit (§7.3). |
| `git add` + `git commit` | `ragnart add` (planned) | Copies, records the origin and commits, and waits (deviation E). On an inbox item, `add` is the operator's "keep". |
| `git commit` | `ragnart commit` (planned) | Indexes every file of the ragnart project's `resources/` and `collateral/` that is not indexed yet, including files placed by hand, and waits for the outcome of each (§7.3). No history is kept (deviation F). |
| `git rm` | `ragnart rm` (planned) | Appends an anchored line to the nearest `.ragignore`; never deletes a file (deviation B). In a ragnart project the verb is `drop` (§7.5). |
| `git mv` | `ragnart mv` (planned) | A file: move detection by `content_hash` (§4.2); nothing is re-embedded. |
| `git worktree move` | `ragnart mv` of a ragnart project (planned) | Moves the project folder, to another place or another source, keeping its id; nothing is re-embedded (§4.5). |
| `git worktree list`, `git worktree repair` | The registered project folders; `ragnart repair <path>` (planned) | `init` registers the project folder with its ingestion host. `repair` re-registers a project folder moved by hand (§4.5). |
| `git diff` | `ragnart diff` (planned) | The plan of the next sync, shown without applying it. |
| `git fsck` | `ragnart fsck` (planned) | Reconciliation (§10.3). |
| `git gc` | `ragnart gc` (planned) | Purges tombstones after the grace period (§10.2), and removes expired and dropped inbox items (§7.5). |
| Staging everything on each run (`git add -A`) | Static policy, run by the timer | Every 6 hours, all files that the inclusion and exclusion rules admit (§7.2). It does not enter a project folder, and it commits no ragnart project (§7.3). |
| Staging chosen paths (`git add <path>`) | Curated policy | Only what is placed in a ragnart project's `resources/` and `collateral/` (§7.2, §7.3). |
| `git log`, branches, remotes | None | Deviation F. The snapshot triples (§10.4) are the nearest equivalent to tags and restore. |

**Deviations from git.** Each deviation is named by a letter and states its reason.

- **A. Ignore rules also remove indexed content.** The index is the NAS minus what the rules
  exclude. A newly excluded item leaves the index through the normal sync, under its deletion
  rules (§10.2). The sync log names the `.ragignore` file and line that matched. *Why:* git's
  ignore rules never affect tracked files. Here that would keep excluded content in the index
  indefinitely, which is what the exclusion exists to prevent.
- **B. `rm` appends a rule and never deletes a file.** `ragnart rm <path>` (planned) adds an
  anchored pattern to the nearest `.ragignore`. *Why:* the exclusion is durable, visible in a text
  file, and reversed by removing its line. The source is the authority, so RAGNART does not delete
  files from it. The one exception is files of a ragnart project: in its `inbox/` and under its
  `.ragnart/dropped/` (§7.5).
- **C. Symbolic links are followed.** The walk follows links, and patterns match the path as
  walked, never a resolved path. A file reached through several links is queued once, under the
  first path the walk meets. *Why:* `Gitlab--fotel` is reached through a link, and a rule should
  match the path the operator sees.
- **D. The global crawl does not enter a ragnart project's folder.** A `.ragnart/` marker acts, for
  the crawl, as a `.ragignore` holding `*`: the crawl reads the marker, so that it can register the
  ragnart project (§4.5), and does not walk the folder. Nothing in a project folder becomes a `global`
  item. Its `resources/` and `collateral/` are indexed only by a commit (§7.3), which applies the admission rules of
  §5.2 and no exclusion rule; the other files of the folder are not indexed at all. `ragnart
  check-ignore` names the project folder as the reason. *Why:* the global crawl and a ragnart project
  are two kinds of ingestion with different meanings. If the crawl walked a project folder, it would
  either give the project material it did not choose or turn its files into `global` items, and an
  exclusion rule written for the archive, such as `*.csv`, would drop material the operator chose.
- **E. `add` commits, and waits.** `ragnart add` copies the file, commits and waits for the outcome;
  `ragnart add --no-commit` only copies, as `git add` does (§7.3). *Why:* adding a source to a
  notebook makes it usable at once. Commits of one ragnart project coalesce (§7.3), so a run of adds
  does not cost one ingestion each.
- **F. No history, branches or remotes.** `ragnart commit` indexes the current content of a ragnart
  project and records nothing else: there is no log of commits. *Why:* the source is the authority, and the NAS already
  keeps history: hourly Synology snapshots and an offsite copy (§10.1). A second history inside
  RAGNART would duplicate it and could disagree with it. The snapshot triples (§10.4) are the restore
  points of the index. A source that submits its items to the hub (§2.3) is not a remote: the hub
  holds a derived index, not a copy that can be fetched or cloned.
- **G. An unreadable `.ragignore` stops the sync.** Git warns and continues. RAGNART raises an
  error: the sync exits with a non-zero code and runs no deletion phase. *Why:* an exclusion that
  cannot be read must neither admit content it would exclude nor delete content it would keep.
  Continuing would do one or the other.
- **H. Capture is ambient.** In a ragnart project with capture on, a page the agent fetches is written to
  its `inbox/` without a command from the operator, by a capture hook on the harness's fetch tool
  (§7.3).
  *Why:* git records nothing unless asked. Work in a ragnart project finds its material during a conversation,
  and a command per item would interrupt it. Captured material stays out of the index until the
  operator keeps it, so the index still changes only by a deliberate act.
- **I. A ragnart project knows only its managed folders.** Its material is the content of
  `resources/` and `collateral/`, and its inbox is `inbox/` (§4.5). Every other file in its folder is
  external: the project does not know it, and it is not indexed (deviation D). A ragnart project
  nested in another one's folder is external to the outer one. *Why:* a repository
  tracks what is added to its working tree. A ragnart project is a notebook, whose material is what
  was put into it, and it never adopts what happens to lie around it.

**`.ragignore` semantics.** The format and matching are those of `.gitignore`:

- Blank lines and `#` comments are ignored. `\` escapes the next character, including `\#` and a
  trailing space.
- A leading `!` re-includes a path. The last matching pattern in a file decides.
- A trailing `/` matches directories only.
- A `/` anywhere except at the end anchors the pattern to the directory of its file. Otherwise the
  pattern matches at any depth below that directory.
- `*`, `?`, bracket expressions (including POSIX classes such as `[[:digit:]]`) and `**` behave as in
  gitignore.
- A `.ragignore` in a directory applies to the paths below it, matched relative to that directory.
  A file in a deeper directory takes precedence over one in a shallower directory. `config/ragignore`
  has the lowest precedence and is matched relative to the crawled root.
- A path below an excluded directory cannot be re-included. The walk does not enter an excluded
  directory, so a `.ragignore` inside it is never read.
- The rules apply to sources of the filesystem connector only (§4.7); today that is the NAS crawl.
  They do not apply to the Claude Code and OpenCode session connectors, nor in a ragnart project's
  folder, which the crawl does not enter (deviation D). A ragnart project under an excluded
  directory is still found, because its folder is registered (§4.5).
  `.gitignore` files are not read.
- An unreadable `.ragignore` stops the sync (deviation G).
- `ragnart check-ignore -v <path>` shows the file, line and pattern that decide a path.

**Global and per-directory exclusion files.** Two files hold exclusion rules. `config/ragignore`
is the live global list. It applies to every NAS root on every scan, has the lowest precedence,
and lives in the repository, so it is reviewed and versioned with the code, as `ragnart.yaml` is.
It holds system-wide rules (`.git/`, `node_modules/`, `.venv/`, and similar). A `.ragignore` in a
directory holds that directory's own rules and belongs to its owner. Precedence, lowest first:
`config/ragignore`, then the `.ragignore` files from the root downward, where a deeper file wins.
`config/ragignore.example` is an inert template (every line commented out, excludes nothing; example
lines start with `#> `, remove the marker to activate one); copy it as `.ragignore` into a directory
to start local rules. This mirrors git's `core.excludesFile` against `.gitignore`.

## 2. Architecture

### 2.1 Hosts and components

| Host | Role in RAGNART |
|---|---|
| **Server 1** — um690, 192.168.1.93 | Qdrant 1.17.1 (vector store) on local NVMe, `/var/lib/qdrant/storage`; config installed locally as `/etc/qdrant/config.yaml` by `scripts/qdrant/install-qdrant.sh` |
| **Server 2** — NucBoxK11, 192.168.1.95 | The hub and, today, the only ingestion host for the NAS (§2.3). `ragnart-search.service` (HTTP API, port 8100); `ragnart-ingest-api.service` (ingestion API, port 8101); `ragnart-intake.timer` (hub intake worker, every 10 minutes); `ragnart-ingest.timer` (ingestion 01, 07, 13, 19:00); Ollama with bge-m3 (port 11434); the reranker bge-reranker-v2-m3 behind llama-swap (port 8000); the only host that opens `rag_state.db`; the OpenCode server (port 4096) |
| **Endpoint host** — the operator's Mac, 192.168.1.3 | An ingestion host for the endpoint sources `MBP` and `claude-MBP` (§2.3, §4.7). Its runner, `ragnart.endpoint`, runs every six hours from a LaunchAgent that starts it through the launcher app `RAGNART Ingest.app` (macOS charges Local Network access to that app); its state database and secrets are under `~/.ragnart/` |
| **NAS** — Synology DS1019, NFS `/mnt/nfs` | The corpus (NFS roots), Claude Code sessions (`~/.claude` is a link into NFS), RAGNART's data root `/mnt/nfs/__Backups/RAGNART--databases/` (state database, logs, snapshot pairs, ingestion lock). BTRFS with hourly snapshots and an offsite copy. |

Clients: Claude Code and OpenCode sessions (through the `/ragnart` skill and commands), the
`ragnart` command-line tool, and Kotaemon as the browser UI. A SoHoAI gateway on Server 1
supplies models to clients; RAGNART does not use it.

### 2.2 What lives where

| Data | Location | Protection |
|---|---|---|
| Documents, Claude Code sessions | NAS | Synology snapshots + offsite |
| `rag_state.db`, `hub_registry.db`, logs, snapshot triples | NAS, `RAGNART--databases/` | Synology snapshots + offsite |
| Qdrant live index | Server 1 local NVMe (Qdrant refuses NFS) | Daily snapshot triple on the NAS (§10.4) |
| Qdrant transient snapshot | Server 1 `/var/lib/qdrant/snapshots` | Exists only during a snapshot run |
| OpenCode's database | Server 2 local NVMe (SQLite in WAL mode is unsafe on NFS) | Nightly online copy to the NAS (§10.5) |
| Installed configs and units | Local `/etc` on both servers | Reinstallable from the repository (on NFS) |

### 2.3 The hub and the ingestion side

**Status.** The split of roles below is the design. Built (v0.3, sessions B1 and B2): the ingestion API
(`ragnart-ingest-api.service`, port 8101, a process separate from search), which records each request in
the intake table of the hub registry (§4.8) and spools uploaded bytes; and the hub intake worker
(`ragnart-intake.timer`, every 10 minutes), which applies the requests under the ingestion lock with a pool
of three workers and keeps the requests for one item in order. The hub, the API plus the intake worker, is the
only writer of the index. Every source submits through the API, the NAS-side ones included. The NAS ingestion
runs on Server 2 as an ingestion side (`ragnart-ingest.timer`): it scans, keeps `rag_state.db` as its state
database, submits to the API on loopback with one secret per source (`nas`, `claude-linux-shared-nfs`,
`opencode-k11`), and reads the outcome of its requests on its next run. It never opens a Qdrant client. Two
endpoint sources run on the operator's Mac: `MBP` (filesystem) and `claude-MBP` (Claude Code). A source
without a fixed owner (`nas`, `claude-linux-shared-nfs`, `opencode-k11`) names the owner of each item in its
upsert. Projects are published through `POST /v1/projects` for the sources that hold them, behind the
one-home gate (§14 D21). A failed upload keeps its spool file. Not built: endpoint sources on other hosts, and
the source-side size pre-check (§5.2) (`docs/Implementation-plan.md` §6).

Ingestion is multi-point: any number of sources (§4.7), on any number of hosts, submit to the
one index at the hub. RAGNART therefore has two roles.

| | Ingestion side, one per ingestion host | The hub, one |
|---|---|---|
| Runs | The sources of that host (§4.7) | The pipeline (§5), the index and the search API |
| Stores | Its state database (§4.3) | Qdrant and the hub registry (§4.8) |
| Decides | What to submit, re-submit and delete: scanning, exclusion (§1.2), change, move and duplicate detection (§4.2), deletion guards and tombstones (§10.2), reconciliation of its own items (§10.3) | Nothing about the lifecycle of a source's items |
| Compute | Light: walking, hashing, rendering a connector-native format to text (§4.7) | Parsing, chunking, embedding, reranking |

- **Five requests per item.** An ingestion side asks the hub to upsert an item with its content, to
  delete an item, to change an item's key, to change an item's metadata, or to change an item's
  source. The last three rewrite payload fields only: a move of a file changes its key (§4.2), a
  correction of `owner` or `project_id` changes metadata, and the move of a ragnart project to
  another source changes the `source_id` of its managed files (§4.5). None carries content, and
  nothing is re-embedded. An ingestion side also publishes the records of the ragnart projects it
  holds, with their references (§4.5).
- **The hub is the only writer of the index.** No source writes to Qdrant.
- **The hub is agnostic to a source's lifecycle.** It does not track which items a source should
  have, and it does not notice that a source is silent. A source's items stay in the index until that
  source deletes them. A laptop that is offline for weeks keeps its items searchable.
- **The hub keeps nothing of an item beyond its chunks** (§4.4). Submitted content is held only in
  transit, from the request until the item is ingested, and is then deleted.
- **What the hub knows spans sources.** No single source can see these: the registry of sources, the
  published ragnart project records that scope resolution needs (§8.8), and the write-time gates
  (§12).
- **An ingestion host is thin in compute, not in logic.** It carries the scanners, the sync and a
  state database. It carries no parser for general document formats and no embedder.

The hub owes a source three services, none of which is bookkeeping:

| Service | Use |
|---|---|
| A listing of the source's own items: key, content hash, chunk count, pipeline version | The source reconciles against it (§10.3), finds the items to re-submit after a pipeline change (§4.1), and rebuilds a lost state database from it |
| The status of a submitted item | Parsing and embedding take minutes, so an upsert is asynchronous |
| The current pipeline version, the accepted file types and the admission limits | The source re-submits the items whose chunks carry an older version (§4.1), and applies the size limit before it submits (§5.2) |

Removing the items of a source that no longer exists, such as a lost or reinstalled machine, is a
maintenance operation by the operator on the hub (§10.2). It is not part of the API.

## 3. The layered model

RAGNART is organised in four layers. The first two are the same for every kind of material;
the last two are where kinds of material differ.

| Layer | Scope | Content |
|---|---|---|
| **Data and access model** | Global | Identity, sources, metadata, projects, grants (§4, §6) |
| **Ingestion mechanics** | Common | One pipeline in the hub, versioned (§5), fed by the sources through their connectors (§2.3, §4.7) |
| **Ingestion policy** | Per kind of material | **Static**: the inclusion rules of `ragnart.yaml` and the exclusion rules of §1.2, unattended, every 6 hours. **Curated**: only what a project deliberately keeps (§7) |
| **Curation (lifecycle)** | Per tier | Archive: blunt, mirrors the filesystem. Runtime: session records. Transient: material in flight (§7) |

## 4. Data model

### 4.1 Versions

Two version numbers govern the data. They are independent.

- **Schema version — the data model.** It names the set of tables and columns and the Qdrant
  payload layout. Each database stores its own value: a state database, and the hub registry
  (§4.8). Each change to the data model is a numbered migration that runs once and records
  the new version.
- **Pipeline version — how a submitted item became chunks and vectors.** Recorded **per item**.
  The current pipeline is **`p1`**, defined in §5.

**What a pipeline is.** A pipeline is what the hub does to a submitted item (§2.3): parsing,
admission, chunking and embedding (§5.1 to §5.4). It starts at the submission. What a connector does
before that point (enumeration, exclusion, hashing, and rendering its native format, §4.7) is not
part of the pipeline. Retrieval (§5.5) is not part of it either.

- **There is one current pipeline, and it is the hub's.** It is the same for every source: a PDF is
  processed identically whichever source submitted it (§4.7). Processing that differs by file type,
  such as flat or parent–child chunking (§5.3), is a choice inside the pipeline, not a second
  pipeline. There is no pipeline per connector and none per source.
- **The version is recorded per item.** A change is applied in batches, so one source is partly on
  the old version while the change runs, and a record per source would be too coarse. The hub stamps
  `pipeline_version` on every chunk it writes (§4.4) and returns it in the listing of a source's
  items (§2.3). The ingestion side keeps a copy per item (§4.3).
- **A version changes only on a major event** (D8): a change of parsing, admission or chunking that
  makes items stored under the old version incompatible with new ones or changes results
  significantly, or a different embedding model. Bug fixes and improvements to the current pipeline
  are part of `p1` and do not create a version. A version therefore names a compatibility epoch, not
  an exact recipe: two `p1` items may have been processed by different revisions of the code.

**Status.** Built: the per-item `pipeline_version` column of the state database, and `pipeline_version`
on every chunk, stamped by ingestion; every item and chunk is `p1`. Built (v0.3, sessions B1 and B2): `pipeline_version` in the listing that the hub
returns to every source, and the ingestion side re-submits the items whose `pipeline_version` is older than the hub's (the NAS side at most 200 per run, `rag.nas_ingest.resubmit_batch_size`).

**Two kinds of change, handled differently.**

| | Parsing, admission or chunking | Embedding model |
|---|---|---|
| What changes | The chunks | The vectors |
| Redoing it needs | The item's content, so its source submits the item again | Only the chunk `text`, which the hub holds in the payload (§4.4) |
| Old and new together | Yes. Both are valid chunks with comparable vectors, so the collection can hold a mix | No. A query is embedded once, and one query vector cannot be compared with the vectors of two models |
| Applied | Per item, in batches, as each source re-submits | To the whole collection, by the hub, without the sources |

- **A change of parsing, admission or chunking.** The hub announces its current version (§12). A
  source re-submits the items whose chunks carry an older version, when it is next reachable.
  The hub keeps no record of an item beyond its chunks and waits for no source, so the items of an
  offline source stay on the old version, and stay searchable, until that source returns.
- **A change of embedding model.** It is never applied item by item and the collection never holds
  the vectors of two models in one searchable set. The hub re-embeds from the stored `text`; no
  source re-submits and nothing is parsed again. The mechanism (a second vector or a second
  collection, and the switch between them) is defined when a second embedding model is chosen
  (§13.3).

**Rendering version.** A connector that renders its own format to text (§4.7) has a rendering
version of its own, kept on the ingestion side. A change to the rendering makes that source
re-submit the affected items. It is not a pipeline version change.

**Retrieval has no version on items.** A change to retrieval (§5.5), such as the fetch sizes, the
fusion, the reranker or the shaping of results, changes no stored data, so no item is on an old or a
new version of it. It is a change to the settings of the search service, and the labelled set is its
gate (§9.3).

### 4.2 Identity of an item

An **item** is one ingestable thing: a file, a session, a captured page or an upload. It is
split into many chunks (Qdrant points).

**The identity of an item is the pair (source, key).** The source is the configured connector
instance the item comes from (§4.7). The key is the item's natural key, in the format its connector
defines. A key is unique within its source and makes no claim outside it: the same path on two
machines is two items, in two sources.

**Status.** Built: the keys below, except the `ragnart://` key of a ragnart project's managed files
(v0.4; today every file is keyed by its full path). Built (v0.3): the source is part of the identity.
Every item carries its `source_id` (§4.3, §4.4), and delete and replace filter on `source_id` and key. An endpoint item's `source_id` is the source whose secret submitted it, never a value from the request (§12; built, v0.3). The three hub sources share one state database; the endpoint sources keep theirs on their host (§4.8). Every delete and replace filters on `source_id` as well as the key, so a key is unique within its source.

| Connector | Item | Key |
|---|---|---|
| Filesystem | File | Full path, e.g. `/mnt/nfs/Florian/…/report.pdf` |
| Claude Code | Session | `claude-session://<session uuid>` |
| Claude Code | Subagent transcript (direct or workflow agent) | `claude-session://<session uuid>/agent-<id>` |
| OpenCode | Session | `opencode://<session id>` |
| Filesystem | File in a ragnart project's `resources/` or `collateral/`: added file, kept capture, note, collateral | `ragnart://<project id>/<path relative to the project folder>`, e.g. `ragnart://3f9c2e1a-…/resources/report.pdf`. The origin of a copy or a capture is recorded in the project's provenance (§4.5) |

**Claude Code transcripts.** The scanner derives the key from the transcript's path
(`claude_jsonl_key()` in `ragnart/scanner.py`). Workflow journals are excluded: a journal is an
orchestration log, not a conversation, so the scanner never discovers it.

| Transcript path (under `~/.claude/projects/<project dir>/`) | Key, or outcome |
|---|---|
| `.../<uuid>.jsonl` (main session) | `claude-session://<uuid>` |
| `.../<uuid>/subagents/agent-a1bbf3e4e190989ea.jsonl` (direct subagent) | `claude-session://<uuid>/agent-a1bbf3e4e190989ea` |
| `.../<uuid>/subagents/workflows/wf_08ea07fa-b92/agent-<id>.jsonl` (workflow agent) | `claude-session://<uuid>/agent-<id>` |
| `.../<uuid>/subagents/workflows/wf_08ea07fa-b92/journal.jsonl` (workflow journal) | Excluded: not indexed |

A subagent transcript's payload `session_id` is still the parent session's uuid. A journal row
already in the state database is removed by the normal deletion path (§10.2) once the scanner no longer discovers it.

**One session, one item.** A session's uuid can be found in more than one transcript file: for example in two Claude Code project folders after a session was copied, or in a git worktree. The copies are one item. The newest copy by modification time wins; an exact tie goes to the greatest path. The winning copy's path is the item's `origin_path` (§4.3). The other copies are duplicate locations and are not indexed. This is the Claude Code connector's duplicate rule (§4.7), and it holds within one source: the same uuid submitted by two sources is two items.

**One turn, one place** (not built; v0.4). A forked session copies the turns of the session it was forked from, under its own session id, and keeps each copied turn's message id (verified with Claude Code 2.1.327). The connector indexes a turn once, by its message id: a turn whose message id already belongs to another session of the same source is not indexed again, so a fork's item holds only the turns added after the fork. Without the rule, the shared turns would be found twice in every search. The OpenCode equivalent is not verified.

Three kinds of identifier exist, and only the natural key, within its source, serves as identity:

| Kind | Example | Why it is or is not the key |
|---|---|---|
| Natural key | path, session id | Already exists, and is unique per item within its source: needed to delete the item's chunks, cite it, and place it in a project |
| Content hash | MD5 of the bytes | Not unique per item (identical copies collide) and changes on every edit; stored as an indexed **attribute** |
| Surrogate id | a generated UUID | Adds a mapping table but no capability: keeping it stable across a move needs the content hash anyway. Only useful for references held outside RAGNART; not used for items |

The file name alone is not a key: of 11,772 indexed NAS files, 6,495 names are distinct and
7,204 files (61%) share a name with another file (`README.md` 219 times,
`Timeline--meeting-notes.md` 254 times).

**Key, locator and display are three things**, and each connector defines its own (§4.7):

| | What it is | Guarantee |
|---|---|---|
| Key | The item's identity within its source | Unique within the source |
| Locator | What the connector needs to read the item's content (`origin_path`, §4.3) | Meaningful to its own connector only. It is opaque everywhere else, it never leaves the ingestion side, and the only valid operation on it is to hand it back to that connector |
| Display | A label for people: a file name, a session title | None |

For a file, the key and the locator are the same path. For a Claude Code session they differ: the
key carries the session uuid, and the locator is the transcript's path. For a file in a ragnart
project's `resources/` or `collateral/` they differ too: the key carries the project id and the path within the
project folder, and the locator is the file's full path, so a move of the project folder changes the
locator only (§4.5). No format is universal: a
key or a locator makes no claim outside its own source.

**Moves.** A move of a ragnart project's folder is not a move of its files: their keys do not change,
and the sync rewrites their locators. Moving a file within a ragnart project's `resources/` and
`collateral/`, and moving any other file, is a move in the following sense; for a file of a ragnart
project, the commit plays the part of the sync. A move is a one-to-one match within one sync of one source: exactly one completed file that vanished
and exactly one new file share a non-empty `content_hash`, and that hash is not the empty-file MD5
(`d41d8cd98f00b204e9800998ecf8427e`). Session keys never take part. Any other case is a deletion
plus a new item: several vanished or several new files with the same hash, a tombstoned item (§10.2),
or a file that was both moved and edited between two scans, whose hash changed. A move rewrites the
key in one operation in the state database and in the `source_path` payload of its chunks; nothing
is re-embedded. A source on another host asks the hub for the payload change (§2.3). A move is not
counted by the mass-delete guard (§10.2, rule 3).

**Sessions** are keyed by session id because their files move: `~/.claude/scripts/
project-chats.sh` relocates a session's `.jsonl` between Claude Code project folders. With the
session id as key, such a move changes only where the transcript is read from (`origin_path`). A
ragnart project includes a session by a reference to its id (§4.5), so the move changes nothing
there either.

**Hashing** is MD5 (change, move and duplicate detection, not security). A NAS file is hashed when
the scan first discovers it, so that a move can be found in the same sync, and ingestion hashes it
again when it writes the item's chunks. A file that cannot be read at discovery is queued without a
hash, and it cannot take part in a move until ingestion sets one. OpenCode items have no hash.

### 4.3 Per-item metadata (state database)

One row per item in the `ingestion_queue` table of a state database. A state database belongs to one
ingestion host and covers the sources that host runs (§2.3, §4.8). Today there is one, on the NAS,
opened by Server 2. Changing a value here is cheap.

**Status.** Built. The columns exist in schema version 3 (§4.1), `source_id` included. New ingestion writes them, and the
metadata backfill and the source backfill (`utils/rag_source_backfill.py`) give existing rows the same values. OpenCode rows have no `size_bytes`,
`content_hash` or `hashed_at`: their content is read through the OpenCode API, not from a file.
`missing_since` is set by tombstoning and cleared when the item reappears (§10.2, rule 2).

| Field | Meaning |
|---|---|
| key | Natural key (§4.2), primary key; the column is `file_path` |
| `source_id` | The source the item belongs to (§4.7) |
| `source_type` | `file`, `claude_session`, `opencode_session`, `web_capture`, `upload`, `collateral` |
| `file_type` | Extension class: `pdf`, `md`, `txt`, …. For sessions: `claude_chat` (Claude Code), `opencode` (OpenCode) |
| `project_id` | The ragnart project whose `resources/` or `collateral/` holds the item (§4.5), or `global` for every other item (§4.6). A session is always `global`: a ragnart project includes it by reference |
| `owner` | Accountable person (metadata, §6) |
| `size_bytes`, `content_hash`, `hashed_at` | Change, move and duplicate detection |
| `first_seen_at` | When RAGNART first discovered the item |
| `modified_at` | Per type: file mtime; session = time of last activity; capture = fetch time |
| `ingested_at` | When its chunks were last written |
| `pipeline_version` | The ingestion side's copy of the version the hub stamped on the item's chunks: `p1`, … (§4.1) |
| `chunk_count` | Number of points written; used by reconciliation (§10.3) |
| `status`, `error_msg`, `skip_reason`, `retry_count` | Queue state |
| `missing_since` | Tombstone: when the item was last seen missing (§10.2) |
| `origin_path` | The locator (§4.2): where the item's bytes are read. A NAS file: the same as its key. A Claude Code transcript: the real `.jsonl` path (its key is `claude-session://…`). OpenCode: `NULL`, read through the API |
| `launch_dir` | Session launch directory: for Claude Code, the first non-empty `cwd` of the transcript; for OpenCode, `location.directory`. Recorded verbatim, never normalised, including a stale or deleted directory. Metadata only: no rule reads it. `NULL` for files |
| origin details | Sessions: `session_id` and `session_title` in the payload (§4.4); there is no separate session start-time field, `first_seen_at` is when RAGNART first saw the item. Files of a ragnart project: the origin of a copy or a capture (URL or original path, and when it was added) is in the project's provenance (§4.5), not here |

### 4.4 Per-chunk payload (Qdrant)

The payload is copied onto every chunk of an item, so it carries only what a search needs to
filter on or to display.

**Status.** Built. Every point carries `source_id`, `source_path`, `project_id`, `owner`, `source_type`,
`file_type`, `modified_at`, `ingested_at`, `content_hash` and `pipeline_version`, with the indexes in the
table below: keyword indexes on `source_id`, `source_path`, `project_id`, `owner`, `source_type`,
`file_type`, `content_hash` and `pipeline_version`, datetime indexes on `modified_at` and `ingested_at`:
ten indexed fields. The metadata backfill (`utils/rag_metadata_backfill.py`) gives existing points these
fields without re-embedding, and the source backfill (`utils/rag_source_backfill.py`) gives existing
points `source_id` and `pipeline_version` the same way. Also
carried: `file_name`, `chunk_index`, `text`, `parent_text`, `page`, `tag`, and on sessions
`session_id`, `session_title` and `project` (a folder or session name, not a project id).

| Field | Indexed | Purpose |
|---|---|---|
| `source_id` | keyword | The item's source (§4.7); with the key, the item's identity |
| key (`source_path`) | keyword | The key within the source; delete and replace by `source_id` and key |
| `project_id` | keyword | Access and scope filter |
| `owner` | keyword | Metadata, display, optional filter |
| `source_type`, `file_type` | keyword | Metadata filters |
| `modified_at`, `ingested_at` | datetime | Time filters |
| `content_hash` | keyword | Collapse identical copies in results |
| `pipeline_version` | keyword | The pipeline version that made the chunk, set by the hub (§4.1) |
| `file_name` / title | — | Display |
| `chunk_index`, `text` (child, embedded), `parent_text` (returned) | — | Content |

The connector type is not copied onto chunks: the hub registry maps a `source_id` to its
connector (§4.8). The locator is not in the payload (§4.2).

Project tags are **not** copied into payloads (§4.5). The current fields `page` (always `0`)
and `tag` (always empty) carry no information; they are either implemented (a real page number
helps citations) or dropped.

### 4.5 ragnart projects

**Status.** This section is the design. Built in v0.2, to an earlier model: marker discovery by the
walk of the crawl roots, tags, and the hub registry tables (below). The built code gives every file under
the directory that holds `.ragnart/` the ragnart project's `project_id`, assigns each session to the
ragnart project whose directory encloses its `launch_dir`, and removes a ragnart project whose marker is
missing at once.
The model below is built in v0.4 (`docs/Implementation-plan.md` §6), with `ragnart init`, the
managed folders, references, registration, `ragnart mv` and `ragnart repair`. The RAGNART repository
holds the first ragnart project, with a hand-written marker.

**What a ragnart project is: a notebook.** It is the self-hosted counterpart of a NotebookLM
notebook (§1).

- **Many per topic.** "ACME-Q1" and "ACME-Q2" are two ragnart projects. A topic is a tag
  (`customer = "ACME"`), never a folder.
- **Material is added, removed and curated** (§7.3).
- **Questions are answered from it**, librarian-style: facts, references and quotes (§8).
- **Collateral is produced from it**: a summary, a commented index of its material, a presentation
  (§7.3).

**How it is built: a folder, as a git repository is.** Git is the model for the mechanics, not for
what a ragnart project is (§1.2).

```
ACME-Q1/                  the project folder, created by `ragnart init ACME-Q1`
  .ragnart/               state and metadata only, as `.git/` is (below)
  inbox/                  captured material, not indexed, expires (§7.5)
  resources/              the material: copies (added files, kept captures, notes) and references
                          (sessions, ragnart projects); indexed
  collateral/             documents produced from the material; indexed as derived material (§7.3)
  draft.docx              external: not part of the ragnart project, and not indexed
```

**`ragnart init <name>`.** It creates the folder `<name>` in the current directory, and inside it
`.ragnart/`, the three managed folders and a `project.toml` holding `name`, a new `id`,
`grounding = "strict"` and `capture = true`. It prints them, registers the project folder with its
ingestion host ("Registration" below), and publishes the ragnart project's record to the hub at once
(§2.3, §12). It is `git init <dir>`, with two differences: the name is required, and a folder `<name>`
that already exists is refused.

- **`init` is not a commit.** It indexes nothing, and nothing is created in Qdrant: a `project_id` is a
  value in the payload of chunks, and a new ragnart project has none. Its first chunks come from its
  first commit.
- **The record is published at `init`, not at the next sync**, so that search knows the ragnart
  project at once: a search at scope `project` answers "no good hits" instead of HTTP 422, and the
  hub's "one home" gate knows the `id`. When the hub cannot be reached, `init`
  still succeeds, says that the record is not published, and the next commit or sync publishes it. A
  new `id` is a fresh UUID, so the delay cannot cause a collision.

- **The current directory has no meaning.** It is only where the folder is created. A ragnart project
  created under `ACME/` has no relation to `ACME/` or to its files, and it can be moved anywhere
  later.
- **`init` never changes what is indexed.** The folder is new and empty, so nothing that already
  existed is adopted.
- `ragnart init --from <project> <name>` creates a ragnart project with a new `id` and a copy of the
  material and settings of another. A folder copied by hand keeps the `id` of its original and is
  refused ("Copies" below).
- **A ragnart project is created from inside a session**, by the agent following the `/ragnart`
  skill, which runs `ragnart init` and then adds the session to the new project (§7.3). Creation from
  a shell is the exception. `init` behaves the same either way: it knows nothing about sessions.

**Content: what the ragnart project manages.**

- **Its managed folders are `inbox/`, `resources/` and `collateral/`.** Its material is the content
  of `resources/` and `collateral/`, which its commits index; `inbox/` holds its working material,
  which is never indexed. Membership is by location: a file placed there by hand belongs to the
  ragnart project as one placed by `ragnart add` does, and a file removed from there leaves it
  (§7.3).
- **Its resources take two forms**: a copy of a file, or a reference to a session or a ragnart
  project, held as a reference file in `resources/` ("Resources: references and copies" below).
- **Every other file in the project folder is external.** The ragnart project does not know it, as
  git does not know an untracked file, and it is not indexed: the global crawl does not enter a
  project folder (§1.2, deviation D).
- **Nesting has no meaning.** A ragnart project whose folder lies inside another one's folder is
  external content to the outer one. The outer one neither contains it nor includes it in its scope,
  and it is not its parent: a ragnart project has no parent. A file belongs to the ragnart project
  whose managed folders hold it.
- **No marker changes what is indexed.** `init` is the supported way to make a ragnart project, and
  its folder is new. A marker that `init` did not make (written by hand, copied in, or `.ragnart/`
  moved out of a project) is accepted only in a folder that holds no indexed `global` item
  ("Registration" below). Otherwise the crawl would stop at it, and the folder's content would leave
  the index.
- **A code repository is not a ragnart project.** Its files are `global` material, found by the
  global crawl. A ragnart project about a repository is a folder of its own, created by `init`, and
  the sessions held in the repository are added to it explicitly (§7.4).
- **There is no mode.** `project.toml` has no `mode` key (§7.2).

**`.ragnart/`: state and metadata only.** It holds what would be lost if the project folder were
copied to another host and every RAGNART database were empty: what the operator wrote, and what was
recorded at the moment of a project action. Everything that can be rebuilt is kept elsewhere.

| Content | Written by | Holds |
|---|---|---|
| `project.toml` | The operator, and `init` once | Identity, settings and tags (below). RAGNART never rewrites it |
| `provenance/` | RAGNART: `add` and `capture` (§7.3) | One file per copy that RAGNART placed: its path in the project folder, its origin (a URL or the original path) and when it was added or captured. The folder is the authority on what exists. An entry whose file is gone is reported and ignored, and a file placed by hand has no entry |
| `dropped/` | `drop` (§7.5) | Items set aside until the grace period ends |

`.ragnart/` never holds parsing results, queue state, hashes, chunk counts or pipeline versions: they
belong to the state database of the ingestion side and to the hub (§2.3, §4.8), and a second copy
could disagree with them. It never holds a session's scope or grounding either: they belong to one
session, not to the ragnart project ("Sessions" below). RAGNART writes into `.ragnart/` one file per
entry, created atomically, and never rewrites a shared file (§10.6).

```toml
name = "ACME-Q1 strategy"          # required, human-readable
id   = "3f9c2e1a-6d1b-4c0e-9a57-2b8f0c4d7e11"   # written by `ragnart init`; never edited
grounding = "strict"               # optional: default grounding in this project (§8.2)
capture = true                     # optional: ambient capture into the inbox (§7.3)
inbox_expiry_days = 30             # optional: lifetime of an inbox item (§7.5)
customer = "ACME"                  # optional tags: any key = value
segment = "ISV"
```

**`id` is the immutable, unique ragnart project identifier.** It lives inside `.ragnart/`, so it
travels with any move or rename of the project folder.

**Address: the name.** People and commands refer to a ragnart project **by its `name`**
("ACME-Q1"), which is unique across the hub registry, compared without regard to case. The `id` is
its identity; the name is how it is addressed.

**Status.** Not built (v0.4). Built today: a ragnart project is addressed by the path of its folder,
and `name` need not be unique.

- **The path of the folder is metadata.** It says where the folder is now, on which source, and it
  changes whenever the folder moves (§4.2). A path still selects ragnart projects by where their
  folders are (§8.8), but a name, an `id` or a tag is the stable way to refer to one.
- **Uniqueness is checked when the record is published** (§2.3, §12). `init` refuses a name that the
  hub registry already holds. When the hub cannot be reached, `init` succeeds, and a name found
  taken at the first publication is reported on every sync, and the record is not published, until
  the operator renames the ragnart project. A name changed in `project.toml` to one already taken is
  refused in the same way, and the registry keeps the last good record (marker guards, below).
- **The folder's name is `name` at creation.** The two may diverge afterwards: `name` in
  `project.toml` is the authority.

**Resources: references and copies.** Every item of a ragnart project's material is a resource, in
`resources/` (or `collateral/`, §7.3). A resource takes one of two forms.

**Status.** Not built (v0.4). Built today: the `project_references` table of the hub registry, which is
empty; no code writes it yet.

| Form | For | Held in `resources/` as |
|---|---|---|
| **Reference**, the rule | A Claude Code or OpenCode session; another ragnart project; any future kind of item with a stable id | A reference file, `<label>.reference`: the target's kind (`session` or `project`), its id, a label (a session's title, a ragnart project's name), for a session whether it works in this ragnart project (`working = true`, below) and when it was added |
| **Copy**, the exception | A file or a folder; a web page, captured | The file itself, with its origin in `.ragnart/provenance/` |

- **The rule: a reference to a stable id.** Nothing is copied, and the target is included as it is
  now: a referenced session is included with every turn it later gains.
- **The exception: files are snapshotted.** A file is copied into `resources/` and ingested from the
  copy, as part of the ragnart project (§7.3). The original may be edited, moved or deleted, and the
  ragnart project must not change or lose material when that happens. A web page is captured for the
  same reason (§7.3).
- **Material made in the ragnart project**, notes and collateral, is written into its managed folders.
  It is neither referenced nor copied.
- **Live or frozen is decided per kind of material**, not by whether it has an id: a session is
  referenced because its past turns are never rewritten and its live record is wanted; a file is
  copied because it can be rewritten or vanish. A future connector declares which side its items are
  on.
- **One verb for both forms.** `ragnart add <item>` adds the item to the current ragnart project, and
  only to it: it copies a file and references a session or a ragnart project (§7.3). A ragnart
  project may be given by its name or by the path of its folder; either is resolved to its `id`, and
  only the `id` is stored. `session:current` names the running session: the CLI reads its id from the
  harness, so the model never types a session id. The mechanism is specific to each harness. Claude
  Code puts `CLAUDE_CODE_SESSION_ID` in the agent's shell: the uuid of the session's transcript, so
  its key is `claude-session://<uuid>` (verified with Claude Code 2.1.326). OpenCode's shell tool puts
  `OPENCODE_SESSION_ID` (`ses_…`) in the environment of every command the agent runs, so its key is
  `opencode://<id>`; it is set after any plugin hook and cannot be overridden (OpenCode source,
  `packages/core/src/tool/plugin/shell.ts`, since v2.0.19; the installed server is v2.0.28). It is
  not in OpenCode's user documentation, so an upgrade is checked against it. octmux adds nothing to
  this: the agent's commands run under the OpenCode server, not in octmux's tmux pane.
- **A reference file is a resource like any other.** Membership is by location: deleting a
  `.reference` file by hand removes the reference, as deleting a copy removes it, and `ragnart drop`
  sets it aside as it does a copy (§7.5). A commit does not ingest a reference file as text: it reads
  the target from it and publishes the ragnart project's references (§7.3, §12).
- **A reference widens scope, never access.** A referenced item or ragnart project is searched only
  for a principal who may read it under its own access rule (§6.1).
- **References between ragnart projects are not transitive.** When A references B, a search in A
  covers B's material, including the sessions B references, and does not follow B's references to
  other ragnart projects.
- **A missing target is reported, never dropped.** The sync reports a reference to an id that no
  ragnart project or session carries, and search ignores it. A session counts as missing only after a
  run of its connector that could have found it, because a session is referenced before it is first
  ingested.
- **"Reference" is the only term.** The words "link" and "ref" are not used for it. A symbolic link is
  a filesystem construct and never creates a reference: how a file reached through a symbolic link is
  ingested is a matter of the filesystem connector, as for any file (§1.2, deviation C; §4.7).

**Sessions.** A Claude Code or OpenCode chat session relates to ragnart projects in two distinct ways.

- **A session works in at most one ragnart project, its current project, for its whole life.** A
  session gets its current project once: when it starts in a project folder (the session-start
  hook, §7.4), when it creates a ragnart project, or when it opens an existing one (§7.3). It never
  changes it. The `ragnart` commands it runs act on its current project only.
- **The current project is recorded in the ragnart project, not in the session.** Becoming a
  session's current project always adds a reference to the session to that ragnart project, marked
  `working = true`; a reference added as material by an explicit `add` is not marked. A session works
  in at most one ragnart project, so at most one working reference to it exists. The session-start
  hook finds the current project by location, when the session started in the project folder, or
  by a local lookup: among the registered project folders of its host, the one whose reference files
  hold a working reference to the session's id. A session is always on the host whose sources hold
  its ragnart project (§10.6, R14), so the lookup needs neither the hub nor the network.
- **There is no separate store of session state.** Three kinds of information meet in a session, and
  each has its place: the session's own data (turns, tool calls) belongs to its harness and is read by
  its connector (§4.7); the ragnart project's settings, including the default scope and grounding,
  are in `project.toml`; the current project is the working reference above. A change of scope or
  grounding by the operator lasts for the conversation only: after a compaction or a resume, the
  session falls back to the ragnart project's defaults, and the status line shows it (§8.4).
- **Any session can also be referenced as material by other ragnart projects**, by an explicit `add`
  there: a `global` session, or one that works in another ragnart project. Being referenced by a
  ragnart project is not working in it.
- **Ingestion does not change.** Every session is ingested by its connector as one `global` item of
  its owner, keyed by its id (§4.2), whether or not any ragnart project references it. A reference
  re-ingests nothing, re-embeds nothing and writes nothing to Qdrant.
- **A session started in a project folder works in that ragnart project.** The session-start hook
  (§7.4) finds the closest `.ragnart/` at or above the session's working directory, makes that
  ragnart project the session's current project, and adds the session to it, once, when the session
  starts.
- **`launch_dir` is metadata only** (§4.3). No rule compares it with a project folder.
- **A search matches a referenced session by its `session_id`**, so the session's subagent
  transcripts are included with it (§4.2).

**Registration.** A ragnart project's folder may lie anywhere on a source that RAGNART can write to,
inside or outside the crawl roots, and under an excluded directory.

- **`init` registers the project folder** with the ingestion host of its source. Each sync of a
  filesystem source reads the registered project folders, and also walks its crawl roots. The marker
  is read before the walk prunes a directory's children, so a rule on the children cannot hide it,
  and the walk does not enter a project folder (§1.2, deviation D).
- **A marker the walk finds that is not registered** is decided by its `id`:
  - a known `id` at a new path is a registered ragnart project that moved ("Moving" below), when its
    registered path no longer carries the marker; otherwise it is a copy ("Copies" below);
  - an unknown `id`, in a folder that holds no indexed `global` item, is registered as a new ragnart
    project;
  - an unknown `id`, in a folder that holds indexed `global` items, is refused. The sync logs an
    ERROR on every run until the marker or the content is removed, and walks the folder as if the
    marker were not there, so nothing leaves the index. Turning an existing folder of material into
    a ragnart project is not supported: `init` a new one and `add` what is wanted, which copies it.
- **`.ragnart/` is never ingested**, whether its folder is a ragnart project or a refused one:
  `config/ragignore` excludes it.
- **`ragnart repair <path>`** registers a project folder that was moved by hand to a place no
  registration or walk covers (`git worktree repair`).
- A marker with the reserved id `global` is logged at ERROR and ignored.
- `scope`, `grounding`, `capture` and `inbox_expiry_days` are reserved keys: they are not tags and
  are not stored. The `/ragnart` client (v0.4) reads them from `project.toml`.

**Keys of managed files.** A file in a ragnart project's `resources/` or `collateral/` is keyed
`ragnart://<project id>/<path relative to the project folder>`, and its full path is its locator
(§4.2). Its key does not change when the project folder moves. Every other file, including an
external file in a project folder, keeps its full path as its key.

**One home.** A ragnart project lives in exactly one place: one source and one path.

**Status.** Built: `home_source_id` on each project in the hub registry, and one source holds ragnart
projects, with a duplicate rule within one scan ("Marker guards" below). Built (v0.3): the
write-time gate: a project id homed on another source is refused with 409 first, and then a publish request from a source whose project support is not built (every source but `nas`; endpoint project support is v0.4) is refused with 409 (§12), so no endpoint source homes a project. The NAS publishes its records through `POST /v1/projects` (v0.3, session B2).

- **The source is tree-shaped and writable.** RAGNART writes `.ragnart/` and the managed folders
  (§7.3). A source that RAGNART can only read cannot hold a ragnart project, and its items are `global`
  (§4.6).
- **Sharing is a grant, never a replica.** A ragnart project that several people use lives on a
  hub source (§4.7), and each of them holds a grant on it (§6.3). It is not copied to their
  machines, and no machine ingests it a second time. A ragnart project on an endpoint source belongs
  to that source's owner and is not shared.
- **A second sighting of a project id is refused.** A project id that is homed on one source and is
  then published by another (§12) is refused and reported; the registered home keeps it. The cause
  does not matter: a cloned repository, a restored backup and a folder synchronised between machines
  all carry the marker with them. The managed folders under the refused marker are ordinary folders:
  their files are ingested as `global` items of their owner, under the exclusion rules.

**Moving a ragnart project.**

- **On one ingestion host**, by `ragnart mv` or by hand: the next sync finds the `id` at the new path,
  gives its `projects` row the new path and rewrites the locators of its managed files. Their keys do
  not change, so nothing is matched by hash and nothing is re-embedded. Its references are
  `.reference` files in `resources/` and move with it. A folder moved by hand to a place the sync does not read is found
  again by `ragnart repair`; until then its marker is not found, and the marker guards apply.
- **To another source**, only by `ragnart mv <project> <destination>`. It copies the project folder,
  verifies the copy, releases the old home and claims the new one. The hub rewrites the `source_id` of
  the managed files' chunks (§2.3), so nothing is re-embedded, there is no window in which the `id` is
  refused, and grants on the `id` survive. A project folder moved by hand to another source is refused
  there as a second sighting until the operator runs `ragnart repair` at the new place, which claims
  the new home once the old home no longer finds the marker. The destination must be a writable
  source with a running ingestion side (§2.3).

**Copies.** A project folder copied by hand carries the `id` of its original. Within one source the
registered path keeps the `id`, and the copy's marker is logged at ERROR and ignored; across sources
the copy is refused as a second sighting. `ragnart init --from` is the way to start a ragnart project
from another.

**Marker guards.** A ragnart project is protected as an item is (§10.2): a marker that cannot be
read, or that is not seen, neither removes the ragnart project nor changes what it holds.

**Status.** Not built (v0.4). Built today: an existing ragnart project whose marker is missing or
malformed is removed in the same sync, with its tags and its `project_links` and `source_links`
rows, and its items are re-attached. Of two markers with one id in a scan, the first in walk order
keeps it.

- **A malformed marker of a folder that holds no registered ragnart project** (invalid TOML, or
  `name` or `id` missing or not a string) is logged at ERROR and ignored. The folder holds no ragnart
  project.
- **A registered ragnart project whose marker turns malformed or unreadable keeps its last good
  record.** The sync logs an ERROR on every run, and nothing about the ragnart project changes until
  the marker can be read again. The reason is that of deviation G (§1.2): a rule that cannot be read
  must neither admit nor delete.
- **A registered ragnart project whose marker is not found is tombstoned, not removed.** Its
  `missing_since` is set at the first sighting. Its record, its tags and its references stay as they
  are. The first sync more than 24 hours later removes it. A marker found again before that, by the
  sync or by `ragnart repair`, clears the tombstone. As for items, this happens only when no root was
  skipped, and with `--user` only for that owner's ragnart projects (§10.2, rule 1).
- **Removal.** The ragnart project's `projects` row, its tags and its published references go. The
  sessions it referenced are unaffected: they are `global` items. A file still in a managed folder of
  a removed ragnart project is an ordinary file: it is ingested as a `global` item under its full
  path, and its `ragnart://` item follows the deletion rules of §10.2.
- **Two markers with one id in one source.** The registered path keeps the id, and the other marker
  is logged at ERROR and ignored. This is the rule of "One home" above, applied within a source. When
  neither path is registered, the first in walk order keeps it. When the registered path has no
  marker any more and exactly one other marker carries the id, the ragnart project has moved.

**Tags** are arbitrary `key = value` pairs: every key except the reserved `name`, `id`, `scope`,
`grounding`, `capture` and `inbox_expiry_days`, and except `mode` and `inherit_access`, which stay
reserved and ignored so that a `project.toml` written before this model does not gain a tag. They are
stored as rows `(project_id, key, value)`. On each sync the tag set read from `project.toml` replaces
the stored set: changing `industry = "ISV"` to `segment = "ISV"` deletes one row and adds one, for
the same `project_id`. A tag filter is resolved to a set of ragnart project ids in the database
before the search, so tag changes never touch Qdrant.

**Tables.** The hub registry (§4.8) holds `sources` (id, kind, connector, owner binding or owner, and `secret_hash`, the SHA-256 of an endpoint source's secret), `projects` (id, path, name, owner and `home_source_id`; also the reserved `global` row, §4.6), `project_tags`, `project_references` (the published references: `project_id`, `kind` `session` or `project`, `target_id` and `working`) and `grants` (§6). The tables have no `parent`, `mode` or `source_links`. No code writes `project_references` or `grants` yet: references are written from v0.4, and `projects.name` is unique without regard to case from v0.4 (see Address: the name, above).

These tables are records published for retrieval: scope resolution and the project listing read them
(§8.8, §12). `project.toml` and the reference files in `resources/` stay the authority. Their place is the hub
registry (§4.8), where `projects` also records each ragnart project's home source and each ingestion
side publishes its records (§12). Built today: the NAS sync publishes its project records, and removes them, through `POST /v1/projects` and
`POST /v1/projects/remove`; the API writes the `projects` and `project_tags` tables, and the search service reads the registry
read-only. Endpoint sources cannot publish (§12).

**Items outside ragnart projects** are ingested and searched exactly as in the single-user RAG. Every
item that is not in the managed folders of a ragnart project follows the baseline data model (§4.6)
and the current RAG retrieval semantics, now and in the future.

### 4.6 Baseline data model

Every item outside every ragnart project follows the baseline data model. An item inside one follows
§4.5. The baseline model stays the model for every item not placed in a ragnart project, now and in
the future.

**Status.** Built. An item outside every ragnart project carries `project_id` `global`, and its
retrieval is the existing RAG's.

#### (a) What it is

The data model of every item outside any ragnart project. Today that is all items: NAS files, Claude
Code sessions and subagent transcripts, and OpenCode sessions. In future it is every item not placed
in a ragnart project.

#### (b) Its data model

- **Natural key and location.** The natural key of §4.2, within the item's source, is the identity. `origin_path` (§4.3) is
  where the item's bytes are read. A session's uuid is one item even when several transcript files carry it (§4.2).
- **Owner.** Taken from the registration of the item's source (§4.7). For the built sources that is
  the crawl config (`ragnart.yaml`): the owner mapped to the NAS root or
  `claude_chats` root the item was found under. The owner is metadata, and for `global` items it
  also decides access (§6.5).
- **Item-level metadata.** The fields of §4.3, including `size_bytes`, `content_hash`, `modified_at`
  and `ingested_at`.
- **Launch directory.** Sessions carry `launch_dir` (§4.3), recorded verbatim and never rewritten or
  normalised, including a stale or deleted directory. It is metadata only. Files have none.
- **`project_id` is `global`.** One reserved value for every item outside a ragnart project. It is one
  value, not one per owner: tenancy is already carried by `owner`. It is not one per source either:
  the source is a separate field (`source_id`, §4.4), so the baseline items of one source are the
  items with `project_id` `global` and that `source_id`.
- **Reserved project row.** The `projects` table holds a row with `id` `global`. It has no path, and
  nothing inherits from it.

#### (c) Retrieval semantics

- Such items are searched by scope `all`, filtered by owner and file type exactly as `GET /v1/search`
  filters them today: by the requesting user's items and `la-familia` items when a user is given, and
  by the requested file types.
- Scope `project` never reaches `global` items, because they belong to no ragnart project.
- A path term in a `<selection>` names ragnart projects only (§8.1). Path selection over `global`
  items is not scheduled, and it stays out of the design: a path would need a per-item list of ancestor
  directories, an item can be reached through several paths (§1.2, deviation C), and a path goes stale
  when its item moves.

#### (d) Access

The owner rule governs `global` items, permanently and by design (§6.5): an item's owner may read it,
and anyone may read an item owned by `la-familia`. Grants govern items in ragnart projects, once any
exist (v0.6). These are two deliberately distinct access rules.

#### (e) Backwards compatibility

Retrieval is unchanged: `/rag` (§8.9) and `GET /v1/search` return what they returned before, with the
same owner and file-type filters. With no ragnart project defined, RAGNART is the existing single-user
RAG.

#### (f) What a crawl root is

A crawl root is a traversal starting point inside a filesystem source (§4.7), plus the owner mapping,
and nothing more. A root is not a source: the NAS is one source with several roots. There are no root
ragnart projects, no per-owner `sessions` ragnart project, and no attachment of items to a ragnart
project by a common path ancestor.

#### (g) How an item comes to belong to a ragnart project

A file never passes from `global` into a ragnart project. It is copied into the ragnart project's
`resources/` (§7.3), and the copy is a new item, ingested as any file is. The original stays a
`global` item, and identical copies collapse in results (§9.1). A session never passes into a
ragnart project either: it stays a `global` item, and a ragnart project includes it by a reference
to its id (§4.5). A reference changes nothing in the state database or in Qdrant.

### 4.7 Connectors and sources

**Status.** The definitions and the contract are the design. Built: the three connectors below, as
scanners in `ragnart/scanner.py` and parsers in `ragnart/ingest.py`, run as three sources (`nas`,
`claude-linux-shared-nfs`, `opencode-k11`) by one ingestion host. The contract is a code interface in
`ragnart/connectors.py`; the three connectors are registered in the hub registry, and the scanners and
parsers do not call the interface yet. Built (v0.3, session B1): two endpoint sources on another host, `MBP` (filesystem connector) and `claude-MBP` (Claude Code connector), run by the endpoint runner (`ragnart/endpoint.py`). Endpoint sources are registered with kind `endpoint`, hold no ragnart projects, and their items are `global`. OpenCode on an endpoint host: not built. Built (v0.3, session B2): the three NAS-side sources submit through the API. The filesystem source sends bytes. The Claude Code and OpenCode sources render a session with `ragnart/chat_render.py` on the ingestion side and send text, so the API accepts the `opencode` connector on its text route. A source without a fixed owner (the three NAS-side sources) sends the owner of each item, checked against the owners in `ragnart.yaml`; a source with a fixed owner (the Mac's) cannot.

A **connector** is a type: the code that knows one kind of store. A **source** is one configured
instance of a connector: a store, a place in it, an owner binding and a credential. Three sources
exist: the files on the NAS, the Claude Code sessions and the OpenCode sessions.

**What a connector defines.**

| # | Element | Definition |
|---|---|---|
| 1 | Key | The identity of an item within one source, in the connector's own format. The connector states how stable the key is, for example whether it survives a move |
| 2 | Locator | What the connector needs to read the item's content. Opaque outside the connector (§4.2) |
| 3 | Display | A label for people. No guarantee |
| 4 | Enumeration | The items of the source, with a change token for each (size, modification time and hash for a file; the time of last activity for a session), and a **completeness signal**: whether this enumeration saw everything. Deletion rule 1 depends on it (§10.2) |
| 5 | Fetch | The item's content, in one of the two submission forms below |
| 6 | Capabilities, each optional | Tree-shaped exclusion (`.ragignore`, §1.2). Holding ragnart projects, which needs a tree and write access (§4.5). Resolving a location to the ragnart project that contains it (§7.4) |
| 7 | Duplicate rule | Its own rule for several copies of one item, when it has one |

**The built connectors.**

| | Filesystem | Claude Code | OpenCode |
|---|---|---|---|
| Key | Full path | `claude-session://<uuid>[/agent-<id>]` | `opencode://<id>` |
| Locator | The same path | The transcript's path | None: the session is read by its id through the OpenCode API |
| Display | File name | Session title | Session title |
| Incomplete when | A root is not a directory, or yields no file while rows exist under it | A transcript root is not a directory, or yields no file while rows exist under it | The OpenCode API cannot be read |
| Capabilities | Exclusion, ragnart projects, location | None | None |
| Duplicate rule | A file reached through several links is queued once (§1.2, deviation C) | One session uuid is one item; the newest copy wins. Not built (v0.4): one turn, one place (§4.2) | None |
| Submits | Bytes | Text | Text |
| Renders its native format as | Nothing: it submits bytes | User and assistant text turns only; tool calls and tool results are dropped | User and assistant text, read from the OpenCode server API |

**Two submission forms, decided by who owns the format.**

| Form | For | Turned into text by |
|---|---|---|
| Bytes, with a declared file type | General document formats: `pdf`, `docx`, `pptx`, `ipynb` and the text-like extensions (§5.1) | The hub. Its parsers are chosen by file type, so a PDF is parsed identically whichever source submitted it |
| Text | A format native to the connector: a Claude Code transcript, an OpenCode session read from its API | The connector. No other connector produces that format, so there is nothing to keep consistent across sources |

A connector for a store whose documents have no bytes of their own exports them to a general format
and submits that. The hub publishes the file types it accepts, and the admission limits a source
applies before it submits (§5.2, §12).

**What a source is.**

- **Id.** `source_id`, chosen by the operator when the source is registered, and unique across
  RAGNART. It matches `[A-Za-z0-9][A-Za-z0-9-]*`: no `_` and no `:`. It keeps its case for storage and display, and every comparison ignores case, so `MBP` and `mbp` are one source. These ids are reserved and never source
  ids: `tag`, `name`, `all`, `project`, `web`, `none` and `global` (§8.8). The reserved list is written in
  `ragnart/schema.py` and compared without regard to case; it is enforced when a source is registered (built, v0.3).
- **Owner binding.** How the owner of each of its items is derived.
- **Credential.** One secret per source, checked at the write-time gate (§12).

| | Hub source | Endpoint source |
|---|---|---|
| Run by | The hub's own host | Any other host; typically a laptop that is reachable only some of the time |
| Owner | From the source's own mapping. The NAS source maps each crawl root to an owner (`ragnart.yaml`) | One owner, fixed at registration. A machine used by two people runs one source per person |
| Its items | `global` items under the owner rule, and ragnart projects that grants can share (§6) | Its owner's: `global` items, and ragnart projects that are not shared (§4.5) |

- **The NAS is one source.** Its crawl roots are traversal starting points inside it, not sources
  (§4.6).
- **An ingestion host may run several sources.** Server 2 runs three. A laptop that submits its files
  and its Claude Code sessions runs two.
- **A source is not a ragnart project and not a tenant.** It is a separate field on every item, and a
  filter. Whether access is later scoped by source belongs to the tenancy design, which is open
  (§13.3).

**Stores that are not POSIX filesystems.** A shared store may be an object store or a file-sharing
service that is reached only through its API. The contract assumes no filesystem semantics: such a
connector would take the store's own stable id as its key and the store's revision as its change
token, and it would hold ragnart projects only if RAGNART can write to the store. No such connector
is scheduled. The contract is defined so that one can be added.

### 4.8 What is stored where

**Status.** Built: the hub registry, `sqlite/hub_registry.db` in the data root, with its own schema
version (§4.1), holds the sources and the ragnart project tables. `rag_state.db` holds the per-item queue
only, at schema version 5: the project tables removed (by the gated tool `utils/rag_registry_migrate.py`); migration 4 in `_MIGRATIONS` carries no statements; migration 5 adds `request_id`. Built (v0.3, session B1): the intake table of the hub registry, with one row per request from an endpoint source, and the spool directory. Built (v0.3, session B2): the listing of every source's items, and the NAS ingestion on the API: `rag_state.db` of Server 2 is an ingestion-side state database, at schema version 5 (a `request_id` per queued row), and the hub registry's intake table, at schema version 3, carries the `owner`, `tag` and `project_id` of a request. A lost state database is rebuilt from the listing: `utils/rag_sync_nfs.py --rebuild-from-hub` on Server 2 (it refuses unless `rag_state.db` has no rows) and `--rebuild-from-hub` of the Mac runner.

**The hub holds what retrieval filters on or displays, and what gates a write. The ingestion side
holds what decides whether to submit, re-submit or delete.** That gives three stores.

| Store | Where | Granularity | Contents |
|---|---|---|---|
| Qdrant payload (§4.4) | The hub | Per chunk | `source_id` and key; `project_id`, `owner`; `source_type`, `file_type`, `modified_at`, `ingested_at`, `content_hash`, `pipeline_version`; the display fields; `text` and `parent_text` |
| Hub registry | The hub | Per source and per ragnart project | The sources: id, kind, connector, owner binding or owner, secret hash. The published ragnart project records: the tables of §4.5, with each ragnart project's home source. The current pipeline version. The status of items in transit. Grants, when tenancy is built (§6) |
| State database (§4.3) | Each ingestion host | Per item | Queue status, errors and retries; size, hash and hash time; `first_seen_at`; its copy of `pipeline_version`, and `chunk_count`; `missing_since`; the locator (`origin_path`); `launch_dir` |

- **Some fields exist on both sides**: the key, `project_id`, `owner`, the types, the times and the
  content hash. They are in the payload because search filters on them, and in the state database
  because the sync decides with them.
- **The locator never leaves the ingestion side** (§4.2).
- **The authority for a ragnart project is its folder**: `.ragnart/project.toml`, and the reference
  files in `resources/`
  (§4.5). The registry holds the published copy.
- **Each store can be inspected and repaired on its own.** The operator's maintenance tools act on
  the hub's two stores. A lost state database is rebuilt from the hub's listing of that source's
  items (§2.3).
- The hub registry is not a ragnart project's catalogue (§7.4), which is the listing of that
  ragnart project's items.

## 5. Pipeline `p1` and retrieval

`p1` is the current pipeline: the processing of §5.1 to §5.4, which turns a submitted item into
chunks and vectors and so decides what is stored. Its version rule is in §4.1. Retrieval (§5.5)
reads what the pipeline stored. It is described here and is not part of `p1`.

### 5.1 Parsing

The pipeline starts at the submission (§4.1). The hub parses the general document formats, which a
source submits as bytes. A connector renders its own native format to text before it submits; that
rendering belongs to the connector and is listed in §4.7. Built today: both run in
`ragnart/ingest.py` on Server 2.

| Input | Parser |
|---|---|
| `pdf`, `docx`, `pptx` | docling 2.89.0 `DocumentConverter`, exported to Markdown. The accelerator is set explicitly to CPU for each type, so the device is chosen in code and not by docling's automatic choice. PDF layout and table models run on the CPU. |
| `pptx` when docling fails | python-pptx 1.0.2, after LibreOffice conversion for OLE2 files. If python-pptx also fails, the item fails (§5.2). |
| `pdf`, `docx` when docling fails | No fallback. The item fails with `docling failed: …` and follows the retry rule of §5.2. |
| `ipynb` | RAGNART's notebook parser (cells as text) |
| Text submitted by a connector: a Claude Code or an OpenCode session | None. The connector has rendered it (§4.7) |
| All other included extensions | Read as UTF-8 text with replacement, then subject to the text-like rules of §5.2 |

### 5.2 Admission

Admission decides, before any embedding, whether an item is indexed. A rule that rejects an item
sets its queue row to `ignored`, and `skip_reason` records the rule with the measured value.
`ragnart status --ignored` lists these rows with their reason and retry count. The thresholds and
caps are the values of `rag.admission` in `ragnart.yaml`. The purge tools in `utils/` use the
same definitions, so they cannot drift from ingestion.

**Before queueing (scan time), on the ingestion side.** An item is considered only if its extension is in
`rag.scanner.include_extensions` (`ragnart.yaml`) and no exclusion rule of §1.2 matches it. An
already-indexed item that an exclusion rule now matches leaves the index through the sync (§10.2).

**After queueing (ingestion), in this order.** These rules run where the content is processed, in
the hub. The result is returned to the source (§2.3), whose queue row records `ignored` and the
reason. The size rule is the exception to where the rules run: a source applies it before it
submits, with the limit the hub publishes (§12), so that an oversized file is not uploaded only to
be refused (not built: the source-side pre-check is deferred. Today the hub refuses an oversized text-like upload at the door, with 413, and the ingestion side marks the row `ignored` and deletes the item's older points). The size, NUL-byte and
replacement-character rules
apply to text-like files only: every type except `pdf`, `docx` and `pptx` (docling), `ipynb`,
Claude Code `.jsonl` sessions and OpenCode sessions.

| Rule | Value | Result and `skip_reason` |
|---|---|---|
| Size | More than 5,000,000 bytes (`max_text_file_size_bytes`), checked from the file size before the file is read | `too large: N bytes > 5000000 byte cap` |
| NUL byte | A NUL byte in the first 8 KB of the file | `binary: NUL byte in first 8KB` |
| Parsing | `pdf`, `docx`: docling raises. `pptx`: docling and python-pptx both raise. | Retried; after 5 failed attempts, the last error (`docling failed: …`) |
| Replacement characters | U+FFFD above 0.5% of the decoded text (`fffd_ratio_threshold`, 0.005) | `binary: N% replacement chars` |
| Quality gate | Printable ratio below 0.85, all items (see below) | `binary content: N% printable chars` |
| Chunk cap | More than 5,000 chunks (`max_chunks_per_item`), all items. Checked after chunking and before embedding, so no embedding time is spent. | `too large: N chunks > 5000 chunk cap` |

**Quality gate.** The printable ratio is the share of characters in the parsed text that are
printable (`str.isprintable()`) or a tab, line feed or carriage return, and that are not U+FFFD.
Empty text, or text that is only whitespace, is not rejected: the item completes with no points.

**Retries.** A parse failure returns the item to `pending` with its retry count raised. The queue's
`max_retries` (default 5) decides when the item is ignored: the fifth failed attempt ignores it,
with the last error as `skip_reason`. The other rejections ignore the item at once, without retry.
A `pdf` or `docx` that docling cannot parse is never read as text.

### 5.3 Chunking

- Tokenizer: **tiktoken 0.12.0, `cl100k_base`**.
- **Parent–child** for most documents: parents of **1000 tokens, overlap 100**; each parent is
  split into children of **250 tokens, overlap 20**. The child is embedded; the parent is what
  a search returns.
- **Flat** chunks of **512 tokens, overlap 50** for `pptx`, `yaml`, `yml`, `csv`, and for any
  document shorter than **600 tokens**.

### 5.4 Embedding

| Vector | Model | Details |
|---|---|---|
| Dense (unnamed) | **bge-m3** served by Ollama on Server 2 | Model digest `790764642607`, F16, 566.7 M parameters, 1024 dimensions, cosine distance. Ollama runs with `OLLAMA_CONTEXT_LENGTH=2048`: input beyond 2048 bge-m3 tokens is rejected with HTTP 500, not truncated (§5.6). |
| Sparse `sparse_text` | **fastembed 0.8.0, `Qdrant/bm25`** | Qdrant IDF modifier |

**Storage.** Each chunk is one point in the `documents` collection (§7.1). A point holds the dense
vector and, when the collection has it, the sparse vector `sparse_text`. Its payload holds `text`
(the child, embedded), `parent_text` (the parent, returned), the payload fields of §4.4 that are
present today, and `page`, which is always `0`. Before an item's points are written, all points for
its `source_id` and `source_path` are deleted. The new points are upserted in batches of 256.

### 5.5 Retrieval

Retrieval is not part of the pipeline (§4.1). It reads what `p1` stored and changes no stored data,
so a change to it puts no item on a new version: it is a change to the settings of the search
service, gated by the labelled set (§9.3). The steps, as built in `ragnart/search.py` and
`ragnart/rerank.py`:

1. The query is embedded with bge-m3 (dense). When hybrid search is on and the collection has
   `sparse_text`, the query is also embedded as BM25 sparse vectors.
2. Hybrid search (`rag.hybrid`, on by default) runs a dense prefetch of 30 candidates
   (`dense_fetch`) and a sparse prefetch of 30 (`sparse_fetch`), fused by Qdrant's reciprocal rank
   fusion. Without `sparse_text` the query is dense-only.
3. Candidate count. With reranking on, hybrid keeps at most 30 candidates (`fetch_cap`), and
   dense-only fetches `min(limit × 6, 30)` (`fetch_multiplier` 6). With reranking off, hybrid keeps
   at most 50, and dense-only fetches `min(limit × 3, 50)`.
4. Reranking (`rag.rerank`, on by default): bge-reranker-v2-m3, served by llama-swap on Server 2,
   scores each candidate's child text, and candidates are ordered by that score. The request has a
   10-second timeout. If the reranker fails, the candidates keep their Qdrant order and a warning
   is logged. The response does not say so yet (§8.7).
5. The hits are collapsed by `content_hash` (§9.1), capped per source by `max_hits_per_source`
   (§9.2), and the first `limit` are returned, each with its `parent_text` as content. When a scope is
   given, the Qdrant query is also filtered by `project_id`, ANDed with the owner and file-type
   filters (§8.8).

The `score` field is the Qdrant score: the RRF value in hybrid search. The relevance signal is the
rerank score (§9.2), which a hit does not carry yet (§8.7).

### 5.6 Known properties

- Chunk sizes are counted in `cl100k_base` tokens, not bge-m3 tokens (§5.3). The embedder's limit is
  counted in bge-m3 tokens. Children (250 tokens) stay below the limit.
- Ollama runs with `OLLAMA_CONTEXT_LENGTH=2048` (§5.4). Flat 512-token chunks of code, YAML or
  non-English text can produce more than 512 bge-m3 tokens, and a pathological chunk can still exceed
  2048. Ollama rejects such input with HTTP 500 (`the input length exceeds the context length`); it
  does not truncate it. The item is retried, then ends `ignored` with the 500 as its `skip_reason`
  (`docs/RAG-troubleshoot.md` §2.13).
- Page, slide and cell numbers are not tracked: `page` is always `0`.
- There is no secret filter: credentials in files and sessions are embedded and returned.

## 6. Access model

### 6.1 Every item has exactly one project

Every item belongs to exactly one project, which is its access home: a ragnart project (§4.5) for a
file in that ragnart project's `resources/` or `collateral/`, or the reserved `global` project for every other item
(§4.6). The other files of a project folder are not indexed (§1.2, deviation D). Every session is a `global` item of
its owner, whichever ragnart projects reference it. A nested ragnart project has no relation to the
one around it (§4.5). NAS roots are not projects: a crawl root is a traversal starting point and the
owner mapping (§4.6). A source is not a project either (§4.7).

A ragnart project may reference other ragnart projects and sessions (§4.5). A reference widens what a
search covers and never who may read: a referenced ragnart project or session is searched only for a
principal who may read it under its own access rule. In a shared ragnart project, a principal is
therefore shown only the referenced sessions they may already read: sharing a ragnart project does
not share the sessions it references.

Access to an item in a ragnart project is defined **only through grants on that ragnart project**
(§6.3). Access to a `global` item is the owner rule (§6.5). Content shared with the family is either
an item owned by `la-familia`, which anyone may read (§6.5), or an item in a ragnart project granted
to each family member.

`owner` is the **accountable person** of an item: shown as metadata and given the default grant. In
a ragnart project it is not an access filter; for a `global` item it is the owner rule (§6.5).

**Why not filesystem permissions:** both NFS roots are mode `777`, nearly all files belong to a
single uid, and household members have no separate Unix accounts. File modes are recorded as
hints only.

### 6.2 Principals

A principal is a **person** or a **group**. Groups come from the identity provider (the groups
claim of an OAuth token), so team membership is managed there. Clients (Claude Code, OpenCode,
the CLI, Kotaemon) act **as the person** using them; they are not principals of their own.

### 6.3 Grants

- A grant gives a principal a role on one ragnart project. It does not reach a ragnart project
  nested in that one's folder: nesting has no meaning (§4.5), so nothing is inherited. There are no
  deny rules.
- Grants live in the hub registry (§4.8) and are managed through the CLI or API. `project.toml`
  holds structure only (the NAS is writable by anyone with access, so it cannot hold access
  rules).
- Grants exist only on ragnart projects homed on a hub source (§4.5, §4.7). A ragnart project on
  an endpoint source, and every `global` item of an endpoint source, is its owner's.

Example (a sales organisation):

```
~/Notebooks/EMEA-plans/      ragnart project, granted to group sales-emea   (regional plans)
~/Notebooks/ACME-Q1/         ragnart project, granted to Alice              (customer material)
~/Notebooks/EMEA-plans/ACME/ a ragnart project inside EMEA-plans' folder: granted to Alice only;
                             the EMEA team does not see it, because no grant is inherited
```

### 6.4 Roles

| Role | Rights |
|---|---|
| `read` | Search and read the project's items |
| `admin` | Manage the project's grants |

**Sharing and ownership transfer** are not implemented. The mechanism that fits the existing
primitives:

- A `share` role: its holder may add and remove grants on the project, up to their own level,
  never `admin`. Checked by the same seam:
  `authorize(principal, "grant", project)`. Grants record `granted_by` and `granted_at`.
- Ownership transfer is a database transaction: set the project's owner, add the new owner's
  default grant, optionally revoke the old owner's. Access is project-based, so Qdrant is not
  touched.
- Interfaces: `POST`/`DELETE /v1/projects/{id}/grants`, `POST /v1/projects/{id}/owner`;
  `ragnart grant | revoke | transfer`.

### 6.5 Enforcement

One function turns a request into a filter, `resolve_scope()` (built, `ragnart/scope.py`), and one
decides access, `authorize()`. No other code builds a scope filter. **Built:** the owner rule, as the
owner filter of `search_rag()` in `ragnart/search.py`, which applies to every scope, including items
inside a ragnart project. **Not built:** `authorize()` as a function, and the `grants` table, which is
empty and read by no code (v0.6). Under the owner rule an item's owner may always read it, and so may
anyone when the item's owner is `la-familia`.
This owner rule governs `global` items (items outside every ragnart project) permanently, by design,
not as a stopgap. Grants govern items inside a ragnart project, once any exist (v0.6). These are two
deliberately distinct access rules, not one rule with a temporary gap.
Authentication (OAuth with group claims) is added when a second user exists.
Multi-tenancy is designed now and built later: the data model (project ids on every chunk,
grants shaped for persons and groups) is in place from v0.2 so nothing has to be backfilled.
What would still be hard to retrofit for use by a team is listed in §15.
How sources enter the access model is open: whether access is also scoped by source is decided with
tenancy (§13.3).

## 7. Tiers and lifecycle

### 7.1 Three tiers, distinguished by lifecycle

| Tier | Lifecycle | Where | Vector-indexed | Leaves the tier by |
|---|---|---|---|---|
| **Transient** | Material in flight: captured, not yet kept | Files in a ragnart project's `inbox/` | **No.** The agent reads the files directly. | Keep (becomes a resource in the Archive), drop, or expiry after 30 days (§7.5) |
| **Archive** | Persistent | The roots of filesystem sources (static crawl; today the NFS roots); the `resources/` and `collateral/` of ragnart projects (curated) | Yes | The file is removed (after the grace period, §10.2) |
| **Runtime** | Persistent session record; a specific case of Archive | Claude Code and OpenCode sessions | Yes | — |

There is **one persistent store** (the `documents` collection, an alias of `documents_new`)
and **one pipeline**. The Transient tier is an ephemeral cache, not a store: material there is
used while work is in progress and becomes persistent only by being kept.

### 7.2 Two ingestion policies

| Policy | What enters | Trigger |
|---|---|---|
| **Static** | Every item of every source: every file under a filesystem source's roots (today the NFS roots) that `ragnart.yaml` includes and no rule of §1.2 excludes, plus every Claude Code and OpenCode session | Each ingestion host's own schedule; on Server 2, `ragnart-ingest.timer`, every 6 hours |
| **Curated** | The content of a ragnart project's `resources/` and `collateral/`, wherever the project folder is (§4.5) | A commit: `ragnart add`, `ragnart note`, `ragnart commit` or `ragnart drop`. Never the scheduled run (§7.3) |

Archive curation is blunt: a file that exists is indexed; a file that is removed has its
chunks removed. Nothing else decides.

The two policies do not overlap: they are separated by the project folder. The static policy's
crawl does not enter a project folder (§1.2, deviation D), and the curated policy indexes only the
`resources/` and `collateral/` of one ragnart project, by a commit; never its `inbox/`. The files of a project folder outside its
managed folders follow neither and are not indexed. "Crawl" names the static policy's walk of a
source and nothing else: a ragnart project has no ingestion mode.

### 7.3 ragnart project workflow

**Status.** Not yet built; v0.4.

Work in a ragnart project is RAGNART's reason to exist. It follows the dynamics of working with git:
`ragnart init <name>` once, then ordinary sessions in which the operator's requests are understood as
actions on the ragnart project.

**A ragnart project is a folder on disk.** `ragnart init <name>` creates it (§4.5), on a source that
RAGNART can write to, normally from inside a session (below). A session started in the project
folder, or below it, works in that ragnart project and is added to it when it starts, as a `.git`
directory makes a repository of the working directory (§4.5, §7.4). There is no mode to enter or
leave. Removing the project folder removes the ragnart
project's identity, inbox, resources, collateral and references. Their chunks leave the index under
the deletion rules of §10.2, and the ragnart project's record is removed after its grace period
(§4.5). The sessions it referenced stay, as `global` items.

```
ACME-Q1/
  .ragnart/        state and metadata: project.toml, provenance/, dropped/ (§4.5)
  inbox/           Transient tier: captured material, not indexed (§7.5)
  resources/       copies (added files, kept captures, notes) and references (*.reference); indexed
  collateral/      documents produced from the resources; indexed as derived material
```

**Four activities, no workflow states.** They interleave in any session and are not entered or
left. The only per-session settings are scope and grounding (§8).

| Activity | What it is | Scope and grounding |
|---|---|---|
| Gather | A discussion in which material is found and captured | A scope that includes `web`, set by the operator, or a page the operator asked for; any grounding (§8.1) |
| Work | Ordinary session work on the material | Any |
| Distill | A request that produces collateral | `project`, `strict` |
| Ask | A question answered from the ragnart project | `project`, `strict`; widened only by the operator |

**Creating a ragnart project from inside a session.** A ragnart project is created from inside a
Claude Code or OpenCode session, by the agent following the `/ragnart` skill. `ragnart init` is
plumbing and makes no assumption about where or how it is called; the skill performs the further
steps after it completes. For a session S, working in `ACME/`, asked to "start a notebook ACME-Q2":

| # | The skill | Command | What the operator sees |
|---|---|---|---|
| 0 | Checks the gate: S has no current project. Takes the request literally if it starts with an action verb; otherwise states its translation first | — | "Creating ragnart project ACME-Q2 in `ACME/`" |
| 1 | Runs `init` where the ragnart project is to live: S's working directory, or a place the operator named. A subshell keeps S's working directory where it is. Reads the folder and the `id` from the output | `(cd <place> && ragnart init ACME-Q2)` | The action line of `init`: folder, `name`, `id`, `grounding = strict`, `capture = on` |
| 2 | Takes ACME-Q2 as S's current project, recorded by the working reference of step 3a. S's scope becomes `project` and its grounding the ragnart project's default | — | The status line shows ACME-Q2 |
| 3 | Moves S's shell into the project folder, where it stays for the rest of the session | `cd <folder>` | S's later shell commands run in the project folder |
| 3a | Adds S to the current project, which is now ACME-Q2, as its working session | `ragnart add session:current` | `[ragnart · add · 1 session → resources]` |
| 4 | Reports the result | — | `[ragnart · scope project ACME-Q2 · grounding strict]` |

- **The gate.** A session works in at most one ragnart project for its whole life (§4.5). If S
  already has a current project, because it started in a project folder or already created one, the
  skill refuses: "this session is already part of a ragnart project; a new ragnart project cannot be
  started from an existing one". This is the session-level counterpart of "nesting has no meaning".
  `ragnart init` itself has no such check: it knows nothing about sessions.
- **Failures stop the sequence and are reported**, never retried silently and never undone. If
  `init` refuses (the folder exists, the source is not writable), steps 2 to 4 do not run and S has
  no current project. If the `add` of step 3a fails, the ragnart project exists, is S's current
  project, and S's shell is in its folder; repeating the `add` is safe, because an existing reference is reported as already present.
  If the skill cannot obtain S's id, it reports that S could not be added and stops after step 2.
- **Creation from a shell** is step 1 alone. No session is in the ragnart project until one is started
  in its folder, or opens it, or is added explicitly.

**Opening an existing ragnart project from inside a session.** A session started outside every
project folder enters an existing ragnart project when the operator says "open ACME-Q1"
(`/ragnart open ACME-Q1`). The skill runs the creation sequence without `init`:

| # | The skill | Command | What the operator sees |
|---|---|---|---|
| 0 | Checks the gate: the session has no current project | — | "Opening ragnart project ACME-Q1" |
| 1 | Finds the ragnart project by name, through the hub's records (§4.5), and its folder | — | — |
| 2 | Takes ACME-Q1 as the session's current project, recorded by the working reference of step 3a. Scope becomes `project`, grounding the ragnart project's default | — | The status line shows ACME-Q1 |
| 3 | Moves the session's shell into the project folder | `cd <folder>` | — |
| 3a | Adds the session to ACME-Q1 as its working session | `ragnart add session:current` | `[ragnart · add · 1 session → resources]` |
| 4 | Reports the result | — | `[ragnart · scope project ACME-Q1 · grounding strict]` |

- **The same gate as creation.** A session that already has a current project cannot open another:
  a session works in at most one ragnart project for its whole life.
- **Only a ragnart project on a source of the session's own host can be opened.** A ragnart project
  homed elsewhere has no folder here, and an action on it from another host is refused (§10.6,
  R14); it can still be searched by name.
- **Relocating the session** (§13.3) applies to `open` as it does to creation.

**Every `ragnart` command acts on the current project only.** The skill runs all `ragnart` commands
only in the current project, the one created by `ragnart init` or found by the session-start hook.
Acting on another ragnart project is not permitted: `ragnart` actions are strictly scoped to the
current project. The CLI finds its project as git finds a repository, from the nearest `.ragnart/`
at or above the working directory, and has no option that names another project. After creating a
ragnart project, the session's shell moves into the project folder with a persistent `cd` and stays
there: the session now exists for the project, so its commands belong there. Only `init` runs in a
subshell, in the place where the folder is created. Reading another ragnart project is a search at a
selection scope (§8.1), not an action on it.

- **What the `cd` moves, in Claude Code** (tested with Claude Code 2.1.327). It moves the shell:
  later commands, relative paths, and `ragnart` finding its project. Claude Code follows it: the
  working directory it reports for the session, and records in every later transcript entry, becomes
  the project folder. It persists only when the project folder is inside the session's working
  directories; a folder created elsewhere is added with `/add-dir` first, otherwise the shell returns
  to the start folder. It does not move the session's **filing folder**: the Claude Code project folder
  under which the transcript is kept, fixed when the session was created. The filing folder decides
  which `/resume` list shows the session, and a resume never changes it. On a resume the hook
  therefore finds the current project by the session's working reference (§4.5), not by location.
- **Relocating the session.** A session is filed in the project folder only if it is created there.
  The skill therefore spawns a session in the project folder, a fork of the creating session, and
  tells the operator: "For further dialogue in ACME-Q2: `/exit`, then
  `cd <folder> && claude --resume <id>`." From then on the operator works in that session, and it is
  the project's working session. The `cd` in the instruction matters: `claude --resume <id>` finds a
  session from any folder, but the session then runs in the folder it was started from. The in-app
  `/resume <id>` cannot switch to it: it lists only the sessions filed under the current session's
  filing folder (tested with Claude Code 2.1.327).
- **The switch is the operator's, by this one instruction.** It is the most robust way, and the only
  one that does not depend on how the session was started. A launcher wrapper that switches on
  `/exit`, and opening the fork in a new tmux window, are not used: each works only in some clients.
  The skill words the instruction for the client it runs in, which it reads from the harness (Claude
  Code: `CLAUDE_CODE_ENTRYPOINT`, `cli` in a terminal): in a terminal, the command above; in a desktop
  app, the app's equivalent, opening the project folder and selecting the session. The desktop apps
  are not verified (§13.3).
- **The fork is a native session of the project folder** (verified with Claude Code 2.1.327). A fork
  made from another folder is filed under the project folder, carries all of the creating session's
  turns and tool calls, and records its own id and the project folder in every copied turn. Claude
  Code gives it the project folder's own per-folder state. When the fork starts, the session-start
  hook finds the project by location and adds the fork as a working session, so the ragnart project
  references both: the creating session, which worked in it until the switch, and the fork, which
  works in it from then on. Their shared turns are indexed once, with the creating session (§4.2,
  "One turn, one place"). Making the switch smoother is open (§13.3).

**Actions.** The operator speaks plain language. The agent, following the `/ragnart` skill (§7.4),
translates a request into a `ragnart` command. The `ragnart` CLI is plumbing: the operator does
not need to type it. A command acts on the ingestion host of the ragnart project's source, which holds
that ragnart project's files and its state database (§2.3).

| The operator says | Action | Command | Decided by |
|---|---|---|---|
| Nothing: the agent fetches a page while capture is on | The page is captured into the inbox | `ragnart capture <url>`, run by the capture hook | The capture hook |
| "add" a file, a folder, a URL, a session or another ragnart project | A file or folder is copied, a URL captured, into `resources/` without passing through the inbox; a session or a ragnart project is referenced by a `.reference` file in `resources/`. Then committed | `ragnart add` | The operator |
| "keep" an inbox item | It moves to `resources/` and is committed | `ragnart add` | The operator |
| "drop" a resource or an inbox item | It is set aside, deleted after the grace period, and committed (§7.5) | `ragnart drop` | The operator |
| "note" a remark | The remark is saved as a Markdown file in `resources/` and committed | `ragnart note` | The operator |
| A request for a summary, a commented index, a presentation, a chart | Collateral is written to `collateral/` and committed | `ragnart commit` | The operator |
| A question | A search at the current scope (§8) | The search API | The agent; read-only |

- **The command prints the action line; the model does not compose it.** For example:
  `[ragnart · keep · 2 items → resources]`.
- **A request that starts with an action verb is taken literally.** Any other wording is
  interpreted, and the agent states its translation before acting, as it does for scope (§8.4).
- **Every change to a ragnart project's content is a deliberate act**: a command, or a file the
  operator places into or removes from a managed folder by hand. Capture writes only to the inbox,
  which is not indexed. The session-start hook writes only a session reference (§4.5).
- **Capture is done by the harness, not by the model.** A capture hook runs after each call of the
  harness's fetch tool (below). A page reached any other way is captured only if the agent follows
  the skill's instruction, best-effort. A page the agent fetched and did not capture is still named
  in the session record.

**Capture.** `ragnart capture <url>` fetches the page itself, so the inbox holds the source and not
the agent's rendering of it. The page is therefore fetched twice, once by the agent and once by
`capture`; that is accepted.

- **The capture hook.** It runs after each call of the harness's fetch tool: in Claude Code a
  `PostToolUse` hook on `WebFetch`, in OpenCode a plugin on `tool` / `execute.after` for its fetch
  tool. Both receive the session's id and the URL. When the session has a current project (found by
  its working reference, §4.5) whose `capture` is on, the hook runs `ragnart capture <url>` for that
  project, detached, so a slow fetch never holds up the conversation. Otherwise it does nothing.
- **The skill routes every web fetch through the fetch tool.** Fetching a page is, for the model, a
  particular kind of tool use: the skill instructs it that a request such as "search the web" or
  "read this page" is done with the harness's search and fetch tools, never with `curl`, a shell
  command or another tool, so that the capture hook sees each page. A web search finds pages and
  the fetch tool reads them: the search-result list itself is not captured, and each page the agent
  then reads is. A page that has to be reached some other way is captured by the skill's instruction
  to run `ragnart capture <url>`, best-effort.

- Captured: pages, and PDF files behind a URL. Search-result lists are not captured.
- A page is stored as Markdown with front matter: title, URL and fetch time. Every capture, a PDF
  included, also gets an entry in `.ragnart/provenance/` with its URL and fetch time (§4.5).
- A second capture of the same URL replaces the item and restarts its expiry.
- A failed fetch is reported; it is never silent.

**Operator add.** Material the operator adds is a deliberate act and skips the inbox.

| Added | Result |
|---|---|
| A file, wherever it is | Copied into `resources/` |
| A folder | Copied into `resources/`, with its files |
| A URL | Captured into `resources/` |
| A file in the ragnart project's own `inbox/` or `collateral/` | Moved into `resources/`, not copied: a keep, or collateral made a resource. Its provenance entry follows it |
| A session, or another ragnart project | Not copied: referenced, by a `.reference` file in `resources/` (§4.5). A ragnart project may be given by its name or the path of its folder; only its `id` is stored. A folder that contains a ragnart project below it is refused, naming that project |

**An add always copies a file.** Whether the original is already an item somewhere else does not
matter: it was not a resource of this ragnart project, and the copy is.

- The copy is a snapshot. A later edit of the original does not reach it, and it stays when the
  original is moved or deleted.
- **The copy is the ragnart project's own file, and it may be edited.** An edit is re-indexed at the
  next commit. The original never sees it, and the provenance still names where the copy came
  from.
- The copy travels with the ragnart project and falls under its access rule (§6.1).
- Its origin and the time of the add are recorded in `.ragnart/provenance/` (§4.5).
- The cost is a duplicate file and a second ingestion. It is accepted for robustness and
  simplicity. Identical copies collapse into one hit in a search at scope `all` (§9.1).
- A file placed into `resources/` by hand is material of the ragnart project too, with no provenance
  entry (§4.5).

**`add` and `commit`.** A file in a managed folder is material of the ragnart project from the moment
it is there; a commit is what indexes it.

| Command | What it does |
|---|---|
| `ragnart add <path or url>…` | Copies or captures each item into `resources/`, records its provenance, then commits and waits |
| `ragnart add --no-commit <path or url>…` | Copies or captures and records, and nothing more. The items are indexed by the next commit |
| `ragnart commit` | Indexes every file of `resources/` and `collateral/` that is not indexed yet, removes from the index every file that has left them, and waits for the outcome of each file |

- **A commit reconciles `resources/` and `collateral/` with the index; it never indexes `inbox/`.** It covers everything present when it
  starts, however it got there: an add, a keep, collateral, or a file placed by hand. It is
  idempotent, and it records no history (§1.2, deviation F).
- **Changes made by hand in `resources/` and `collateral/` are picked up by the next commit**, never by the
  scheduled run. Until then they are uncommitted changes, counted on the status line (§7.4).

| Done by hand in `resources/` or `collateral/` | At the next commit |
|---|---|
| A file added | Indexed: the manual equivalent of `ragnart add --no-commit`, without a provenance entry |
| A file edited | Indexed again |
| A file deleted | Removed from the index at once (below) |
| A file renamed or moved within `resources/` and `collateral/` | A move when its content hash matches one-to-one (§4.2): the key changes and nothing is re-embedded. Otherwise a deletion and an addition |
| A `.reference` file created or deleted | The reference is added or removed |

- **A commit removes at once.** A file that has left `resources/` and `collateral/`, by a drop or by hand, leaves
  the index in that commit, with no tombstone: the commit is the operator's deliberate act, and
  undoing it means restoring the file and committing again. The guard is against a folder that cannot
  be read: a commit runs no deletion phase when the project folder or one of its managed folders
  cannot be read, or when the folder no longer carries the ragnart project's `id` (§10.6, R13). The
  24-hour tombstone of §10.2 belongs to the global crawl, where a vanished file may be a mount failure
  that nobody asked for.
- **There is no implicit commit.** The scheduled run never commits a ragnart project: a ragnart
  project's index changes only by `ragnart add`, `ragnart commit` and `ragnart drop`, and every file
  of a commit gets an outcome that is reported to whoever ran it. A file placed or deleted by hand is
  an uncommitted change until the next commit: it is not found, or still found, at scope `project`
  until then. Uncommitted changes are shown, not applied: the session-start hook's listing and the
  status line count them (§7.4), as `git status` does, and the skill offers a commit when a question
  at scope `project` meets uncommitted changes. It does not commit on its own.
- **What changes without a commit, by design.** A referenced session is a `global` item, ingested by
  its connector on the schedule, so its new turns reach the ragnart project's scope without a commit;
  so does a referenced ragnart project's own commit. The existence of the ragnart project is checked
  by the scheduled sync: a missing folder is guarded as §4.5 says.
- **Commits coalesce.** At most one commit runs per ragnart project, and at most one more waits behind
  it. An `add` waits for the first commit that started after its files were in place: if one is
  running, it joins the waiting one, and if one is already waiting, it joins that. A run of adds
  therefore costs at most two commits, and each caller still gets the outcome of its own files.
- **Every file gets an outcome**, never silence:

| Outcome | Meaning | What follows |
|---|---|---|
| Indexed | The file is in the index, with its chunk count | — |
| Refused | An admission rule rejected it (§5.2), with the reason | The file stays in the folder: location is membership. The refusal is recorded, and later commits do not retry it until the file changes |
| Failed | Parsing or embedding failed | The file stays and is marked as an error. The next commit retries it, under the retry rule of §5.2 |
| Pending | The wait ended before the file was processed | Not an error. Ingestion continues in the background |

- `add` and `commit` exit with a non-zero code when any of their files was refused or failed.
  `ragnart status`, scoped to the ragnart project, lists the files that are not indexed and why.
- **A commit that waits depends on the split of the hub and the ingestion side** (§2.3). On the
  ingestion side, a commit only walks `resources/` and `collateral/`, plans and submits, and holds the
  single-writer lock (§10.3) briefly. The waiting is on the hub's status of each submitted item, and
  the hub gives an interactive submission priority over the bulk of a scheduled run. Without that
  split, a commit would meet the lock of a scheduled run that parses for hours. The concurrency rules
  are in §10.6.

**Collateral.** Distill produces a document on demand from the ragnart project's resources: an
executive summary, a commented index of its material, a presentation, a chart. The agent writes it
at scope `project` and grounding `strict`.

- **The agent writes it with its own file tools**, into `collateral/` of the current project (with the
  session's shell in the project folder, `collateral/<name>.md`), and the skill then runs
  `ragnart commit`. There is no `distill` command (D9).
- **Collateral is Markdown.** A summary or a commented index is a Markdown file. A presentation or a
  chart is produced from a Markdown source and kept beside it, so every piece of collateral has a
  Markdown file that states its sources.
- **The model lists the sources; the commit records their state.** The front matter's `sources:` lists
  the resources it was made from, by their paths in `resources/` and by the labels of references.
  The first commit that indexes the collateral records the content hash of each listed source in
  `.ragnart/provenance/`; the model never computes or copies a hash. A listed source that does not
  exist is reported at that commit.
- **Working files stay out of `collateral/`.** Scratch and intermediate files the agent writes go
  outside the managed folders, elsewhere in the project folder, where they are external and not
  indexed, or in a temporary directory. Only finished collateral is written to `collateral/`.
- It is indexed with `source_type = collateral`, so a result shows it as derived material and not
  as a source. Its weight in results is a `type_weights` setting (§9.2).
- An inbox item that it lists is kept.
- **Stale collateral is reported, never repaired.** A commit and `ragnart status` report a piece of
  collateral when a resource its front matter lists has changed since the hashes were recorded, or
  is gone. RAGNART never regenerates or drops collateral on its own.
- **Collateral is the ragnart project's output, kept apart from what was gathered.** It stays in
  `collateral/`, and it becomes a resource only explicitly: `ragnart add collateral/<file>` moves it
  into `resources/`.
- **Why collateral is kept apart.** Both folders are the ragnart project's material, indexed by its
  commits, but they do not carry the same weight as evidence. A resource was gathered and chosen by
  the operator. Collateral is the ragnart project's own synthesis, written by the agent from those
  resources. Under `strict` grounding, an answer resting on collateral rests on a summary the agent
  wrote, which is circular unless it is marked: a hit from collateral is therefore shown as derived
  material (`source_type = collateral`), carries its own `type_weights` setting, and is the only
  material checked for staleness. Promoting collateral to a resource is the operator vouching for it
  as material. The mark is the folder and not a field in the file, because membership is by location
  (§4.5): a field in the file is lost when a file is placed or edited by hand, and a folder cannot be.

**The ask loop.** A question is asked at project scope. When the answer is "no good hits in this
scope" (§8.3), the operator widens the scope or opens the grounding. What is found there can be
added to the ragnart project: an Archive file as a copy, a web page as a capture, and a session or
another ragnart project as a reference. The same question is then answered at project scope.

Relations between resources are not recorded. Search relates them when a question is asked.

### 7.4 ragnart project context

**Status.** Not yet built; v0.4.

Working in a ragnart project gives the same experience as a Claude Code session in a git repository: the
harness knows where it is, and nothing has to be re-seeded.

| Layer | Content | Use |
|---|---|---|
| **Resources** | `resources/` | Searched (§8) |
| **Collateral** | `collateral/` | Searched; shown as derived material |
| **Referenced sessions** | The Claude Code and OpenCode sessions the ragnart project references: those started in its folder, referenced by the session-start hook, and those referenced explicitly (§4.5) | Searched; weight set by `type_weights` (§9.2) |
| **Referenced ragnart projects** | The ragnart projects referenced by `.reference` files in `resources/` (§4.5) | Searched at scope `project` (§8.1) |
| **Working set** | The Transient tier: `inbox/` | Read directly by the agent |

**The catalogue is a listing, not a file.** No index file is kept in the ragnart project: it would duplicate
the state database and the folders, and could disagree with them.

- `ragnart ls-files`, scoped to the ragnart project, lists its resources and collateral, each with
  its outcome (§7.3).
- `ragnart ls-files --others` lists its inbox, each item with its origin and expiry date.
- The resource listing reads the state database of the ragnart project's ingestion host (§10.3). From
  another host it is served by the item-listing endpoint (§12). The inbox listing reads the folder,
  and the references are read from the `.reference` files in `resources/`.

**How a session learns it is in a ragnart project.** Three parts, each mirroring how the harness knows git:

| Part | Carries | Counterpart for git |
|---|---|---|
| The `/ragnart` skill, installed once for all ragnart projects | The action vocabulary (§7.3), the librarian rules (§8) and the capture policy. It is the same for every ragnart project and is refined over time. | The harness's built-in git instructions |
| The session-start hook | Finds the session's current project: by the working reference that holds the session's id (§4.5). A session without one that starts at or below a project folder gets the nearest `.ragnart/` at or above its working directory as its current project, and is added to it (§4.5). The hook then injects the ragnart project name, scope, grounding and a capped listing: counts, uncommitted changes and the most recent items. A session with no current project, outside every project folder, gets nothing: the hook is silent and writes nothing. | "This is a git repository", and the status snapshot |
| The status line | Ragnart project, scope, grounding, inbox count and the count of uncommitted changes, at all times | The branch name |

In Claude Code the hook runs on new, resumed and compacted sessions, and after `/clear`; its input
says which. The OpenCode equivalent is open (§13.3).

**Resume and compaction.** On each, the hook restores the session's working context; nothing else
carries it over.

| | After a compaction (same process) | After a resume (new process) |
|---|---|---|
| Current project | Found again: by location, or by the session's working reference (§4.5) | The same |
| The session's shell | Unchanged: still in the project folder | Back in the directory the session started in. The hook names the project folder, and the skill moves the shell into it before its first `ragnart` command |
| Scope and grounding | Back to the ragnart project's defaults, shown on the status line (§4.5, §8.4) | The same |
| The librarian rules | Re-injected by the hook as a short block: strict grounding, the status line copied from the API's response, every web fetch through the fetch tool, `ragnart` commands only in the current project. A compaction can summarise the skill's instructions away, and losing `strict` silently is the failure the librarian principle exists to prevent | The same |
| Project context | The ragnart project's name, scope, grounding and folder, and a capped listing: counts, the most recent items, the inbox count and the uncommitted changes | The same |

The lookup of the working reference is a small `ragnart` command that takes a session id; its name
is decided with the rest of the CLI.

- **Context is the session's current project**, set by location (the nearest `.ragnart/` at or above
  the working directory when the session starts) or by creating a ragnart project (§7.3). Other
  ragnart projects are searched **explicitly by name**, at a selection scope (§8.1): "structure the
  docs the way AYA does" searches `name:AYA`. That reads AYA and acts on nothing in it (§7.3).
- **Context is alive.** Each session adds to it.
- **Location is resolved within a source.** A session's working directory is a path on its own host,
  so the hook finds only a ragnart project held by a source of that host (§4.7). A ragnart project on
  another source, such as a shared one on a hub source reached from a laptop, is named explicitly
  (§8.8), and a session there is referenced explicitly.
- **A code repository is not a ragnart project** (§4.5). Its files are `global` items, found at
  scope `all`. A ragnart project about a repository is a folder of its own, created by `init`, and the
  sessions held in the repository are added to it explicitly: a session started at the repository's
  root has no current project.

**Ragnart project resources are not the harness's memory files.** Claude Code keeps its own memory files per
working directory. RAGNART neither writes nor replaces them.

| | Claude Code memory files | Ragnart project resources |
|---|---|---|
| About | How to work in the ragnart project, and its state | What the material says |
| Written by | The agent, on its own judgement | The operator's adds and keeps |
| Reachable from | Claude Code, for that working directory | Any client; indexed and cited |

**A ragnart project has no ingestion mode.** Its content is curated: only its resources and
collateral are indexed as its own (§7.2). The other files of its folder are not its content, and
they are not indexed (§1.2, deviation D).

Captured material is noise until it is kept, and the session in which it was used is already
archived and resumable in the Runtime tier.

A folder that must be searchable by meaning before curation is added as a whole: `ragnart add
<folder>` copies it into `resources/` (§7.3). Kotaemon uploads are operator adds: they enter directly
as resources, and are committed.

### 7.5 Transient lifecycle

**Status.** Not yet built; v0.4.

The inbox is a cache of files. It is not vector-indexed, so capture, drop and expiry write nothing
to Qdrant. The only write to the index is a keep.

| Event | By the operator | Automatic |
|---|---|---|
| Enter | A file placed in `inbox/` | Capture (§7.3) |
| Persist | "keep" | A piece of collateral lists the item as a source |
| Leave | "drop" | Expiry, 30 days after capture (`inbox_expiry_days`) |

- **Expiry is announced.** The inbox listing (§7.4) shows each item's expiry date. A file placed in
  `inbox/` by hand expires 30 days after its modification time.
- **A drop is undoable.** A dropped item moves to `.ragnart/dropped/` and is deleted after the grace
  period of §10.2. Until then a keep restores it.
- **Dropping a resource** sets its file aside in the same way, and `drop` commits, so its chunks
  leave the index at once (§7.3). A keep within the grace period restores the file and indexes it
  again. A reference is a resource: dropping it sets its
  `.reference` file aside in the same way, and leaves the referenced session or ragnart project as it
  is.
- **`ragnart gc` deletes** expired and dropped items. It runs in the ingestion run. It changes no
  index: inbox items were never indexed, and a drop has already committed.
- **RAGNART deletes files only in a ragnart project's `inbox/` and `.ragnart/dropped/`**, never elsewhere
  in a source (§1.2, deviation B).
- **The inbox stays small.** Reading files directly works for a bounded working set, and expiry
  keeps it bounded. A larger body of material is kept, which indexes it.

## 8. Retrieval: scope and grounding

**Status.** The API part is built in v0.2: `GET /v1/search` requires `scope` (§8.8) and echoes the
resolved scope, and `GET /v1/projects` lists the ragnart projects (§12). Not built: the `/ragnart`
skill and commands, and the client-side values `project` and `none` (v0.4). The `/rag` command
passes `scope=all` (§8.9).

### 8.1 Two different dimensions

Every answer is governed by two settings that answer different questions. **Scope** is where
evidence may come from. **Grounding** is whether an answer may go beyond that evidence. Either can be
changed without the other, but they are not fully orthogonal: what each means depends on the other,
and not every combination makes sense ("Where the two meet", below).

**Scope — where evidence may come from**

| Value | Searches |
|---|---|
| `project` | The current ragnart project: its resources and collateral, the sessions it references, and the ragnart projects it references (§4.5). A ragnart project nested in its folder is not part of it, nor are the other files of its folder: they are not indexed (§1.2, deviation D). Inbox files are not searched: the agent reads them directly (§7.4) |
| `<selection>` | ragnart projects named explicitly: names (`ACME-Q1`), paths (`~/Gin-AI/projects/AYA`), subtrees (`~/Customers/`), tag filters (`kind=customer`, `segment=ISV`), in any combination |
| `all` | Everything the principal may read: the items outside every ragnart project (`project_id` `global`, filtered by owner and file type as today, §4.6), and all accessible ragnart projects |
| `web` | The web, through the harness's search and fetch tools. Resolved by the client and never sent to the API (§8.8). Every page read is captured into the current ragnart project's inbox by the capture hook (§7.3), so it can be cited and checked later. Combines with any value above: `project` with `web` searches the ragnart project, then the web; `web` alone searches only the web |
| `none` | No retrieval |

**Name terms in a `<selection>`.** A name term names a ragnart project by its unique `name` (§4.5),
resolved through the `projects` records to its `project_id`. It is the stable way to name a ragnart
project, because a path changes when its folder moves. The operator may write a bare name: the client
completes it to a name term and shows the completed term.

**Path terms in a `<selection>`.** A path term names a ragnart project by its source and its path
(§4.5). It is resolved through the `projects` records to that project's `project_id`. The operator
may write a bare path: the client completes it with its default source (§8.2) and shows the
completed term. Path selection over items outside ragnart projects
is not part of the baseline data model and is not scheduled (§4.6): a path would need a per-item
list of ancestor directories, an item can be reached through several paths (§1.2, deviation C), and
a path goes stale when its item moves.

**Grounding — whether an answer may go beyond the evidence**

| Value | Meaning |
|---|---|
| `strict` | Only the evidence found at the current scope: retrieved passages, pages read when the scope includes `web`, and the inbox files of the current ragnart project, cited as inbox. Every statement cites its source. When the evidence is not there, the answer says so and stops. |
| `open` | The evidence at the scope first, cited; beyond it the model's own knowledge, marked as uncited. A statement is one of three kinds: cited from RAGNART; cited from a page read on the web, which is captured (§7.3); or uncited, from the model. |

**Where the two meet.** Scope and grounding are different concepts, and each answers its own
question, but they are not independent.

- **The web is a scope, not a grounding.** The web supplies evidence: a page read is cited and
  captured like any other source. Placed under grounding, as the outermost reach of `open`, it would
  bundle two different widenings, more evidence and no evidence, into one switch. Then "answer only
  from sources, with the web as a source" could not be expressed at all: reaching the web would also
  let the model answer from its own memory. Keeping the web under scope makes `strict` with `web` the
  librarian's way of gathering sources from the web.
- **Not every combination makes sense.** Scope `none` with grounding `strict` asks for answers that
  rest only on evidence while searching for none: every question can only end in "no evidence". The
  combination is accepted, because actions still work under it (below), but it is nonsensical for
  asking, and the client says so when it is set.
- **A widening of either is the operator's act** (§8.4). Adding `web` to the scope is a widening like
  any other, and the agent never makes it on its own.

**What `strict` guarantees, and who guarantees it.**

| Part | Guaranteed by |
|---|---|
| Which evidence exists at the scope, how relevant each passage is, and whether it is enough to answer from (§8.7, §9.2) | The search API |
| The status line and the "no good hits" stop, copied from the API's response and not judged by the model (§8.3) | The `/ragnart` skill: a fixed request template, and the rule to copy the echoed scope and stop when the API reports no good hit |
| Using only that evidence, and citing every statement | The model, following the `/ragnart` skill. Best-effort, as capture is (§7.3) |

The first part is RAGNART's guarantee. The second is a fixed procedure of the skill: the model fills
in the query and copies the response, and never judges relevance itself. The third is an
instruction. `strict` is meant to
govern every informational answer in the session, not only those for which the agent chose to
search, and under it a statement of fact without a citation is a fault. No component of RAGNART sees
an answer that used no retrieval, so RAGNART cannot detect that fault: in free conversation, staying
within the evidence is best-effort.

**Actions are not answers.** Editing a file, running a command and fetching a page the operator
asked for are actions, and `strict` allows them. A page the operator asks for by name is a one-off
`web` scope for that page: it is captured into the inbox (§7.3), and an answer may then cite it as
inbox.

### 8.2 Defaults

Smallest first: the librarian is always on duty.

| Where the session starts | Scope | Grounding |
|---|---|---|
| Inside a ragnart project | `project` | `strict` |
| Outside any ragnart project (its items are `global`, §4.6) | `all` | `strict` |

The defaults are tunable at three levels, each overriding the one before it, as the knobs of §9.2
are: `ragnart.yaml`; the ragnart project's `project.toml` (`scope`, `grounding`), where `init` writes
`grounding = "strict"` (§4.5); and the session or the single request (§8.4).

**Default source.** A path term sent to the API always names its source (§8.8). The client holds a
default source, with which it completes a bare path: the source of its own host that holds ragnart
projects. The default is explicit, visible and changeable. It is a setting of the client;
`/ragnart status` shows it; every completed term is shown with its source (§8.3); and the operator
changes it with `/ragnart source` (§8.4). Not built: it arrives with the `/ragnart` skill (v0.4).
The `/rag` command sends `scope=all`, which has no path term.

### 8.3 Visibility

Every answer that used retrieval starts with a status line, copied from the API's response: the
scope it shows is the scope the API echoes as searched, not the one the agent meant to search:

```
[ragnart · scope project ACME-Q1 · grounding strict]
```

A ragnart project is shown by its name (§4.5). A path term the operator used is shown with its
source, as `<source>:<path>` (§8.8), so the default source the client
applied is always visible.

When the API reports no good hit (§8.7), the answer ends there:

```
[ragnart · scope project ACME-Q1 · grounding strict]
No good hits in this scope. You may widen it (/ragnart scope …, including web) or open the
grounding (/ragnart grounding open).
```

The response does not suggest where else to look.

Between answers, the harness status line shows the project, scope, grounding and inbox count at all
times (§7.4).

### 8.4 Changing scope and grounding

Changes are made **only by the operator**, and always shown.

| How | Effect |
|---|---|
| `/ragnart scope project \| <selection> \| all \| web \| none`, with `web` combinable, e.g. `/ragnart scope project web` | Sets the scope for the rest of the conversation, until a compaction or a resume, after which the ragnart project's default applies again (§4.5) |
| `/ragnart grounding strict \| open` | Sets the grounding in the same way |
| `/ragnart ask --scope <…> [--grounding <…>] "<question>"` | One answer at that scope; the session settings are unchanged |
| `/ragnart source <source id>` | Sets the default source that completes a bare path term (§8.2), in the same way |
| `/ragnart status` | Shows the current scope, grounding and default source |
| Natural language that names a scope | The agent may interpret it, and states its translation before the results, e.g. *"Any customer in the ISV segment" → searching `kind=customer, segment=ISV` across all accessible projects* |

The agent never widens or narrows the scope, or opens the grounding, on its own initiative.
Narrowing works the same way as widening (`/ragnart scope project` returns to the current
project).

Capturing a page into the inbox (§7.3) changes neither the scope nor the grounding.

### 8.5 Examples

| Situation | Status line and behaviour |
|---|---|
| In the ragnart project RAGNART-notes: "what is our snapshot retention?" | At `scope project RAGNART-notes · grounding strict` the answer is "no good hits", unless that document was added to the ragnart project. At `scope all`, set by the operator, the answer quotes `docs/RAG-strategy.md`, a `global` item, with citations |
| In RAGNART-notes: "structure the docs the way AYA does" | Agent states "scope: `name:AYA`"; searches AYA |
| Working on customer A: "which other customers use vendor ABC?" | Agent states "scope: `~/Customers/` except `~/Customers/A`"; results grouped by customer, each with its source |
| "PDF files about Qdrant modified in the last month" | Filters `file_type=pdf`, `modified_at ≥ <date>` at the current scope |
| "OpenCode sessions last week about snapshots" | Filters `source_type=opencode_session`, `modified_at ≥ <date>` |
| Outside any project: "where did I write about Qdrant aliases?" | `scope all · grounding strict` |

### 8.6 Filtering

- **Structured filters** on any search: `source_type`, `file_type`, time ranges on
  `modified_at` and `ingested_at`, project tags (resolved to project ids). Turning a natural
  language request into filters is the client's job; the API accepts only structured filters.
- **Metadata-only queries** ("which PDFs changed last month") have no text to match and are
  answered from the state database through a listing endpoint.
- **Semantic plus metadata** queries run in Qdrant with payload filters.

### 8.7 Result shape

**Status.** Built: results are **collapsed** by `content_hash` (§9.1). The hit that represents a group
carries `locations`, the source path of every identical copy. A hit carries its `source_id`, `source_path`, file
name, `file_type`, `session_title`, passage (`content`, the `parent_text`), `score`, `content_hash`
and `locations`. Not built: the project, `source_type` and `modified_at` fields in a hit, and grouping
by project.

A hit names its origin by `source_id`, key (`source_path`) and display label. It makes no promise
that the caller can open the item: a key is meaningful within its own source only, and the caller may
be on another host (§4.2).

**The relevance verdict.** The response says how good the evidence is, so that the "no good hits"
stop (§8.3) is a fact the API reports and not a judgement of the model.

**Status.** Not built. It depends on neither v0.3 nor v0.4.

| Field | On | Meaning |
|---|---|---|
| `id` | Each hit | A short handle, unique within the response, that an answer cites |
| `rerank_score` | Each hit | The reranker's score for the hit: the relevance signal (§9.2). Absent when the reranker did not run |
| `good_hits` | The response | The number of returned hits whose rerank score is at or above `min_relevance` (§9.2) |
| `reranked` | The response | False when reranking was off or the reranker failed. The hits are then in Qdrant order, and none counts as a good hit |

A hit names its item and returns the passage. It does not say where in the item the passage is:
`page` is always `0` (§5.6).

### 8.8 The API

- `scope` is **required** on every search; there is no server-side default. A request without it
  is answered with HTTP 422.
- `scope` is repeatable, and the projects searched are the **union** of its terms. Each term is one of:
  - `all`: every ragnart project and the `global` items (owner rule). No project filter is sent.
  - `<source>:<path>`: the ragnart project at exactly this path on its source. A path that names no ragnart
    project is HTTP 422, with a message that names the path.
  - `<source>:<path>/`: the ragnart project at this path, if there is one, and every ragnart project whose
    folder is below it. It selects by where the folders are now, and implies no relation between
    those ragnart projects (§4.5). A path that names no project at or below it is HTTP 422.
  - `name:<name>`: the ragnart project with that name, compared without regard to case. A name that
    no ragnart project carries is HTTP 422. Not built (v0.4).
  - `tag:<key>=<value>`: every ragnart project carrying that tag. A tag that no project carries, or
    a term without a non-empty key and value, is HTTP 422.
  - An empty term is HTTP 422.
- **A path term names its source**: `<source>:<path>` and `<source>:<path>/`, split at the
  first `:`. The API has no default source, as it has no default scope: a path term without a source
  is HTTP 422. `tag` and `name` are reserved and are never source ids, so `tag:<key>=<value>` and
  `name:<name>` stay unambiguous. `all`, name terms and tag terms carry no source. Built: the
  source-qualified forms. Only a source whose project support is built accepts a path term, which is `nas`
  today (endpoint project support is v0.4); a term on any other source is HTTP 422, and so is a bare path.
- **A ragnart project named by a term includes the sessions it references.** The API resolves each
  ragnart project to its `project_id` and to the `session_id`s of its session references, before the
  search: the filter matches a chunk whose `project_id` is among the ragnart projects, or whose
  `session_id` is among their referenced sessions (§4.5). Nothing about a reference is stored in
  Qdrant. Not built (v0.4).
- A selection (any term except `all`) never includes the `global` items, except the sessions that its
  ragnart projects reference. Only `all` includes them all.
- `project` and `web` are resolved by the client, and `none` causes no retrieval call at all. None of
  them is sent to the API: `web` means the harness's search and fetch tools. The client sends
  `project` as the `name:` term of the current ragnart project, and one term for each ragnart project
  it references. It reads the references from `GET /v1/projects`
  (§12; not built, v0.4). A `none` term that is sent anyway is a path term and is answered with HTTP
  422.
- The response **echoes the resolved scope** in `scope`: `all` (whether `all` was a term), `global`
  (true only for `all`), and `projects`, the ragnart projects actually searched, each as `id`, `path`
  and `name`, and with its `source_id`. It also echoes `max_hits_per_source` (§9.2).
- The API returns evidence, and its verdict on that evidence (§8.7). It writes no answer. Strict
  grounding of the answer is applied by the client (§8.1): the `/ragnart` skill and commands for
  Claude Code and OpenCode, the prompt configuration in Kotaemon. The skill is where the librarian
  behaviour is defined.
- **How the agent searches.** The `/ragnart` skill calls `GET /v1/search` with a fixed request
  template, as the `/rag` command does today. Two properties make it trustworthy, whatever the
  mechanism. The request is built from the session's settings: the scope term is the current
  project's `name:` term or the scope the operator set in this conversation, never one the model chooses; a one-off scope is used
  only when the operator asked for it. The verdict comes from the API: the model copies the echoed
  scope into the status line and stops when `good_hits` is 0, and does not judge relevance from a
  score. A `ragnart search` CLI verb that renders the response in code is added only if the
  template proves unreliable in practice. The `/rag` command's present rule, which judges relevance
  from `score` below 0.45, does not survive: in hybrid search `score` is a rank-fusion value
  (§5.5).

### 8.9 The `/rag` command

`/rag` remains as an alias of `/ragnart`: `/rag on` → grounding `open`; `/rag only` →
grounding `strict`; `/rag off` → scope `none`; `/rag search <query>` → a one-off `ask`. New
features exist only in `/ragnart`. Outside ragnart projects, `/rag` is today's single-user RAG search,
unchanged (§4.6). Every search request the `/rag` command sends carries `scope=all`.

## 9. Retrieval quality

**Status.** Built: `rerank`, the fetch sizes, `max_hits_per_source` and collapse by `content_hash`,
all in `ragnart.yaml` (§9.2) except collapse, which has no knob. Not built: the other knobs, the
labelled set and `ragnart calibrate` (§9.3, v0.4).

### 9.1 Predictable, not rigidly deterministic

The corpus changes and polluting content appears; rankings must be able to change with it.
RAGNART is therefore **predictable** rather than fixed:

- **Stable for identical input.** Equal scores are ordered by key and chunk index, and the
  reranker receives its candidates in a fixed order, so the same query on the same data returns
  the same list.
- **Judged by tolerant criteria.** Quality checks ask whether expected sources appear in the
  top results and whether the first correct source stays near its rank, never whether lists
  are identical.
- **Bounded influence of one source.** A source contributes at most `max_hits_per_source`
  passages to a result; identical copies collapse; source types can be weighted or excluded.
  The shaping order is fixed: rerank, then collapse of identical copies by `content_hash`, then the
  `max_hits_per_source` cap, then the `top_k` cut. Collapse never merges the chunks of one file (they
  share its hash), and never merges empty-file or missing hashes.
- **Pollution is visible.** A corpus health report lists sources that appear in the results of
  many unrelated queries, items with abnormal chunk counts, and low-quality text.

These properties hold for search. They do not hold for inbox files, which the agent reads directly
and which are not ranked (§7.5).

### 9.2 Knobs

| Knob | Controls |
|---|---|
| `min_relevance` | Rerank score below which a hit is not a "good hit". The search API applies it to the raw rerank score and reports the count of good hits (§8.7). It decides the "no good hits" stop (§8.3), which is what leads the operator to widen the scope (§7.3) |
| `max_hits_per_source` | Per-source cap in one result list (default 3, `ragnart.yaml` `rag.search.max_hits_per_source`) |
| `type_weights` | Multipliers or exclusions by `file_type` / `source_type`, including `collateral` and the session types. A weight changes the order of hits only: it never decides whether a hit is good |
| `dense_fetch`, `sparse_fetch`, `fetch_cap` | Candidates brought to the reranker |
| `rerank` | Reranking on or off |

They are set at three levels, each overriding the previous: `ragnart.yaml` (global),
`project.toml` (per project), the request (echoed in the response). The exception is
`min_relevance`, which has one global value in `ragnart.yaml` until measurements show that ragnart
projects need different ones.

**The relevance signal is the rerank score.** In hybrid search the `score` field is an RRF
rank-fusion value and does not measure relevance.

### 9.3 Calibration

- A **labelled set** of two kinds of query: queries with the sources they should return, and
  questions whose answer is not in the material, which should end in "no good hits". It is a plain
  file, started from real use before any tooling exists, and grown by
  `/ragnart expect "<query>" <path>` and by agent-proposed entries the operator confirms.
- `ragnart calibrate` runs the set and finds `min_relevance`: the value that best separates the
  queries that should be answered from those that should stop. It also reports the hit rate in the
  top 5 and the rank of the first correct source. It does not sweep the other knobs.
- The operator applies settings explicitly; there is no automatic tuning.
- The same set is the gate for a new pipeline version (`p2`) and for a change to retrieval (§5.5):
  neither may lower the top-5 hit rate beyond an agreed tolerance.

## 10. Failure and consistency

### 10.1 Authority

| Store | Role | Recovery |
|---|---|---|
| The content of each source; today the files and session stores on the NAS | The knowledge itself | The source's own protection; for the NAS, Synology snapshots (hourly) and offsite copy |
| A state database, one per ingestion host; today `rag_state.db` | Bookkeeping | Restore copy taken with the snapshot (§10.4), or a NAS snapshot plus reconciliation. A state database on another host is rebuilt from the hub's listing of its sources' items (§2.3) |
| Hub registry (§4.8) | Sources and published ragnart project records | Restore copy taken with each snapshot (§10.4); the ragnart project records are published again by their sources |
| Qdrant | Derived index | Daily snapshot triple; full re-embedding as last resort, which needs every source to submit again |

NAS backups are for emergencies; normal operation never relies on them.

### 10.2 Deletion rules

**Status.** All five rules are built. Rule 4 is built in its minimal form, the sync log line. The NAS sync plans on its own state database and asks the hub: a purge is a `delete` request, and the row is removed once the hub has confirmed it (rule 5 holds through the request status). The 413 refusal of an oversized upload deletes the item's older points and marks the row `ignored`; a 422 deletes nothing and leaves the row pending with an error logged, for the Mac runner as for the NAS.

Deletion is the dangerous operation. The sync applies these rules. They run on the ingestion side
(§2.3): each rule and each guard counts the rows of one ingestion host only. The hub deletes an item
only when its source asks.

1. Nothing is deleted because it was **not seen**, unless the scan that missed it is complete. A
   root is incomplete when it is not a directory, or when it yields zero files while the queue still
   holds rows under it, because an unmounted mount point looks the same. If any root is incomplete,
   the sync plans no moves, no tombstones and no ragnart project removals. It logs an error and exits
   with code 1, and the queueing done by the scan stands. Purges of rows whose grace period has ended
   still run (rule 2), because they do not depend on this run having seen every root. The check covers
   only the owners the sync scans: with `--user`, only that owner's rows. An OpenCode session row is
   kept while the OpenCode API cannot be read. An unreadable `.ragignore` stops the sync before this
   point (§1.2, deviation G).
2. A vanished item is **tombstoned**, not deleted. Its `missing_since` is set to the sync's time, and
   only the first time it is seen missing, so the grace period runs from that first sighting. Its
   points stay searchable. The first sync run more than 24 hours after `missing_since` purges it: its
   points are deleted (Qdrant, `wait=true`), and then its row. A tombstoned item found again before its
   purge has its `missing_since` cleared. A queue row that is not `completed` has no points, so when its
   source is gone it is removed at once, without a tombstone. The grace period is 24 hours
   (`DEFAULT_GRACE_HOURS` in `ragnart/sync_plan_core.py`).
3. Tombstoning is limited to `rag.sync.max_delete_fraction` (0.10 in `ragnart.yaml`) of the **completed**
   rows. When more completed rows would be newly tombstoned than that fraction of all completed rows,
   no tombstone is set, the sync logs an error and exits with code 1. `--allow-mass-delete` lets the
   tombstoning proceed for that run. This limit gates only new tombstones. Moves and reappearances
   still apply, and purges of rows already tombstoned are not counted against it, because those rows
   were vetted when they were tombstoned.
4. Every purge is recorded in the sync log with one line: the key, the chunk count, the reason and
   the `missing_since` time, for example `purged <key> chunks=<n> reason=tombstone grace period expired
   missing_since=<time>`. The sync log also names each tombstoned path and each stale row removed, with
   its reason. No purge table exists: the sync log is the purge record.
5. Qdrant deletes wait for Qdrant to confirm them (`wait=true`), and a queue row is removed only after
   its points are gone. If a delete fails, the sync stops, the row survives, and the next sync retries.

**Ragnart projects.** A ragnart project whose marker is missing or unreadable is protected in the
same way: it is tombstoned for the same grace period, or it keeps its last good record (§4.5). Not
built (v0.4): today the sync removes it at once. The content of a ragnart project is not under these
rules: a commit removes what has left its `resources/` and `collateral/` at once, behind its own guard against an
unreadable folder (§7.3).

**A silent source.** The hub does not notice that a source has stopped submitting (§2.3). Its items
stay in the index and stay searchable for as long as it is silent. Nothing expires them: a tombstone
is set only by the source itself, from a complete enumeration.

**A source that no longer exists.** A lost or reinstalled machine cannot delete its own items.
Removing every item of one `source_id` is a break-glass operation by the operator, with a
maintenance tool on the hub, as the purge tools in `utils/` are today. There is no API request for it
and no automatic policy. Built (v0.3, session B1): `utils/rag_source_remove.py --source <id>` deletes the points, intake rows and spool files of one endpoint source. It is a dry run by default, takes the ingestion lock on `--apply`, keeps the source's registry row and secret hash, and refuses a source that is the home of at least one project in the hub registry unless `--allow-projects-source` is given.

### 10.3 Operating rules

**Status.** Built: the single-writer lock covering every writer of the index, idempotent ingestion,
plan-then-apply sync, and scheduled reconciliation. Built (v0.3, session B2): reconciliation is on the ingestion side, against the hub's listing of each source's items (`ragnart/sync_reconcile.py`), for the NAS sources and the Mac's alike; there are two locks, the index lock for the intake worker and hub maintenance, and the NAS-ingestion lock for the NAS run; the snapshot takes both. The purge tools `utils/rag_purge_corrupt.py` and `utils/rag_purge_rescan.py` are read-only reports. Notifications are not yet scheduled.

- **The hub is the only writer of the index.** No source writes to Qdrant: a source asks the hub
  (§2.3). Within the hub, and within each ingestion host, one writer runs at a time.
- **Two locks, two writers.** Every process that writes Qdrant points or the hub registry holds the
  index lock (`RAGNART--databases/rag-ingest.lock`, `rag.ingest_lock`, required in `ragnart.yaml`) while it
  writes: the intake worker (`ragnart/intake_worker.py`), `utils/rag_source_remove.py --apply`,
  `utils/rag_reset.py` without `--dry-run`, and `scripts/sqlite-qdrant-snapshot.sh`. The NAS ingestion run
  holds the NAS-ingestion lock (`RAGNART--databases/rag-nas-ingest.lock`, `rag.nas_ingest.lock`) while it
  writes `rag_state.db` and submits: `utils/rag_sync_nfs.py` without `--dry-run`, which takes no lock in a dry run.
  `scripts/ragnart-ingest-run.sh` holds the NAS lock for its whole run (sync, WAL checkpoints, snapshot) and
  not the index lock, so the intake worker can drain while the NAS run scans. The snapshot script takes both
  locks, the index lock first and then the NAS lock, so the restore triple is taken while neither writer
  runs. The one exception is `utils/rag_sparse_migrate.py`, a one-off migration that is run with the timers
  stopped. A lock is fail-fast: a second writer exits with status 75 at once and prints the holder (pid,
  host, command line, start time), which is also written into the lock file. It never waits. The planned
  v0.4 verbs `add`, `commit`, `drop`, `rm` and `gc` will take the lock of the state database they write, on the
  ingestion side (§7.3, §10.6). A state database is opened by its own ingestion host only: `rag_state.db`
  is opened from Server 2 only. `hub_registry.db` is also written by the ingestion API (the publish requests
  and the intake rows) without a lock, so the registry copy of a restore triple is weaker than the other
  two (§10.4).
- **Idempotency.** Every operation (ingest a file, sync, backfill, `commit`, `gc`) can be
  re-run safely after a crash.
- **Failed sync.** A sync that refuses deletion or crashes does not stop the ingestion run. The
  WAL checkpoints and the snapshot step still run. The run then exits with the sync's exit
  code, so `ragnart-ingest.service` is marked failed.

- **Plan-then-submit.** The NAS run is, in order: poll the outcome of the previous run's requests, scan,
  compute the sync plan, reconcile against the hub's listing, submit. The plan covers project upserts and
  removals, moves, tombstones, purges, reappearances, stale-row removals and attachment changes. The sync
  logs it, then submits it as API requests in a fixed order: projects, moves (`change_key`), purges
  (`delete`), attachment changes (`change_metadata`), uploads. A queue row is marked `processing` with the
  request id when the hub accepts the request, and `completed`, `failed` or `ignored` by the next run's poll
  of the request's status. A move, a purge or a project removal changes its local row only after the hub has
  accepted the request. `utils/rag_sync_nfs.py --dry-run` reads the live hub (listing, info, statuses), runs
  the scan on a private copy of the queue, prints the plan and the reconciliation counts, and exits 0; it
  sends no request, writes nothing and takes no lock.
- **Reconciliation** (`ragnart/sync_reconcile.py`). Every ingestion run reconciles each of its sources with the
  hub's listing of that source's items (`GET /v1/items`: key, content hash, chunk count, pipeline version,
  owner, `project_id`), the NAS sources and the Mac's alike. It applies four rules. An **orphan**, a hub item
  with no local row, is deleted on the hub; this is skipped when the local state holds no completed row for
  that source, and when the orphans exceed `rag.sync.max_delete_fraction` of that source's own completed rows. A
  completed row that is present on the source and **missing** from the hub is sent again, except a row with a
  `chunk_count` of 0, which has no points to list. A row whose **pipeline version** on the hub is older than the
  hub's current one is sent again, at most `rag.nas_ingest.resubmit_batch_size` (200) per NAS run. A row whose
  hub `owner` or `project_id` **differs** from the local value gets a `change_metadata` request. Reconciliation
  never touches Qdrant; a source whose listing cannot be read is skipped for that run, and in a dry run it only counts. A lost state database is rebuilt
  from the listing (§4.8). The run does not compare a source's point count with the sum of its completed rows'
  `chunk_count`.
- **Notifications** (failed run, guard tripped, snapshot failed, reconciliation problems) are
  part of the design; the channel and implementation are not defined (§13).

### 10.4 Snapshot policy

- One restore triple per day (the Qdrant snapshot, and the matching copies `rag_state-<ts>.db` and
  `hub_registry-<ts>.db`, §4.8), taken by the
  ingestion run when the newest triple is older than 20 hours (normally the 01:00 run);
  `RAG_FORCE_SNAPSHOT=1` forces one.
- Seven triples kept on the NAS (`RAGNART--databases/qdrant-snapshots/documents/`).
- Qdrant writes the snapshot to its local `snapshots_path` and the script downloads it to the
  NAS and deletes the local copy; stale local snapshots and partial downloads are removed
  before each run; the run aborts below 60 GB free on the NAS.
- Restore procedures: `docs/RAG-strategy.md` §9.3 (full) and §9.4 (state database only).

### 10.5 OpenCode database

**Status.** Decided; built in v0.2.

`opencode.db` stays on Server 2's local disk. A nightly online copy (`sqlite3 .backup`) is
written to `RAGNART--databases/opencode-backups/<hostname>-<ip>/opencode-<YYYY-MM-DD>.db`,
with the last 7 copies kept, by a timer installed with RAGNART's units.

### 10.6 Concurrency in ragnart projects

**Status.** Not built (v0.4). Every rule here is part of the design of `add`, `commit`, the inbox and
`.ragnart/` (§4.5, §7.3, §7.5).

A ragnart project is written by several actors at once: the operator by hand, the commands of §7.3,
the session-start hook of every session started in its folder, the scheduled run and the hub. Each
race below names what would go wrong and the rule that prevents it.

| # | Race | What would go wrong | Rule |
|---|---|---|---|
| R1 | A file is still being copied when a commit's walk reaches it, as when a file manager drops it into `resources/` | A truncated file is indexed | `add` copies to a temporary name in the same folder and puts it in place with the non-replacing link of R3, so the file appears whole and replaces nothing. A file whose size or modification time changed between its discovery and its hashing is skipped, and the next commit takes it |
| R2 | A file changes between the plan of a commit and its submission | The stored hash does not match the indexed bytes | The stored hash is the hash of the bytes submitted, never the one seen at discovery |
| R3 | Two adds put files with one name into `resources/` | One silently replaces the other | The file is put in place by a link that refuses to replace, which is atomic on NFS. The same content is reported as already present; different content is refused, naming the conflict. An add never overwrites |
| R4 | An add joins the waiting commit just as that commit starts its walk | The add waits for a commit that does not cover its file | Each ragnart project has a commit generation number. An add records it once its file is in place, and waits for a commit that started with a higher one |
| R5 | Two commits of one ragnart project start at once, from two sessions or a session and a shell | The same item is submitted twice | One writer of the state database at a time, on the ingestion side (§10.3). Commits of one ragnart project coalesce (§7.3). The hub treats an upsert whose content hash equals the stored one as a no-op |
| R6 | An item is deleted or dropped while its upsert is still in flight | The upsert lands after the delete and leaves points with no queue row | The hub applies the requests for one item in the order they were submitted: the later request wins. Reconciliation (§10.3) catches anything left |
| R7 | A newer version of a file is submitted while an older one is still in flight | The older version wins | Per-item order at the hub, as in R6: the latest submission wins |
| R8 | A keep runs while `gc` expires the same inbox item | The kept file is deleted | A keep puts the file in place with the non-replacing link of R3 and then removes it from `inbox/`. `gc` first renames an item into `.ragnart/dropped/`, and fails cleanly when it is already gone. Both take a short lock on the ragnart project |
| R9 | A restore from `.ragnart/dropped/` runs while `gc` purges the same item | The restored file is deleted | As in R8 |
| R10 | Two sessions start in one project folder at once, and both hooks add their session | A lost update: an append is not atomic across NFS clients | One file per entry, each created atomically: a `.reference` file in `resources/`, an entry in `.ragnart/provenance/`. No shared file is rewritten |
| R11 | The hook or a command writes while the operator edits `project.toml` | The operator's edits or comments are lost | RAGNART never writes `project.toml` after `init`: the operator owns it, and RAGNART owns the rest of `.ragnart/` |
| R12 | `project.toml` is read while it is being saved | A torn read looks malformed | The marker guard keeps the last good record (§4.5) |
| R13 | The project folder is moved or renamed during a commit | Every file looks deleted | At its end, a commit checks that the project folder still carries the same `id`. If not, it runs no deletion phase, as for deviation G (§1.2); the next sync finds the move (§4.5) |
| R14 | A commit is started from a host that is not the ingestion host of the ragnart project's source, as from a laptop that mounts the NAS | Two ingestion sides write one ragnart project | Refused: a ragnart project has one home (§4.5). How a project action from another host reaches the home is open (§13.3) |

## 11. Capacity and placement

| | Server 1 (.93) | Server 2 (.95) |
|---|---|---|
| RAM | 23 GB; Qdrant uses 17.2 GB | 27 GB; Ollama, llama-swap and the GPU models share it |
| Other duties | AYA Windows VM host (its passthrough boot entry reserves 8 GB of hugepages) | Ingestion, search, embedding, reranking |

Qdrant keeps vectors in RAM. A corpus that passes the admission rules of §5.2 is about 400,000 points instead of about
3 million, roughly 2–3 GB instead of 17 GB, which removes the memory pressure on Server 1. The
placement of Qdrant is decided after the cleanup, from measured memory use.

## 12. Interfaces

| Interface | Role |
|---|---|
| HTTP API (Server 2, port 8100) | `GET /v1/search`: hybrid search and reranking (§5.5); answers 503 when Qdrant is unreachable or fails. `GET /health`: `ok` when Qdrant's `/healthz` answers within 2 seconds, otherwise `degraded`. `GET /v1/projects` (built) returns the `global` row and every ragnart project, each with `id`, `path`, `name`, `owner`, `source_id`, `tags` and `item_count` (completed items); the listing has no `parent` or `mode`, as the design has no use for them (§4.5). With `user`, it lists the `global` row and the projects whose owner is that user or `la-familia`, the same owner rule as search. Planned: the relevance verdict in a search response (§8.7), each ragnart project's references to ragnart projects and sessions in the project listing (§4.5), resolution, item listing for metadata queries and for the project catalogue (§7.4), grant management |
| Ingestion API (the hub) | Built (v0.3, sessions B1 and B2) for every source, the three NAS-side sources included. The write path from a source to the hub (§2.3). Requests: upsert an item, as bytes with a file type or as text (§4.7); delete an item; change an item's key, change an item's metadata such as `owner`, and change an item's source when a ragnart project moves to another source (v0.4), all three as payload rewrites that carry no content and re-embed nothing (§2.3); list the source's own items with key, content hash, chunk count and pipeline version; read the status of a submitted item; publish the source's ragnart project records, with their references (§4.5); read the current pipeline version, the accepted file types and the admission limits (§5.2). As built (v0.3, session B1): the write requests (upsert, delete, change key, change metadata) answer 202 with a request id and never touch Qdrant, and each one logs one INFO line naming the route, the source, the caller's User-Agent (`ragnart-endpoint` or `ragnart-nas-ingest`, with the package version and the source id) and the request id; `GET /v1/status/{request_id}`, `GET /v1/items` and `GET /v1/info` are built. Built (v0.3, session B2): the text route takes the `opencode` connector besides `claude_code`; an upsert carries `tag` and `project_id` (a project that exists in the registry), and, for a source without a fixed owner, `owner`; a change-metadata request may change `owner` or `project_id`; `POST /v1/projects` publishes a project record and `POST /v1/projects/remove` removes one, and accepts project records from the sources whose project support is built (`nas`). It answers 409 first for a project id homed on another source (D21), and then for any source whose project support is not built (v0.4). The hub stores the content hash of what it received: the md5 of the bytes for a file, the md5 of the rendered text for a Claude Code session, none for an OpenCode session; a NAS Claude Code point written before B2 keeps the md5 of the `.jsonl` file until the session is submitted again. The change of source is not built (v0.4). Every request carries the Bearer secret of its source, and the source comes from that secret, never from the body. **Write-time gates**, each a sanity check and not a policy: the source is registered and presents its secret; a request touches only items of that source; a published project id is not homed on another source. One secret per source, and nothing richer until tenancy (§13.3). A filesystem source's key is checked to be an absolute path and is not confined to the source's root, by design (§14 D18, D19): the hub keeps no root for an endpoint source, and isolation between sources is by `source_id`, which comes from the authenticated secret. |
| `ragnart` CLI | Read-only commands built (§1.2): `check-ignore`, `ls-files`, `status`. Planned (v0.4): `init`, `init --from`, `capture`, `add`, `add --no-commit`, `commit`, `drop`, `note`, `rm`, `mv`, `repair`, `diff`, `fsck`, `gc`, `where`, `expect`, `calibrate`, `grant`, `revoke`, `transfer`. In project work the CLI is plumbing, called by the agent (§7.3). Until then, the operations tools are the separate `utils/rag_*.py` scripts. |
| `/ragnart` skill and commands (Claude Code, OpenCode) | Planned (v0.4); today the `/rag` command. Scope and grounding (§8), librarian behaviour, the action vocabulary and the capture policy (§7.3) |
| Session-start hook, capture hook and status line (Claude Code, OpenCode) | Planned (v0.4). Project detection, the reference to a session started in a project folder (§4.5), and the project's state in every session (§7.4). The capture hook on the harness's fetch tool (§7.3) |
| Kotaemon | Browser UI: project picker as scope, uploads as operator adds into a project's `resources/`, answers by its own model through the SoHoAI gateway |

API versioning, the division of work between CLI, skill and API, and Kotaemon's fit are open
(§13).

## 13. Open issues

### 13.1 Corpus pollution

Binary and oversized content is kept out by the admission rules of §5.2. A PDF or DOCX that docling
cannot parse is never read as text. Binary text-like files are rejected by the NUL-byte and
replacement-character checks, and items over the size, quality or chunk limits are ignored with a
reason. Mirrored trees are settled: `AWS--fotel` and `Gitlab--fotel` are both indexed (decision D6 in
§14).

### 13.2 Open items in sync and tooling

None open. The purge record is the sync log line of §10.2, rule 4.

### 13.3 Undefined parts of the design

- **Project actions from a host other than the ragnart project's ingestion host.** An add, a keep or
  a commit writes in the project folder and changes the state database of the ragnart project's
  ingestion host (§7.3), and a commit from another host is refused (§10.6, R14). The ingestion API
  (§12) carries items from a source to the hub; it does not carry a project action from another host
  to a source. How a session on a laptop adds to a ragnart project on a hub source, other than by
  placing the file in that source, is not defined.
- **Sources and tenancy.** Whether access is scoped by source, and how a source's secret relates to
  a person's identity, belong to the tenancy design.
- **Embedding model change.** The rule is settled (§4.1): collection-wide, by the hub, from the
  stored text, never mixed. The mechanism is not: a second vector or a second collection, and how
  search switches from one to the other.
- **`ignored` items and a pipeline version change.** A source re-submits the items whose chunks
  carry an older version. An item that the old pipeline rejected has no chunks, so whether and how
  it is submitted again under a new version is not defined.
- **A fix inside a pipeline version.** A fix stays in the current version (D8), so no version change
  makes a source on another host re-submit the items the fix affects. On Server 2 a purge tool
  requeues them; the equivalent for a source on another host is not defined.
- **Grants of a removed ragnart project.** Whether they are kept or deleted is decided with tenancy.
- **A context boundary for `strict`.** Staying within the evidence is an instruction to the model
  (§8.1). If it proves leaky, `/ragnart ask` can be answered by a sub-call that receives only the
  retrieved passages and has no tools. Not scheduled.
- **Connectors for stores that are not POSIX filesystems.** The contract is defined (§4.7). No such
  connector is scheduled.
- **Session-start hook in OpenCode.** The Claude Code hook is defined (§7.4); its OpenCode
  equivalent is not verified.
- **Relocating a session into the project folder it created** (§7.3). Settled for Claude Code: the
  skill spawns a fork of the creating session in the project folder, and the operator switches to it
  with `/exit`, then `cd <folder> && claude --resume <id>`, an instruction the skill words for its
  client. Still open:
  - **The desktop apps.** In the Claude desktop app and the OpenCode desktop app: how the operator
    opens the fork (presumably by opening the project folder and selecting the session, if the app
    lists sessions by the same filing folders), and whether the agent's shell can run the `claude`
    command that makes the fork. Where Claude desktop files its sessions is itself unverified
    (`docs/Implementation-plan.md` §8, item 9).
  - **OpenCode.** A session's directory is a property of the session (`location.directory`), and
    octmux switches sessions in-app, so relocation may need no switch at all. Not verified, nor
    whether an OpenCode fork keeps each turn's message id.
- **Video transcripts.** Capture has no transcript fetcher.
- **Interfaces**: API versioning, CLI / skill / API division, Kotaemon fit, including Kotaemon as
  the view of a project's resources and inbox.
- **Sharing and ownership transfer**: mechanism in §6.4, not built.
- **Authentication**: OAuth with group claims, when a second user exists.
- **Notifications**: channel and events.
- **Qdrant placement**: after the corpus cleanup (§11).

### 13.4 Archive limits

- The Archive keeps the current version of each item only; no document history.
- Symbolic links: a file reached through several paths is one item, by the filesystem connector's
  duplicate rule (§1.2, deviation C; §4.7). A symbolic link never creates a reference (§4.5).
- A resource is a snapshot copy. It does not follow later edits of the original (§7.3).
- An active Claude Code session is re-embedded in full on every change while it grows.
- Secrets in sessions and files are embedded and returned; there is no secret filter.
- The static policy runs every 6 hours; a ragnart project is indexed at once by `add` and `commit` (§7.3).
- Inputs longer than 2048 bge-m3 tokens are rejected by Ollama, not truncated (§5.6).

### 13.5 Smaller items

- SoHoAI's `/v1/rag/search` redirect stub is removed on or after 2026-11-16, after checking the
  gateway log for remaining callers.
- The Claude Code `rag` command still describes itself as "Search local SoHoAI RAG".
- The name RAGNART spells Archive, Runtime, Transient: the three lifecycle tiers of §7.1.

## 14. Decision register

| ID | Decision | Rationale | Consequence |
|---|---|---|---|
| D1 | Item identity = the pair (source, natural key); the key's format is its connector's (D18); content hash (MD5) as attribute; no surrogate id | A natural key already exists and is unique within its source; hash collides and changes on edit; surrogate adds nothing inside RAGNART | Moves rewrite the key everywhere in one step; sessions keyed by id survive file moves |
| D1b | Schema version (data model) and pipeline version (`p1`, per item), documented explicitly | Separate "shape of the data" from "how content was processed" | Explicit migrations; after a change of parsing or chunking, only the items still on the old version are submitted again (D30) |
| D1c | Payload carries filterable metadata; identical copies collapse; metadata and time filters; metadata-only queries from the state DB | Operator needs "PDF modified last month", "OpenCode sessions last week"; duplicates are noise | Indexes on type, time, hash; listing endpoint |
| D2 | Every item belongs to exactly one project, its access home: a ragnart project for a file in its `resources/` or `collateral/`, otherwise the reserved `global` project (§4.6, §6.1). Grants on one ragnart project at a time, to persons and groups, with no inheritance; grants in the DB; roles `read`, `admin`; owner = metadata | One access concept instead of three; filesystem permissions unusable; NAS cannot hold access rules. Nesting has no meaning (D32), so there is nothing to inherit from | `project_id` on every chunk; auth added later without backfill. Sessions are `global` and enter a ragnart project's scope by reference (D34). `inherit_access` is reserved and ignored |
| D2b | A ragnart project is addressed by its `name`, unique across the hub registry without regard to case; its identity is its immutable `id` in `project.toml`; the path of its folder is metadata, which changes when the folder moves. Free tags, DB-only (§4.5) | People refer to a project by name; a path is transient, so addressing by path alone is fragile. Moves must not touch Qdrant; tags change often | `init` refuses a taken name. A scope accepts `name:<name>`; a path term still selects by location. Tag edits are row updates. A move changes the path, never the `id` or the name (D37) |
| D3 | Tiers by lifecycle: Transient (ephemeral cache, not indexed), Archive, Runtime; one store, one pipeline, two policies: curated in a ragnart project's managed folders, static everywhere else (D32) | Captured material is noise until kept; sessions already archive the work | No transient collection; a keep places a file and commits it (D36) |
| D3b | Project context = an on-demand listing of resources, collateral and inbox, with no index file; context implicit by location, explicit by name | No re-seeding. A file would duplicate the state database and the inbox folder, and could disagree with them | The session-start hook injects the listing |
| D4 | Scope × grounding: two different settings, scope for where evidence may come from (`project`, a selection, `all`, `web`, `none`; `web` combines with the others) and grounding for whether an answer may go beyond it (`strict`, `open`). They are not fully orthogonal: the web is a scope because it supplies evidence, and scope `none` with `strict` is nonsensical for asking. Defaults `project+strict` / `all+strict`; changes only by the operator, always shown; natural-language scope translated aloud (§8.1) | Librarian answers; no silent widening. Placing the web under grounding bundled two widenings, more evidence and no evidence, and made "only sources, with the web as a source" inexpressible | Status line; `/ragnart` commands; required `scope` in the API. `web` is resolved by the client and never sent to the API; every page read under it is captured |
| D4d | Default scope and grounding are tunable at three levels. `strict` governs every informational answer. `open` adds the model's own knowledge, uncited; the web is reached through scope, not grounding (D4) | Different ragnart projects need different defaults; an answer given without a search must not escape `strict` | `scope` and `grounding` in `project.toml`; three kinds of statement under `open` (§8.1) |
| D4b | Retrieval-only | Librarian answers; clients already have models | No model gateway; client enforces strict grounding |
| D4c | Predictable, not rigid; knobs at three levels; calibration with a labelled set; no auto-tuning | Corpus pollution is a permanent hazard; tuning needs primitives | `max_hits_per_source`, `type_weights`, `min_relevance`, `ragnart calibrate` |
| D5 | The source is the authority (today the NAS); reconciliation repairs and fails loudly; deletion rules; single writer | Mirror SoHoAI's robustness; NAS backups cover disasters | Guards in the sync; scheduled reconciliation |
| D5b | `opencode.db` local, nightly copy to the NAS per server | SQLite in WAL mode is unsafe on NFS | Backup timer installed with RAGNART |
| D5c | Daily snapshot pair, 7 kept; local transient `snapshots_path` | Balance restore points against NAS space; avoid double NAS writes | NAS snapshot area about 150 GB |
| D6 | Mirrored trees (`AWS--fotel`, `Gitlab--fotel`) are both indexed. The duplicated `_AWS` subtree is absent from the NAS | The remaining contents of the two trees differ | No deduplication rule in the scanner. A file reached through several links is queued once (§1.2, deviation C) |
| D7 | RAGNART mirrors git in ingestion, exclusion and lifecycle. It uses git's command names where the mapping is exact, and its own terms elsewhere (§1.2). Deviations A to I are recorded in §1.2 with their reasons | Git already answers the questions this system asks: what is on disk, what is excluded and why, what would change, what is indexed. Its names are widely known, so one vocabulary serves the CLI and the docs | CLI verbs follow §12. `.ragignore` follows gitignore syntax and precedence. Each deviation carries its reason |
| D8 | A pipeline version changes only on a major event: a change of parsing, admission or chunking that is incompatible with stored items or changes results significantly, or a different embedding model. Bug fixes and improvements stay in the current version (§4.1) | A version is a signal to the operator and to the calibration gate (§9.3). A new version per fix would make the signal meaningless and send every fix through the gate | A version names a compatibility epoch, not an exact recipe. Items affected by a fix are re-checked or re-ingested, with the version unchanged. A new version is applied as D30 defines |
| D9 | Project work follows the dynamics of git. A ragnart project is a folder created by `ragnart init <name>` (D32); a global `/ragnart` skill, a session-start hook and the status line make the harness aware of it; the `ragnart` CLI is plumbing behind plain-language requests. Capture is ambient, done by a hook on the harness's fetch tool, and goes to an unindexed inbox that expires after 30 days; the skill routes every web fetch through that tool; a file enters the index by being placed in a managed folder and committed, by an add, a keep or by hand (D36); distill produces collateral, indexed as derived material (§7.3 to §7.5) | Gathering happens inside a conversation, so a command per item would interrupt it. Transient material must not cause writes or deletes in the shared index | Deviation H (§1.2). No workflow states beyond scope and grounding. `capture`, `drop` and `note` in the CLI; no `distill` or `inbox` command; no `MEMORY.md` |
| D10 | `ragnart init <name>` never changes what is indexed, by construction: it creates a new, empty project folder, so nothing that existed is adopted (D32). Every new ragnart project starts with `strict` and capture on. `init` refuses a folder `<name>` that already exists (§4.5) | A ragnart project created in the current directory gave the directory a meaning it does not have, and raised the question of what to do with its content | `init` prints the settings it writes, registers the project folder (D37) and publishes the project record to the hub at once; it commits nothing. The operator changes `grounding` or `capture` by editing `project.toml` |
| D11 | One session uuid = one item, within one source, and one turn is indexed once: a forked session's copied turns, recognised by their message ids, stay with the session that first had them. When several transcript copies carry the same uuid, the newest copy by modification time wins (an exact tie goes to the greatest path) | Copies come from moved sessions and worktrees and are the same conversation; indexing each copy would duplicate the item | The scanner keeps one copy per key; the backfill resolves collisions. A fork's item holds only the turns added after the fork (§4.2) |
| D12 | `scope` on every search is a list of terms, a path term naming its source (D24): `all`; `name:<name>` (exactly one ragnart project, by name); `<path>` (exactly one ragnart project); `<path>/` (that project and those below it); `tag:<key>=<value>`. Repeated terms are unioned. The parameter is required and has no default. A term that names no ragnart project is HTTP 422 | One grammar for paths and tags, so no search silently covers nothing. The scope is stated on every request, as D4 requires | `ragnart/scope.py` is the only builder of a scope filter. `all` sends no project filter. A selection includes no `global` item except the sessions its ragnart projects reference (D34). The response echoes the projects searched (§8.8) |
| D13 | A move is a one-to-one match, within one sync, of a vanished completed file and a new file with the same non-empty content hash, excluding the empty-file hash. Any other case is a deletion plus a new item | An ambiguous match could rewrite the key of an unrelated item, and the empty-file hash matches every empty file (§4.2) | The key is rewritten in SQLite and in the `source_path` payload, with no re-embedding. Sessions never move. Moves are not counted by the mass-delete guard (§10.2) |
| D14 | A vanished item is tombstoned (`missing_since`). Its points stay searchable for 24 hours, then the first sync after that purges its points and row. Reappearance clears the tombstone. The mass-delete guard gates new tombstones, not purges | A vanished file may be a mount failure or a move not yet visible, and a grace period lets a mistake be undone without re-embedding. Purges were vetted when their rows were tombstoned | Each purge is one line in the sync log (§10.2, rule 4). A tombstoned row is not tombstoned again on later syncs |
| D15 | A file belongs to the ragnart project whose `resources/` or `collateral/` holds it, else to `global`. A session is a `global` item of its owner, keyed by its id; a ragnart project includes it by a reference (D34). The session-start hook references a session started in a project folder, once, when it starts. `launch_dir` is metadata only (§4.5) | A file's place in a managed folder is a present fact. A session's membership is provenance, recorded at a known moment, not a rule re-evaluated against a tree that moves | Moving a ragnart project changes its `projects` row and no session. A launch path spelled differently has no effect, because no rule reads `launch_dir`. Built to the earlier rule, the re-evaluation of every item on every sync, until v0.4 |
| D16 | The scheduled sync scans every configured root in one run, including the shared La-Familia root. `--user` remains available for manual runs | The per-owner loop in the scheduled run left the shared root out of the index | `utils/rag_sync_nfs.py` is called once per scheduled run. With `--user`, the scan and the deletion checks cover that owner only (§10.2) |
| D17 | Reconciliation runs in every scheduled ingestion run, on the ingestion side, against the hub's listing of each source's items. It deletes orphans (under an empty-state guard and a per-source mass-delete guard), sends again the rows that are missing on the hub or older than its pipeline version, and corrects drift in `owner` and `project_id`. It never touches the index itself, and a source whose listing cannot be read is skipped for that run | The index can drift from the queue (§10.3). Unattended repair keeps it consistent, and the hub stays the only writer of the index | One listing per source per run: about 9 s for the NAS source's 8,703 items. The guard counts items |
| D18 | A connector is a type and a source is a configured instance of one. Each connector defines its own key, locator and display formats. A key is unique within its source only, and a locator is opaque outside its connector (§4.2, §4.7) | Claude Code and OpenCode sessions already had keys and locators unlike a file path. Naming the pattern removes the assumption of one filesystem namespace | `source_id` on every item and chunk. No format claims universality. A new kind of store is a new connector, not a change to the model |
| D19 | Ingestion is multi-point. Each ingestion side owns its bookkeeping and the lifecycle of its items. The hub parses, chunks, embeds, stores and searches; it is the only writer of the index, is agnostic to a source's lifecycle, and keeps nothing of an item beyond its chunks (§2.3) | Consistency, deletion and reconciliation are specific to a source, and are decided where the source can be seen. An endpoint is reachable only some of the time, so the hub must not depend on it | Five requests per item: upsert, delete, change of key, change of metadata and change of source. The last three rewrite payload fields and re-embed nothing (D37). A silent source's items stay. After a change of parsing, admission or chunking a source re-submits when it is next reachable (§4.1) |
| D20 | Three stores: the Qdrant payload and the hub registry in the hub, and a state database on each ingestion host (§4.8) | The hub needs what retrieval filters on or displays and what gates a write; the ingestion side needs what decides a submission. One database held both only because one host played both roles | The ragnart project tables and the sources live in the registry. Each store is inspected and repaired on its own |
| D21 | A ragnart project has one home: one source and one path, on a source RAGNART can write to. Sharing is a grant, never a replica: a shared ragnart project lives on a hub source. A project id published by a second source is refused and reported, unless it arrives by `ragnart mv` or `ragnart repair` (D37) (§4.5) | A ragnart project living independently on several machines is distributed data management: consistency, races and divergent copies. RAGNART writes into the project folder, so a source that can only be read cannot hold one | Sharing stays inside the access model. A cloned or synchronised marker never forks a ragnart project; its files are `global` items. Within one source the registered path keeps a duplicated id (D29) |
| D22 | The hub's checks on a write are gates, not policy: the source is known and presents its secret, the request stays within that source, and a project id is not homed elsewhere. One secret per source. Removing the items of a source that no longer exists is a break-glass operator tool on the hub (§10.2, §12) | A submission is a write across hosts, so the source must prove which one it is. Anything richer is tenancy, which is open | No lifecycle logic in the hub. No API request and no automatic expiry for a dead source |
| D23 | A source submits bytes for a general document format, which the hub parses, and text for a format native to its connector, which the connector renders (§4.7, §5.1) | The line follows who owns the format. The hub's parsers are chosen by file type, so a document is treated identically whichever source submitted it; a native format has no second producer to stay consistent with | A connector's rendering version is kept on the ingestion side, separate from the pipeline version (§4.1). A store whose documents have no bytes exports them to a general format |
| D24 | A path term in a scope names its source: `<source>:<path>`. The API has no default source. The client completes a bare path with its default source, which is explicit, visible and changeable (§8.2, §8.8) | A path means nothing outside its source. A server-side default would hide which source was searched, against D4 | `tag` and `name` are reserved. `/ragnart status` shows the default source, and `/ragnart source` changes it |
| D25 | A hit names its origin by `source_id`, key and display label, and makes no promise that the caller can open the item (§8.7) | A key and a locator are meaningful to their own source only, and the caller may be on another host | The passage is returned in the hit. Opening an item is an operation of its connector, not of the client |
| D26 | The NAS is one source, and its crawl roots are traversal starting points inside it. `global` stays one value, not one per source (§4.6, §4.7) | Existing keys are full paths, so one source for the NAS needs no re-keying. Crawl roots are not an identity (§4.6) | Existing items gain `source_id` by backfill, with no re-keying and no re-embedding. A hub source derives owners from its own mapping; an endpoint source has one owner |
| D27 | A ragnart project is a notebook, named and identified by its `project.toml`: its material is what it manages, the content of `resources/` and `collateral/`, with `inbox/` as working material: its three managed folders. Every other file of its folder is external and not indexed (D39). There is no `mode` (§4.5, §7.2, §7.4) | The intent is a self-hosted NotebookLM: several notebooks per topic, each independent of where its folder is. A ragnart project that claimed its folder's other files would change what is indexed there, and would need a mode to say how | Deviation I (§1.2). The project folder is where the session-start hook finds the project; its path is metadata. "Crawl" names the static policy's walk. At scope `project`, a repository's own files are not found |
| D28 | An add always copies a file into the ragnart project's `resources/`, whether or not the original is an item elsewhere: files are the exception to the rule of references by stable id (D34). "Reference" is the only term for a pointer to a ragnart project or a session; a symbolic link is a filesystem construct and never creates one (§4.5, §7.3) | "Link" meant three things: a reference to a ragnart project, a reference to an item, and an instruction to ingest a file. A copy is robust and simple: it does not change or vanish with the original, it needs no filter of its own in search, and it raises no access question. The redundancy is accepted | No use for `source_links`. Identical copies collapse in results. A resource is a snapshot, with its origin in `.ragnart/provenance/` (D33) |
| D29 | A ragnart project's marker is guarded as an item is. A registered ragnart project whose marker is malformed or unreadable keeps its last good record. One whose marker is missing is tombstoned for 24 hours before it is removed. Within one source the registered path keeps a duplicated id (§4.5) | A typing error in `project.toml`, or a marker not seen in one scan, must not remove a ragnart project and what is recorded for it. The reasoning is that of deviation G | `missing_since` for ragnart projects; `ragnart repair` clears it for a folder moved by hand (D37). Not built (v0.4): today removal is immediate, and walk order decides between duplicates |
| D30 | A pipeline is what the hub does to a submitted item: parsing, admission, chunking and embedding. Retrieval is not part of it. There is one current pipeline, for every source. The version is recorded per item, and the hub stamps it on every chunk. A change of parsing, admission or chunking is applied per item as sources re-submit, and old and new coexist. A change of embedding model is applied to the whole collection by the hub, from the stored text, and is never mixed (§4.1, §5) | One version number covered three things that behave differently when they change. The vectors of two embedding models cannot be searched with one query vector. A change to retrieval touches no stored data. The hub does the processing, so the hub is the one that knows which version made a chunk | `pipeline_version` in the payload and in the listing (v0.3). Connector rendering is listed in §4.7, not §5.1. A change to retrieval is gated by the labelled set and has no version on items. The mechanism of an embedding change is open (§13.3) |
| D31 | The librarian's guarantee is split by who can keep it. The search API guarantees the evidence and its verdict on it: a hit id, the rerank score, the count of good hits and the reranker's status. The client copies the status line and the "no good hits" stop from that response, and the model never judges relevance itself: the `/ragnart` skill calls the API with a fixed request template whose scope comes from the session's settings (§8.8). Staying within the evidence in free conversation is an instruction to the model, and best-effort. Actions, including fetching a page the operator asked for, are allowed under `strict` (§8.1, §8.7, §9.2) | `strict` was stated as a guarantee while no component could detect a breach. The threshold applies to the rerank score, which a hit did not carry, so no client could apply it | Any mechanism with those two properties is enough: a template-driven call from the skill, as `/rag` makes today, or a `ragnart search` verb if the template proves unreliable. `min_relevance` is applied by the API to the raw rerank score, with one global value; `type_weights` change order only. The labelled set includes questions that should not be answered. `ragnart calibrate` finds `min_relevance` and sweeps nothing else |
| D32 | `ragnart init <name>` creates the folder `<name>`, with `.ragnart/` and the managed folders inside it; the current directory has no meaning. A ragnart project's material is only what it manages. Nesting has no meaning: a ragnart project has no parent, a nested one is external content of the outer one, and scope `project` does not include it. A marker that `init` did not make is registered only in a folder with no indexed `global` item (D41) (§4.5) | What a ragnart project is comes first, and git is the model for how it is built. Creating `.ragnart/` in the current directory tied the project to a directory by chance, and raised the question of what to do with the directory's content | `init` is `git init <dir>` with a required name. `init --from` starts a ragnart project from another with a new `id`. A hand-made marker never changes what is indexed. `parent` is dropped |
| D33 | `.ragnart/` holds state and metadata only: what would be lost if the project folder were copied and every database were empty. `project.toml` is the operator's; RAGNART writes `provenance/` and `dropped/`, one file per entry. Parsing results, queue state, hashes and a session's state are never kept there (§4.5) | `.git/` is heavy because it holds the objects; here the managed folders hold the material, references included, so `.ragnart/` needs far less. A second copy of the bookkeeping would disagree with the state database | The managed folders sit beside `.ragnart/`. The provenance of a captured PDF is recorded. Concurrent writers never rewrite a shared file (§10.6) |
| D34 | References by stable id are the rule: a ragnart project includes another ragnart project, or a Claude Code or OpenCode session, by a reference to its id. Files are the exception: they are snapshotted into `resources/` and ingested from the copy, as are captured pages. Both are resources: a reference is a `.reference` file in `resources/`, added by `ragnart add` and removed as any resource is. Live or frozen is decided per kind of material. A reference widens scope, never access, and is not transitive between ragnart projects. A missing target is reported (§4.5) | A session has a stable id and its live record is wanted; a file can be rewritten or vanish, and the notebook must not change with it. A reference is material of the ragnart project like a copy, so it lives and is managed where the material is | Sessions stay `global` items; ingestion is unchanged. One verb, `add`, copies or references by the kind of item; there is no separate reference command. "Reference" is the only term; "ref" is not used. Search matches referenced sessions by `session_id`, resolved before the search; nothing is written to Qdrant |
| D35 | Exclusion rules do not apply in a ragnart project's folder: the global crawl does not enter it (D39), and a commit applies the admission rules and no exclusion rule. A refusal is reported (§1.2, deviation D) | Placing a file in a managed folder is already the deliberate act of curation; an exclusion rule written for the archive would drop it silently | A ragnart project under an excluded directory is still found, because its folder is registered (D37) |
| D36 | Membership is by location in `resources/` and `collateral/`, and a commit indexes it. `ragnart add` copies, records the provenance, commits and waits; `ragnart add --no-commit` only copies; `ragnart commit` reconciles the managed folders with the index and waits; the scheduled run commits no ragnart project (D40). Adding a file from the project's own `inbox/` or `collateral/` moves it into `resources/`. At most one commit runs and one waits per ragnart project, and an add waits for the first commit that started after its files were in place. Every file gets an outcome: indexed, refused, failed or pending (§7.3) | Adding a source to a notebook should make it usable at once, and a file placed by hand should belong to the notebook as one placed by a command does. A commit covers everything present, so commits can coalesce | Deviations E and F (§1.2). Refusals are recorded and not retried until the file changes; failures are retried. A commit removes what left the managed folders at once, with no tombstone, and runs no deletion phase when a folder cannot be read. A copy may be edited; stale collateral is reported, never repaired. A waiting commit needs the split of the hub and the ingestion side (v0.3). The races are in §10.6 |
| D37 | `init` registers the project folder with its ingestion host; the sync reads the registered folders and walks the crawl roots, and `ragnart repair` re-registers a folder moved by hand. A file in a managed folder is keyed `ragnart://<project id>/<relative path>`, with its full path as locator. A move to another source is made by `ragnart mv`, which has the hub rewrite the `source_id` of the managed files' chunks (§4.2, §4.5) | A ragnart project's folder may lie anywhere, including outside the crawl roots and under an excluded directory. Keys relative to the project make every move a change of locator, with no matching by hash. An explicit change of home keeps "one home" safe against clones and restores | `git worktree` is the model. The ingestion API gains a fifth request, change of source (D19). Nothing is re-embedded by a move, there is no refusal window, and grants survive. The destination of a move is a writable source with an ingestion side |
| D38 | A ragnart project is created from inside a Claude Code or OpenCode session, by the `/ragnart` skill: it runs `ragnart init`, makes the new ragnart project the session's current project with scope `project` and the project's default grounding, and adds the session to it. A session works in at most one ragnart project, its current project, for its whole life, set when it starts in a project folder, when it creates a ragnart project, or when it opens an existing one (`/ragnart open`, the same sequence without `init`); the skill refuses to create a ragnart project from a session that has one. Every `ragnart` command acts on the current project only; acting on another ragnart project is not permitted. A session may still be referenced as material by other ragnart projects (§4.5, §7.3) | `ragnart init` is plumbing and makes no assumption about its caller; the workflow belongs to the skill. A session creating or switching between ragnart projects would be nesting by another route, and an action on a project other than the current one invites confusion between where a session is and what it changes | The CLI has no option that names another project: it finds its project from the working directory. After `init` the session's shell moves into the project folder with a persistent `cd` and stays there; to relocate the session itself, the skill spawns a fork of it in the project folder, a native session of that folder that both sessions' working references then share, and the operator switches to it with `/exit`, then `cd <folder> && claude --resume <id>`; the switch is the operator's, by one instruction the skill words for its client, because it works in every client; the in-app `/resume` cannot switch across filing folders, and the desktop apps are not verified (§7.3, §13.3). The current project is recorded by the session's reference in the ragnart project, marked `working = true`; there is no separate store of session state, and a change of scope or grounding lasts for the conversation only (§4.5). Creation from a shell is the exception, and no session is in the ragnart project until one starts in its folder or is added |
| D39 | The global crawl does not enter a ragnart project's folder: for the crawl, a `.ragnart/` marker is a `.ragignore` holding `*`. The crawl reads the marker to register the ragnart project, and nothing in the folder becomes a `global` item. `resources/` and `collateral/` are indexed only by commits; the other files of the folder, and `inbox/`, are not indexed. `.ragnart/` is never ingested (§1.2, deviation D; §4.5; §7.2) | The global crawl and a ragnart project are two kinds of ingestion with different meanings. A crawl walking a project folder would give the project material it did not choose, or turn its files into `global` items | `config/ragignore` excludes `.ragnart/`. `check-ignore` names the project folder as the reason. Collateral is indexed as the ragnart project's output; it becomes a resource only by `ragnart add`, which moves it |
| D40 | There is no implicit commit. A ragnart project's index changes only by `ragnart add`, `ragnart note`, `ragnart commit` and `ragnart drop`; the scheduled run commits no ragnart project. Uncommitted changes are counted by the session-start hook and the status line, and the skill offers a commit, never makes one (§7.3) | Every change to a ragnart project's index stays a deliberate act, and every outcome of a commit has someone to report it to. A timer commit would have no caller, and refusals and failures would be silent | A file placed or deleted by hand waits for a commit, as an unstaged change does in git. `drop` commits. `gc` changes no index. Live references (sessions, other ragnart projects) still change the scope without a commit, by design |
| D41 | No marker changes what is indexed. A marker with an unknown `id` found by the crawl is registered only in a folder that holds no indexed `global` item; otherwise it is refused, logged at ERROR on every run, and the folder is walked as if it had no marker. A code repository is not a ragnart project (§4.5) | Under D39 a marker removes its folder from the crawl, so a marker placed by hand in a folder of material would delete that material from the index after the grace period, below the reach of the mass-delete guard. The reasoning is that of deviation G | Turning an existing folder of material into a ragnart project is not supported: `init` a new one and `add` what is wanted. A ragnart project about a repository is its own folder. The RAGNART repository's hand-written marker is removed in the v0.4 migration |
| D42 | The role that runs the pipeline, the index and the search API is called the hub; its registry is the hub registry, and a source run on the hub's own host is a hub source (§2.3, §4.7, §4.8) | "Central" named both the role and a kind of source, two different things sharing a word | The ingestion side keeps its name. `init` publishes the project record to the hub at once (D10) |

## 15. Future considerations: use by a team or an organisation

RAGNART is designed for one household lab and one operator (§1, §2.1). Use by a team or a small
organisation, as a shared service that manages the context given to models, is not a goal of any
planned version. This section records which choices would be hard to change if it became one, so
that the notebook work of v0.4 does not close the option by accident. Nothing here is a decision:
an item enters the decision register (§14) only when a version adopts it.

**Status.** Considerations only. The provisions of §15.2 are confirmed or declined at the v0.4a
kickoff (`docs/Implementation-plan.md` §6, §8 item 21). §15.3 is not scheduled.

### 15.1 What is hard to change

A choice is as hard to change as the place it is recorded in.

| Where a choice is recorded | Cost of a later change | Why |
|---|---|---|
| The hub registry (§4.8) | Low | One database on one host, with numbered migrations (§4.1). Tags, references and grants are rows |
| A state database (§4.3) | Low | Bookkeeping. A lost one is rebuilt from the hub's listing (§4.8) |
| The Qdrant payload (§4.4) | Low to moderate | Rewritten by filter, with no re-embedding, as the metadata backfill and the source backfill were. The cost grows with the corpus |
| The vectors | High, with a defined procedure | A pipeline version change (§4.1) |
| Files in project folders: `project.toml`, the `.reference` files, `.ragnart/provenance/` (§4.5) | High | They lie in folders on every source that holds ragnart projects, they are the authority (§4.8), and RAGNART never rewrites `project.toml` (§10.6, R11). No migration reaches them |
| Grammar that clients hold: scope terms (§8.8), key formats (§4.2), CLI verbs, the skill's request template | High | Every client carries a copy, so a change needs every client changed together |

The first three rows are where the design already keeps what changes often. The last two are where
v0.4 writes for the first time.

### 15.2 Provisions that are cheap when v0.4 writes its first files

Each provision keeps an option open at the cost of a field or a reserved word. Each would cost a
migration of files or of clients if it were added after ragnart projects exist.

| # | Provision | What it keeps open | Why it is hard later |
|---|---|---|---|
| P1 | A format version in every file RAGNART defines in a project folder: `project.toml` (written by `init`), each `.reference` file, each provenance entry | Any later change to those formats | Files in project folders have no migration (§15.1). A reader that finds no version cannot tell an old file from a damaged one |
| P2 | A reference to a session names the session's source as well as its id: kind, `source_id`, id. `project_references` gains the column | References when one hub serves several people | An item's identity is the pair (source, key), and the same uuid submitted by two sources is two items (§4.2). A reference by id alone, matched by `session_id` (§8.8), covers both, and only the access rule then keeps them apart. A reference to a ragnart project needs no source: a ragnart project has one home (D21) |
| P3 | A provenance entry records the origin of a copy as an identity: `source_id` and key when the original is an item, the URL for a page, the path otherwise | Rules on what may be added to a shared ragnart project | An add copies (D28), and the copy falls under the ragnart project's access rule (§7.3). In a shared ragnart project an add therefore shares the content with everyone who holds a grant. Whether the person who added it could read the original, and whether the original was later restricted or deleted, can be answered only from a recorded identity |
| P4 | Clients address a ragnart project by its `id`: a scope term `id:<uuid>`, with `id` reserved among the source ids (§4.7). `name:` stays the term for people. A name contains no `/` and no `:` | Names unique per owner or per group, instead of across the hub | Names are unique across the hub registry (D2b). With many people, two will want the same name. Qualifying names by owner changes what `name:<name>` means, and the skill's request template sends that term on every search (§8.8) |
| P5 | Every search request names its principal, and `resolve_scope()` receives it, including where it resolves references | Enforcing grants without changing any caller | The seam is in place (§6.5). It stays a seam only if no client builds a filter and no path resolves a scope without a principal. v0.4 adds the first new path: references (§8.8) |
| P6 | Nothing that expresses membership by reference, or access, is copied into the payload: references, tags and grants stay in the hub registry | Changing who may read, or what a ragnart project references, without touching Qdrant | Already the design (§4.5, D34). It is listed because v0.4 is where the shortcut is tempting: a field on chunks naming the ragnart projects that reference them would simplify the scope filter and make every reference a payload rewrite |
| P7 | `add` and `commit` reach the index and the hub registry only through the ingestion API (§12) | A project action started on another host (§13.3) | Already the design (§7.3). A command that wrote the registry or a state database directly, from the ragnart project's own host, could not later be run from a laptop |

### 15.3 What v0.4 does not address

These parts of the design hold for one operator and would need design work before a team used
RAGNART. None is made harder by v0.4, provided the provisions of §15.2 hold.

- **Access to `global` items.** The owner rule governs them permanently (§6.5): an item is read by
  its owner, or by everyone when its owner is the shared space, and the owner comes from the
  registration of the item's source (§4.7). In an organisation most material would be `global`
  (shared drives, wikis, tickets), and the store it comes from already says who may read each item.
  The connector contract (§4.7) has no element for that: a connector reports no readers for an item,
  and no change token for them. The payload side would be a rewrite (§15.1). The contract is the
  hard part, because it touches every connector and every item.
- **Identity.** The `user` of a search is trusted as given, and a source's secret identifies a
  source, not a person (§12). Both are v0.6, and how the two relate is open (§13.3).
- **Where a ragnart project lives.** A ragnart project has one home, a folder on a source RAGNART can
  write to (D21). Its folder is the authority (§4.8). An action on it from another host is refused
  (§10.6, R14). A session works in it only on the host that holds it (D38). A team on laptops meets
  all four at once. The alternative, with the hub registry as the authority and the folder as one
  client's working copy, reverses D33 and §4.8. It stays reachable as long as the hub never depends
  on a ragnart project's path (D2b), and P4 and P7 hold.
- **Copies.** Every add duplicates a file (D28). The redundancy is accepted for one operator. With
  many people adding the same documents it multiplies storage and ingestion, and each copy is
  detached from its original (P3).
- **What may be used for inference, and where.** An item carries an owner, a project, a source and a
  type, and nothing that says how sensitive it is or which models may receive it. RAGNART returns
  passages to any client that asks, and the client chooses the model (D4b). There is no secret
  filter (§5.6), and sessions, the largest kind of item in the corpus, hold whatever was typed into
  them. A sensitivity class would be a payload field and a filter (§15.1); a default per source,
  set when the source is registered, is the cheap place to start it. A search is written to the
  service log and to no audit record.
- **One hub.** A state database on NFS opened by one host, fail-fast locks, one intake worker on a
  timer and a crawl every 6 hours (§10.3) fit one operator. They are implementation behind the
  ingestion API and the search API, and can be replaced without changing either.

**Not hard to change.** The session mechanics (the current project, the hooks, capture, the inbox
and the status line) are client-side and specific to each harness (§7.3, §7.4). They can be rebuilt
without touching stored data.
