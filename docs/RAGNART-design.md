---
title: "RAGNART — Design"
created_at: 2026-11-13--23-55
created_by: Claude Code (Claude Opus 5.5)
updated_by: Claude Code (Claude Fable 5.2); Claude Code (Claude Haiku 5.5, 2026-11-15 -- §10.3 single-writer lock for every index writer, holder and fail-fast exit 75; §13.2 sync-lock item removed); Claude Code (Claude Haiku 5.5, 2026-11-15 -- v0.2 session A invariants)
updated_at: 2026-11-15--20-04
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

1. **Retrieval** — given a question and a scope, return the relevant passages with exact
   provenance.
2. **Knowledge-base management** — ingestion, access control and lifecycle, per project.

Its origin is a self-hosted NotebookLM: a per-project set of sources that a conversation is
grounded in. That idea is generalised here into projects whose context is available
automatically, built up by capturing material as the work goes, keeping what matters, and
producing collateral from it (§7.3).

**Principles.** Every design decision below follows from these.

| Principle | Meaning |
|---|---|
| **Librarian answers** | Answers are facts, references and quotes from the material at hand. Nothing is extrapolated or guessed unless the operator explicitly opens the grounding (§8). |
| **Retrieval-only** | RAGNART returns evidence; it never writes answers and contains no model gateway. Clients (Claude Code, OpenCode, Kotaemon) compose answers. |
| **The NAS is the authority** | Files and session stores on the NAS are the truth. The state database is bookkeeping; the vector index is derived. |
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
| Parallel with git (§1.2): ignore rules and the read-only `ragnart` commands `check-ignore`, `ls-files`, `status` | Built. `add`, `rm`, `mv`, `diff`, `fsck`, `gc`: v0.3 |
| Item identity by path for NAS files, `opencode://` keys (§4.2) | Built; `claude-session://` keys, content hash and move detection: v0.2 |
| State-database metadata and payload fields beyond today's (§4.3, §4.4), schema version (§4.1), per-item pipeline version | v0.2 |
| Projects, `project.toml`, tags, grants, `resolve_scope()` / `authorize()` (§4.5, §6) | v0.2 (data model); authentication and grant management: v0.5 |
| Scope and grounding in the API and the `/ragnart` skill and commands (§8) | API scope and project listing: v0.2; skill and commands: v0.3 |
| Project workflow: `init`, capture, the inbox, keep and drop, collateral, `add --wait`, the `/ragnart` skill, the session-start hook and the status line (§7.3 to §7.5) | v0.3 |
| Retrieval-quality knobs and calibration (§9) | `max_hits_per_source` and collapse: v0.2; other knobs and calibration: v0.3 |
| Deletion rules (§10.2), reconciliation (§10.3) | Guards, owner scoping and synchronous Qdrant deletes: built. Tombstone and grace period, reconciliation: v0.2. Purge record: not yet scheduled (§13.2) |
| OpenCode database backup (§10.5) | v0.2 |
| Kotaemon (§12) | v0.4 |

### 1.2 Parallel with git

RAGNART follows git as closely as it can in ingestion, exclusion and lifecycle. Where a RAGNART
command does exactly what the git command of the same name does, it takes that name. Where the
mapping is not exact, RAGNART uses its own terms: the Archive, Runtime and Transient tiers (§7.1),
the queue, the index, the project, and the static and curated policies (§7.2). The reasons for
each deviation are recorded here and in decision D7 (§14).

| git concept | RAGNART | Note |
|---|---|---|
| Working tree | The NAS sources under the NFS roots | The authority. RAGNART does not modify them. It writes only inside `.ragnart/` directories (planned, §7.3) and appends lines to `.ragignore` files (by `rm`, planned, deviation B). |
| Untracked files | A project's inbox, `.ragnart/inbox/` | On disk and not in the index (§7.1). |
| Staging area | Queue rows with status `pending` | Queued for ingestion, not yet indexed. |
| Committed content | The index: Qdrant points and `completed` queue rows | Derived from the NAS (§10.1). |
| Repository (`.git`) | Project (`.ragnart/project.toml`) | Addressed by path (§4.5). `init` (planned) is `git init`. |
| `.gitignore` | `.ragignore`, one per directory | Same syntax and precedence (below). |
| `core.excludesFile` | `config/ragignore` | Global exclusion rules with the lowest precedence. There is no `.git/info/exclude` equivalent. |
| System, global and local config | `ragnart.yaml`, then `project.toml`, then the request | Each level overrides the one before it (§9.2). |
| `git status` | `ragnart status` (built) | Queue and Qdrant counts; `--ignored` lists ignored files with their reason. |
| `git ls-files`, with `--others`, `--ignored`, `--stage` | `ragnart ls-files` (built), same flags | Without a flag, lists indexed files. `--stage` lists every queue row with its status. Planned: scoped to a project, the listing is the project's catalogue, and `--others` lists its inbox (§7.4). |
| `git check-ignore -v` | `ragnart check-ignore -v` (built) | Prints the file, line and pattern that decide each path. |
| `git add` | `ragnart add` and `ragnart add --wait` (planned) | `add` queues; `add --wait` blocks until the item is indexed (deviation E). On an inbox item, `add` is the operator's "keep" (§7.3). |
| `git rm` | `ragnart rm` (planned) | Appends an anchored line to the nearest `.ragignore`; never deletes a file (deviation B). |
| `git mv` | `ragnart mv` (planned) | Move detection by `content_hash` (§4.2); nothing is re-embedded. |
| `git diff` | `ragnart diff` (planned) | The plan of the next sync, shown without applying it. |
| `git fsck` | `ragnart fsck` (planned) | Reconciliation (§10.3). |
| `git gc` | `ragnart gc` (planned) | Purges tombstones after the grace period (§10.2), and removes expired and dropped inbox items (§7.5). |
| Staging everything on each run (`git add -A`) | Static policy, run by the timer | Every 6 hours, all files that the inclusion and exclusion rules admit (§7.2). |
| Staging chosen paths (`git add <path>`) | Curated policy: `add` | Only what a project chooses (§7.2, §7.3). |
| `git commit`, `git log`, branches, remotes | None | Deviation F. The snapshot pairs (§10.4) are the nearest equivalent to tags and restore. |

**Deviations from git.** Each deviation is named by a letter and states its reason.

- **A. Ignore rules also remove indexed content.** The index is the NAS minus what the rules
  exclude. A newly excluded item leaves the index through the normal sync, under its deletion
  rules (§10.2). The sync log names the `.ragignore` file and line that matched. *Why:* git's
  ignore rules never affect tracked files. Here that would keep excluded content in the index
  indefinitely, which is what the exclusion exists to prevent.
- **B. `rm` appends a rule and never deletes a file.** `ragnart rm <path>` (planned) adds an
  anchored pattern to the nearest `.ragignore`. *Why:* the exclusion is durable, visible in a text
  file, and reversed by removing its line. The NAS is the authority, so RAGNART does not delete
  files from it. The one exception is files under a project's `.ragnart/` (§7.5).
- **C. Symbolic links are followed.** The walk follows links, and patterns match the path as
  walked, never a resolved path. A file reached through several links is queued once, under the
  first path the walk meets. *Why:* `Gitlab--fotel` is reached through a link, and a rule should
  match the path the operator sees.
- **D. One ignore hierarchy across nested projects.** `.ragignore` files apply across project
  boundaries within an NFS root. *Why:* a project is a folder marked with `.ragnart/`, not a
  separate repository. A boundary that reset the rules would let excluded content in through the
  project.
- **E. `add` queues; `add --wait` blocks.** *Why:* git's `add` is cheap because hashing is cheap.
  Ingestion parses on the CPU (§5.1) and can take a long time per item, and a blocking default
  would hold the caller for that time. The ingestion run is the single writer (§10.3), so the queue,
  not the caller, drives the index.
- **F. No history, branches or remotes.** *Why:* the NAS is the authority and already keeps
  history: hourly Synology snapshots and an offsite copy (§10.1). A second history inside RAGNART
  would duplicate it and could disagree with it. The snapshot pairs (§10.4) are the restore points
  of the index.
- **G. An unreadable `.ragignore` stops the sync.** Git warns and continues. RAGNART raises an
  error: the sync exits with a non-zero code and runs no deletion phase. *Why:* an exclusion that
  cannot be read must neither admit content it would exclude nor delete content it would keep.
  Continuing would do one or the other.
- **H. Capture is ambient.** In a project with capture on, a page the agent fetches is written to
  the inbox without a command from the operator, by the policy of the `/ragnart` skill (§7.3).
  *Why:* git records nothing unless asked. Project work finds its material during a conversation,
  and a command per item would interrupt it. Captured material stays out of the index until the
  operator keeps it, so the index still changes only by a deliberate act.

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
- The rules apply to the NAS crawl only, not to the Claude Code and OpenCode session scanners.
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
| **Server 2** — NucBoxK11, 192.168.1.95 | `ragnart-search.service` (HTTP API, port 8100); `ragnart-ingest.timer` (ingestion 01, 07, 13, 19:00); Ollama with bge-m3 (port 11434); the reranker bge-reranker-v2-m3 behind llama-swap (port 8000); the only host that opens `rag_state.db`; the OpenCode server (port 4096) |
| **NAS** — Synology DS1019, NFS `/mnt/nfs` | The corpus (NFS roots), Claude Code sessions (`~/.claude` is a link into NFS), RAGNART's data root `/mnt/nfs/__Backups/RAGNART--databases/` (state database, logs, snapshot pairs, ingestion lock). BTRFS with hourly snapshots and an offsite copy. |

Clients: Claude Code and OpenCode sessions (through the `/ragnart` skill and commands), the
`ragnart` command-line tool, and Kotaemon as the browser UI. A SoHoAI gateway on Server 1
supplies models to clients; RAGNART does not use it.

### 2.2 What lives where

| Data | Location | Protection |
|---|---|---|
| Documents, Claude Code sessions | NAS | Synology snapshots + offsite |
| `rag_state.db`, logs, snapshot pairs | NAS, `RAGNART--databases/` | Synology snapshots + offsite |
| Qdrant live index | Server 1 local NVMe (Qdrant refuses NFS) | Daily snapshot pair on the NAS (§10.4) |
| Qdrant transient snapshot | Server 1 `/var/lib/qdrant/snapshots` | Exists only during a snapshot run |
| OpenCode's database | Server 2 local NVMe (SQLite in WAL mode is unsafe on NFS) | Nightly online copy to the NAS (§10.5) |
| Installed configs and units | Local `/etc` on both servers | Reinstallable from the repository (on NFS) |

## 3. The layered model

RAGNART is organised in four layers. The first two are the same for every kind of material;
the last two are where kinds of material differ.

| Layer | Scope | Content |
|---|---|---|
| **Data and access model** | Global | Identity, metadata, projects, grants (§4, §6) |
| **Ingestion mechanics** | Common | One pipeline, versioned (§5) |
| **Ingestion policy** | Per kind of material | **Static**: the inclusion rules of `ragnart.yaml` and the exclusion rules of §1.2, unattended, every 6 hours. **Curated**: only what a project deliberately keeps (§7) |
| **Curation (lifecycle)** | Per tier | Archive: blunt, mirrors the filesystem. Runtime: session records. Transient: material in flight (§7) |

## 4. Data model

### 4.1 Versions

Two version numbers govern the data. They are independent.

- **Schema version — the data model.** One value for the whole system, stored in the state
  database. It names the set of tables and columns and the Qdrant payload layout. Each change
  to the data model is a numbered migration that runs once and records the new version.
- **Pipeline version — how an item's content became chunks and vectors.** Stored **per item**.
  The current pipeline is **`p1`**, defined in §5. A pipeline version changes only on a major
  event: a change that makes items stored under the old version incompatible with new ones, or a
  significant change of behaviour. Examples are a different embedding model, a change of reranking,
  and a change to processing that alters results. Bug fixes and improvements to the current pipeline
  are part of `p1` and do not create a version. Because the version is per item, the collection can
  hold a mix, and a new version re-embeds only the items still on the old version, in batches,
  instead of the whole corpus.

### 4.2 Identity of an item

An **item** is one ingestable thing: a file, a session, a captured page or an upload. It is
split into many chunks (Qdrant points).

**The identity of an item is its natural key**, chosen per type:

| Type | Key |
|---|---|
| NAS file | Full path, e.g. `/mnt/nfs/Florian/…/report.pdf` |
| Claude Code session | `claude-session://<session uuid>` |
| Claude Code subagent transcript | `claude-session://<session uuid>/agent-<id>`, in the parent session's project (not yet built) |
| OpenCode session | `opencode://<session id>` |
| Captured page, operator-added file, note, collateral | Its path under the project's `.ragnart/`; the URL of a capture is an attribute |

Three kinds of identifier exist, and only the natural key serves as identity:

| Kind | Example | Why it is or is not the key |
|---|---|---|
| Natural key | path, session id | Already exists, unique per item, and is also the **locator**: needed to read the item, delete its chunks, cite it, and place it in a project |
| Content hash | MD5 of the bytes | Not unique per item (identical copies collide) and changes on every edit; stored as an indexed **attribute** |
| Surrogate id | a generated UUID | Adds a mapping table but no capability: keeping it stable across a move needs the content hash anyway. Only useful for references held outside RAGNART; not used for items |

The file name alone is not a key: of 11,772 indexed NAS files, 6,495 names are distinct and
7,204 files (61%) share a name with another file (`README.md` 219 times,
`Timeline--meeting-notes.md` 254 times).

**Moves.** A file that disappears at one path while the same `content_hash` appears at another
is a move. The key is rewritten in one operation in the state database, in the payload of its
chunks and in any link rows; nothing is re-embedded. A file both moved and edited between two
scans is seen as a deletion plus a new file.

**Sessions** are keyed by session id because their files move: `~/.claude/scripts/
project-chats.sh` relocates a session's `.jsonl` between project folders. With the session id
as key, such a move only changes the session's project.

**Hashing** is MD5 (change and move detection, not security), computed only when a file's size
or modification time changed.

### 4.3 Per-item metadata (state database)

One row per item in the state database. Changing a value here is cheap.

| Field | Meaning |
|---|---|
| key | Natural key (§4.2), primary key |
| `source_type` | `file`, `claude_session`, `opencode_session`, `web_capture`, `upload`, `collateral` |
| `file_type` | Extension class: `pdf`, `md`, `txt`, … |
| `project_id` | Project the item belongs to (§4.5) |
| `owner` | Accountable person (metadata, §6) |
| `size_bytes`, `content_hash`, `hashed_at` | Change, move and duplicate detection |
| `first_seen_at` | When RAGNART first discovered the item |
| `modified_at` | Per type: file mtime; session = time of last activity; capture = fetch time |
| `ingested_at` | When its chunks were last written |
| `pipeline_version` | `p1`, … (§4.1) |
| `chunk_count` | Number of points written; used by reconciliation (§10.3) |
| `status`, `error_msg`, `skip_reason`, `retry_count` | Queue state |
| `missing_since` | Tombstone: when the item was last seen missing (§10.2) |
| origin details | Captures: `url`, `fetched_at`. Sessions: `session_id`, start time, title, launch directory (`cwd`) |

### 4.4 Per-chunk payload (Qdrant)

The payload is copied onto every chunk of an item, so it carries only what a search needs to
filter on or to display.

**Status.** Today's payload carries `source_path` (indexed), `owner`, `file_name`, `file_type`,
`chunk_index`, `text`, `parent_text`, `page`, `tag`, and on sessions `session_id`,
`session_title` and `project` (a folder or session name, not a project id). The fields
`project_id`, `source_type`, `modified_at`, `ingested_at` and `content_hash`, and all indexes
except `source_path`, are added by the v0.2 metadata backfill (no re-embedding).

| Field | Indexed | Purpose |
|---|---|---|
| key (`source_path`) | keyword | Identity; delete and replace by key |
| `project_id` | keyword | Access and scope filter |
| `owner` | keyword | Metadata, display, optional filter |
| `source_type`, `file_type` | keyword | Metadata filters |
| `modified_at`, `ingested_at` | datetime | Time filters |
| `content_hash` | keyword | Collapse identical copies in results |
| `file_name` / title | — | Display |
| `chunk_index`, `text` (child, embedded), `parent_text` (returned) | — | Content |

Project tags are **not** copied into payloads (§4.5). The current fields `page` (always `0`)
and `tag` (always empty) carry no information; they are either implemented (a real page number
helps citations) or dropped.

### 4.5 Projects

A **project** is a directory that contains `.ragnart/project.toml`:

```toml
name = "Acme"                      # required, human-readable
id   = "3f9c2e1a-6d1b-4c0e-9a57-2b8f0c4d7e11"   # written by `ragnart init`; never edited
inherit_access = true              # optional, default true (§6.3)
mode = "curated"                   # curated | crawl; written by `ragnart init` (§7.4)
grounding = "strict"               # optional: default grounding in this project (§8.2)
capture = true                     # optional: ambient capture into the inbox (§7.3)
inbox_expiry_days = 30             # optional: lifetime of an inbox item (§7.5)
kind = "customer"                  # optional tags: any key = value
segment = "ISV"
```

- **Addressing.** A project is addressed **by its path** (`~/Gin-AI/projects/AYA`). The path
  is unambiguous and needs no name resolution.
- **`id` is the immutable, unique project identifier.** It lives inside the folder, so it
  travels with any move or rename: moving a project changes one row in the state database and
  nothing in Qdrant, although every chunk carries `project_id`. An unmarked NFS root's project id
  is generated and stored in the `projects` table; a marked directory's id is the one already
  written in its `project.toml`.
- **Parent** is the nearest ancestor directory that is also a project. Each NFS root is a
  top-level project; unmarked directories belong to the enclosing project.
- **Sessions project and session matching (not yet built).** Each owner has a `sessions` project,
  the fallback project for any item with no enclosing marked project. A session's project is found
  by resolving its launch directory (Claude Code `cwd`, OpenCode `location.directory`) to its
  physical path, with symlinks resolved, and matching that path against the physical paths of
  projects. A launch directory that matches no project falls back to the owner's `sessions`
  project. Known realpath aliases between mount points (for example `/mnt/nfs/fotel--NAS-home/...`
  and `/home/florian/Gin-AI`) need an explicit mapping. An NFS-located project is an artefact of
  the current setup, not a rule.
- **Tags** are arbitrary `key = value` pairs (every key except the reserved `name`, `id`,
  `inherit_access`, `mode`, `scope`, `grounding`, `capture`, `inbox_expiry_days`). They are stored as rows `(project_id, key, value)`. On each sync
  (and on the `ragnart` command) the tag set read from `project.toml` replaces the stored set:
  changing `industry = "ISV"` to `segment = "ISV"` deletes one row and adds one, for the same
  `project_id`. A tag filter is resolved to a set of project ids in the database before the
  search, so tag changes never touch Qdrant.

Tables: `projects` (id, parent, path, name, owner, inherit_access, mode), `project_tags`,
`project_links` (a project referencing another), `source_links` (one item referenced from
another project; an alias symlink becomes such a link), `grants` (§6).

**The `init` invariant: `ragnart init` never changes what is indexed.** It chooses the project's
settings from the state of the folder, writes them to `project.toml`, and prints them:

| Folder at `init` | `mode` | `grounding` | `capture` |
|---|---|---|---|
| Holds indexed content | `crawl` | `open` | `false` |
| Holds no indexed content | `curated` | `strict` | `true` |

A folder that is already indexed, such as a code repository, stays indexed in full and keeps
ordinary session behaviour. A new folder becomes a knowledge project. The operator changes any of
the three settings afterwards by editing `project.toml`.

## 5. Pipeline `p1`

`p1` is the current pipeline: the processing of §5.1 to §5.4, which decides what is stored, and the
retrieval of §5.5, which reads it. Its version rule is in §4.1.

### 5.1 Parsing

| Input | Parser |
|---|---|
| `pdf`, `docx`, `pptx` | docling 2.89.0 `DocumentConverter`, exported to Markdown. The accelerator is set explicitly to CPU for each type, so the device is chosen in code and not by docling's automatic choice. PDF layout and table models run on the CPU. |
| `pptx` when docling fails | python-pptx 1.0.2, after LibreOffice conversion for OLE2 files. If python-pptx also fails, the item fails (§5.2). |
| `pdf`, `docx` when docling fails | No fallback. The item fails with `docling failed: …` and follows the retry rule of §5.2. |
| `ipynb` | RAGNART's notebook parser (cells as text) |
| Claude Code session (`.jsonl`) | RAGNART's parser: user and assistant text turns only; tool calls and tool results are dropped |
| OpenCode session | Read from the OpenCode server API: user and assistant text |
| All other included extensions | Read as UTF-8 text with replacement, then subject to the text-like rules of §5.2 |

### 5.2 Admission

Admission decides, before any embedding, whether an item is indexed. A rule that rejects an item
sets its queue row to `ignored`, and `skip_reason` records the rule with the measured value.
`ragnart status --ignored` lists these rows with their reason and retry count. The thresholds and
caps are the values of `rag.admission` in `ragnart.yaml`. The purge tools in `utils/` use the
same definitions, so they cannot drift from ingestion.

**Before queueing (scan time).** An item is considered only if its extension is in
`rag.scanner.include_extensions` (`ragnart.yaml`) and no exclusion rule of §1.2 matches it. An
already-indexed item that an exclusion rule now matches leaves the index through the sync (§10.2).

**After queueing (ingestion), in this order.** The size, NUL-byte and replacement-character rules
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
present today, and `page`, which is always `0`. Before a file's points are written, all points for
its `source_path` are deleted. The new points are upserted in batches of 256.

### 5.5 Retrieval

Search is part of `p1`, because its results depend on the pipeline: a change to it that changes
results is a version change (§4.1). Search does not change stored data. The steps, as built in
`ragnart/search.py` and `ragnart/rerank.py`:

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
   is logged.
5. The first `limit` results are returned, each with its `parent_text` as content.

The `score` field is the Qdrant score: the RRF value in hybrid search. The relevance signal is the
rerank score (§9.2).

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

### 6.1 Everything is a project

Access is defined **only through grants on projects**. Each NFS root is a top-level project
(the operator's root, the shared family root); folders marked with `.ragnart/` are
sub-projects; every item belongs to exactly one project. "Shared with the family" is a
top-level project granted to each family member, not a special owner value.

`owner` is the **accountable person** of a project and its items: shown as metadata and given
the default grant, but not an access filter.

**Why not filesystem permissions:** both NFS roots are mode `777`, nearly all files belong to a
single uid, and household members have no separate Unix accounts. File modes are recorded as
hints only.

### 6.2 Principals

A principal is a **person** or a **group**. Groups come from the identity provider (the groups
claim of an OAuth token), so team membership is managed there. Clients (Claude Code, OpenCode,
the CLI, Kotaemon) act **as the person** using them; they are not principals of their own.

### 6.3 Grants and inheritance

- A grant gives a principal a role on a project. It applies to the project and to its
  sub-projects.
- A project with `inherit_access = false` in `project.toml` does not inherit grants from its
  parents: only its own grants apply. There are no deny rules.
- Grants live in the state database and are managed through the CLI or API. `project.toml`
  holds structure only (the NAS is writable by anyone with access, so it cannot hold access
  rules).

Example (a sales organisation):

```
/Sales/EMEA/            project, granted to group sales-emea         (regional plans)
/Customers/Acme/        project, granted to Alice (account manager)  (customer material)
/Sales/EMEA/Acme/       if a customer sits under a region instead: inherit_access = false,
                        granted to Alice only, so the EMEA team does not see it
```

### 6.4 Roles

| Role | Rights |
|---|---|
| `read` | Search and read the project's items |
| `admin` | Manage the project's grants |

**Sharing and ownership transfer** are not implemented. The mechanism that fits the existing
primitives:

- A `share` role: its holder may add and remove grants on the project (and on sub-projects
  that inherit), up to their own level, never `admin`. Checked by the same seam:
  `authorize(principal, "grant", project)`. Grants record `granted_by` and `granted_at`.
- Ownership transfer is a database transaction: set the project's owner, add the new owner's
  default grant, optionally revoke the old owner's. Access is project-based, so Qdrant is not
  touched.
- Interfaces: `POST`/`DELETE /v1/projects/{id}/grants`, `POST /v1/projects/{id}/owner`;
  `ragnart grant | revoke | transfer`.

### 6.5 Enforcement

One function turns a request into a filter, `resolve_scope()`, and one decides access,
`authorize()`. No other code builds a scope filter. The `grants` table starts empty (not yet built).
While it has no rows, `authorize()` applies the owner rule of `ragnart/search.py`: an item's
owner may always read it, and so may anyone when the item's owner is `la-familia`.
Authentication (OAuth with group claims) is added when a second user exists.
Multi-tenancy is designed now and built later: the data model (project ids on every chunk,
grants shaped for persons and groups) is in place from v0.2 so nothing has to be backfilled.

## 7. Tiers and lifecycle

### 7.1 Three tiers, distinguished by lifecycle

| Tier | Lifecycle | Where | Vector-indexed | Leaves the tier by |
|---|---|---|---|---|
| **Transient** | Material in flight: captured, not yet kept | Files in a project's `.ragnart/inbox/`, or an attached folder | **No.** The agent reads the files directly. | Keep (becomes a resource in the Archive), drop, or expiry after 30 days (§7.5) |
| **Archive** | Persistent | NFS roots (static crawl); project resources and collateral (curated) | Yes | The file is removed (after the grace period, §10.2) |
| **Runtime** | Persistent session record; a specific case of Archive | Claude Code and OpenCode sessions | Yes | — |

There is **one persistent store** (the `documents` collection, an alias of `documents_new`)
and **one pipeline**. The Transient tier is an ephemeral cache, not a store: material there is
used while work is in progress and becomes persistent only by being kept.

### 7.2 Two ingestion policies

| Policy | What enters | Trigger |
|---|---|---|
| **Static** | Every file under the NFS roots that `ragnart.yaml` includes and no rule of §1.2 excludes, plus every Claude Code and OpenCode session | `ragnart-ingest.timer`, every 6 hours |
| **Curated** | A project's resources (`.ragnart/resources/`, and items linked into the project) and its collateral (`.ragnart/collateral/`) | An operator add, a keep or new collateral: `add --wait` for that item, or the next ingestion run |

Archive curation is blunt: a file that exists is indexed; a file that is removed has its
chunks removed. Nothing else decides.

### 7.3 Project workflow

**Status.** Not yet built; v0.3.

Project work is RAGNART's reason to exist. It follows the dynamics of working with git:
`ragnart init` once, then ordinary sessions in which the operator's requests are understood as
actions on the project.

**A project is a marker on disk.** `ragnart init` creates `.ragnart/` (§4.5). A session is in a
project because a `.ragnart/` directory is at or above its working directory, as a `.git` directory
makes a repository. There is no mode to enter or leave. Removing `.ragnart/` removes the project's
identity, inbox, resources and collateral, and the next sync removes their chunks from the index.

```
<project>/.ragnart/
  project.toml     identity and settings (§4.5)
  inbox/           Transient tier: captured material, not indexed (§7.5)
  resources/       kept and operator-added material, and notes; indexed
  collateral/      documents produced from the resources; indexed as derived material
  dropped/         items set aside by a drop, until the grace period ends (§7.5)
```

**Four activities, no workflow states.** They interleave in any session and are not entered or
left. The only session state is scope and grounding (§8).

| Activity | What it is | Scope and grounding |
|---|---|---|
| Gather | A discussion in which material is found and captured | Grounding `open` (§8.1) |
| Work | Ordinary session work on the material | Any |
| Distill | A request that produces collateral | `project`, `strict` |
| Ask | A question answered from the project | `project`, `strict`; widened only by the operator |

**Actions.** The operator speaks plain language. The agent, following the `/ragnart` skill (§7.4),
translates a request into a `ragnart` command. The `ragnart` CLI is plumbing: the operator does
not need to type it.

| The operator says | Action | Command | Decided by |
|---|---|---|---|
| Nothing: the agent fetches a page while capture is on | The page is captured into the inbox | `ragnart capture <url>` | The skill's policy |
| "add" a file or a URL | It becomes a resource directly, without passing through the inbox | `ragnart add --wait` | The operator |
| "keep" an inbox item | It moves to `resources/` and is indexed | `ragnart add --wait` | The operator |
| "drop" an item | It is set aside, and deleted after the grace period (§7.5) | `ragnart drop` | The operator |
| "note" a remark | The remark is saved as a Markdown file in `resources/` | `ragnart note` | The operator |
| A request for a summary, a presentation, a chart | Collateral is written to `collateral/` and registered | `ragnart add --wait` | The operator |
| A question | A search at the current scope (§8) | The search API | The agent; read-only |

- **The command prints the action line; the model does not compose it.** For example:
  `[ragnart · keep · 2 items → resources]`.
- **A request that starts with an action verb is taken literally.** Any other wording is
  interpreted, and the agent states its translation before acting, as it does for scope (§8.4).
- **Every change to the index is an operator request.** Capture writes only to the inbox, which is
  not indexed.
- **Capture is best-effort.** It depends on the skill being followed. A page the agent fetched and
  did not capture is still named in the session record.

**Capture.** `ragnart capture <url>` fetches the page itself, so the inbox holds the source and not
the agent's rendering of it.

- Captured: pages, and PDF files behind a URL. Search-result lists are not captured.
- A page is stored as Markdown with front matter: title, URL and fetch time.
- A second capture of the same URL replaces the item and restarts its expiry.
- A failed fetch is reported; it is never silent.

**Operator add.** Material the operator adds is a deliberate act and skips the inbox.

| Added | Result |
|---|---|
| A file outside the NFS roots | Copied into `resources/` |
| A file already under an NFS root | Linked into the project (`source_links`, §4.5); not copied |
| A URL | Captured into `resources/` |

**Collateral.** Distill produces a document on demand from the project's resources: an executive
summary, a presentation, a chart. The agent writes it at scope `project` and grounding `strict`.

- Its front matter lists the resources it was made from.
- It is indexed with `source_type = collateral`, so a result shows it as derived material and not
  as a source. Its weight in results is a `type_weights` setting (§9.2).
- An inbox item that it lists is kept.

**The ask loop.** A question is asked at project scope. When the answer is "no good hits in this
scope" (§8.3), the operator widens the scope or opens the grounding. What is found there can be
added to the project: an Archive item as a link, a web page as a capture. The same question is then
answered at project scope.

Relations between resources are not recorded. Search relates them when a question is asked.

### 7.4 Project context

**Status.** Not yet built; v0.3.

Working in a project gives the same experience as a Claude Code session in a git repository: the
harness knows where it is, and nothing has to be re-seeded.

| Layer | Content | Use |
|---|---|---|
| **Resources** | `.ragnart/resources/` and the items linked into the project | Searched (§8) |
| **Collateral** | `.ragnart/collateral/` | Searched; shown as derived material |
| **Project sessions** | Claude Code and OpenCode sessions started in the project | Searched; weight set by `type_weights` (§9.2) |
| **Working set** | The Transient tier: inbox and attached folders | Read directly by the agent |

**The catalogue is a listing, not a file.** No index file is kept in the project: it would duplicate
the state database and the inbox folder, and could disagree with them.

- `ragnart ls-files`, scoped to the project, lists its resources and collateral.
- `ragnart ls-files --others` lists its inbox, each item with its origin and expiry date.
- The resource listing reads the state database, which only Server 2 opens (§10.3). From another
  host it is served by the item-listing endpoint (§12). The inbox listing reads the folder.

**How a session learns it is in a project.** Three parts, each mirroring how the harness knows git:

| Part | Carries | Counterpart for git |
|---|---|---|
| The `/ragnart` skill, installed once for all projects | The action vocabulary (§7.3), the librarian rules (§8) and the capture policy. It is the same for every project and is refined over time. | The harness's built-in git instructions |
| The session-start hook | Finds the nearest `.ragnart/` above the working directory and injects the project name, scope, grounding and a capped listing: counts and the most recent items. Outside a project it is silent. | "This is a git repository", and the status snapshot |
| The status line | Project, scope, grounding and inbox count, at all times | The branch name |

In Claude Code the hook runs on new, resumed and compacted sessions. The OpenCode equivalent is open
(§13.3).

- **Context is implicit by location** (the nearest `.ragnart/` above the working directory)
  and **explicit by path** from anywhere: "structure the docs the way
  `~/Gin-AI/projects/AYA` does" loads AYA's listing and searches AYA's resources.
- **Context is alive.** Each session adds to it.
- In a code repository the project's own `CLAUDE.md`, docs and git history are part of the folder;
  RAGNART's files live under `.ragnart/` and it never modifies tracked files.

**Project resources are not the harness's memory files.** Claude Code keeps its own memory files per
working directory. RAGNART neither writes nor replaces them.

| | Claude Code memory files | Project resources |
|---|---|---|
| About | How to work in the project, and its state | What the material says |
| Written by | The agent, on its own judgement | The operator's adds and keeps |
| Reachable from | Claude Code, for that working directory | Any client; indexed and cited |

**Ingestion mode per project** (`mode` in `project.toml`, set by the `init` invariant of §4.5):

| Mode | Indexed | Set by `init` for |
|---|---|---|
| `curated` | Only the project's resources and collateral; everything else in the folder is excluded from the static crawl | A folder that holds no indexed content |
| `crawl` | The whole folder, as the static policy, with the project's resources and collateral | A folder that holds indexed content |

In a curated project, captured material is noise until it is kept, and the session in which it was
used is already archived and resumable in the Runtime tier.

Folders that must be searchable by meaning before curation are attached as resources (which indexes
them). Kotaemon uploads are operator adds: they enter directly as resources, followed by
`add --wait`.

### 7.5 Transient lifecycle

**Status.** Not yet built; v0.3.

The inbox is a cache of files. It is not vector-indexed, so capture, drop and expiry write nothing
to Qdrant. The only write to the index is a keep.

| Event | By the operator | Automatic |
|---|---|---|
| Enter | A file placed in `inbox/` | Capture (§7.3) |
| Persist | "keep" | A piece of collateral lists the item as a source |
| Leave | "drop" | Expiry, 30 days after capture (`inbox_expiry_days`) |

- **Expiry is announced.** The inbox listing (§7.4) shows each item's expiry date.
- **A drop is undoable.** A dropped item moves to `.ragnart/dropped/` and is deleted after the grace
  period of §10.2. Until then a keep restores it.
- **Dropping a resource** sets its file aside in the same way; its chunks leave the index through
  the sync, under the deletion rules of §10.2. Dropping a linked item removes the link row and
  leaves the file where it is.
- **`ragnart gc` deletes** expired and dropped items. It runs in the ingestion run.
- **RAGNART deletes files only under a project's `.ragnart/`**, never elsewhere on the NAS (§1.2,
  deviation B).
- **The inbox stays small.** Reading files directly works for a bounded working set, and expiry
  keeps it bounded. A larger body of material is kept, which indexes it.

## 8. Retrieval: scope and grounding

**Status.** Not yet built. Today `GET /v1/search` takes `q`, `user`, `top_k`, `file_types`,
`score_threshold` and `rerank`, filters by owner only, and has no `scope`. The client side is
the `/rag` command (`on`, `only`, `off`, `search`). The `scope` parameter and project listing
arrive in v0.2; the `/ragnart` skill and commands in v0.3.

### 8.1 Two orthogonal dimensions

Every retrieval-backed answer is governed by two independent settings. Either can change
without the other.

**Scope — which knowledge is searched**

| Value | Searches |
|---|---|
| `project` | The current project: its resources, collateral and sessions, its sub-projects, and the projects it links to. Inbox files are not searched: the agent reads them directly (§7.4) |
| `<selection>` | Projects named explicitly: paths (`~/Gin-AI/projects/AYA`), subtrees (`~/Customers/`), tag filters (`kind=customer`, `segment=ISV`), in any combination |
| `all` | Everything the principal is granted: the static crawl, all sessions, all accessible projects |
| `none` | No retrieval |

**Grounding — what an answer may rest on**

| Value | Meaning |
|---|---|
| `strict` | Only the material at the current scope: retrieved passages, and the inbox files of the current project, cited as inbox. Every statement cites its source. When the evidence is not there, the answer says so and stops. |
| `open` | Retrieved material first, cited; beyond it the model's own knowledge and, as its outermost reach, the web. A statement is one of three kinds: cited from RAGNART; cited from a fetched page, which is captured (§7.3); or uncited, from the model. |

`strict` governs every informational answer in the session, not only those for which the agent
chose to search. Under `strict`, a statement of fact without a citation is a fault. Actions, such
as editing a file or running a command, are not answers.

### 8.2 Defaults

Smallest first: the librarian is always on duty.

| Where the session starts | Scope | Grounding |
|---|---|---|
| Inside a project | `project` | `strict` |
| Outside any project | `all` | `strict` |

The defaults are tunable at three levels, each overriding the one before it, as the knobs of §9.2
are: `ragnart.yaml`; the project's `project.toml` (`scope`, `grounding`), where `init` writes
`grounding` by its invariant (§4.5); and the session or the single request (§8.4).

### 8.3 Visibility

Every answer that used retrieval starts with a status line:

```
[ragnart · scope project ~/Gin-AI/projects/RAGNART · grounding strict]
```

When the scope yields too few good hits, the answer ends there:

```
[ragnart · scope project ~/Gin-AI/projects/RAGNART · grounding strict]
No good hits in this scope. You may widen it (/ragnart scope …) or open the grounding
(/ragnart grounding open).
```

The response does not suggest where else to look.

Between answers, the harness status line shows the project, scope, grounding and inbox count at all
times (§7.4).

### 8.4 Changing scope and grounding

Changes are made **only by the operator**, and always shown.

| How | Effect |
|---|---|
| `/ragnart scope project \| <selection> \| all \| none` | Sets the scope for the rest of the session |
| `/ragnart grounding strict \| open` | Sets the grounding for the rest of the session |
| `/ragnart ask --scope <…> [--grounding <…>] "<question>"` | One answer at that scope; the session settings are unchanged |
| `/ragnart status` | Shows the current scope and grounding |
| Natural language that names a scope | The agent may interpret it, and states its translation before the results, e.g. *"Any customer in the ISV segment" → searching `kind=customer, segment=ISV` across all accessible projects* |

The agent never widens or narrows the scope, or opens the grounding, on its own initiative.
Narrowing works the same way as widening (`/ragnart scope project` returns to the current
project).

Capturing a page into the inbox (§7.3) changes neither the scope nor the grounding.

### 8.5 Examples

| Situation | Status line and behaviour |
|---|---|
| In `~/Gin-AI/projects/RAGNART`: "what is our snapshot retention?" | `scope project … · grounding strict`; answer quotes `docs/RAG-strategy.md` with citations |
| In RAGNART: "structure the docs the way AYA does" | Agent states "scope: `~/Gin-AI/projects/AYA`"; loads AYA's listing, searches AYA |
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

Each hit carries its key and display name, project (path and name), `source_type`,
`file_type`, `modified_at`, the passage (`parent_text`), and its scores. Results are
**collapsed** by `content_hash` (one hit listing every location of identical copies) and can be
**grouped** by project.

### 8.8 The API

- `scope` is **required** on every search; there is no server-side default.
- `scope none` is enforced entirely client-side (not yet built). It causes no retrieval call at
  all, so the API never receives `scope=none`.
- The response **echoes the resolved scope** (the projects actually searched) and every
  setting that applied (§9.2).
- The API returns evidence only. Strict grounding is applied by the client: the `/ragnart`
  skill and commands for Claude Code and OpenCode, the prompt configuration in Kotaemon. The
  skill is where the librarian behaviour is defined.

### 8.9 The `/rag` command

`/rag` remains as an alias of `/ragnart`: `/rag on` → grounding `open`; `/rag only` →
grounding `strict`; `/rag off` → scope `none`; `/rag search <query>` → a one-off `ask`. New
features exist only in `/ragnart`.

## 9. Retrieval quality

**Status.** Today search has `rerank`, the fetch sizes, and `max_hits_per_source`, all in
`ragnart.yaml` (§9.2). Collapse by `content_hash` is not yet built; the other knobs, the labelled
set and `ragnart calibrate` are not yet built either (§9.3).

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
  The shaping order is fixed: rerank, then collapse of identical copies by `content_hash` (not yet
  built), then the `max_hits_per_source` cap, then the `top_k` cut.
- **Pollution is visible.** A corpus health report lists sources that appear in the results of
  many unrelated queries, items with abnormal chunk counts, and low-quality text.

These properties hold for search. They do not hold for inbox files, which the agent reads directly
and which are not ranked (§7.5).

### 9.2 Knobs

| Knob | Controls |
|---|---|
| `min_relevance` | Rerank score below which a hit is not a "good hit". It decides the "no good hits" stop (§8.3), which is what leads the operator to widen the scope (§7.3) |
| `max_hits_per_source` | Per-source cap in one result list (default 3, `ragnart.yaml` `rag.search.max_hits_per_source`) |
| `type_weights` | Multipliers or exclusions by `file_type` / `source_type`, including `collateral` and the session types |
| `dense_fetch`, `sparse_fetch`, `fetch_cap` | Candidates brought to the reranker |
| `rerank` | Reranking on or off |

They are set at three levels, each overriding the previous: `ragnart.yaml` (global),
`project.toml` (per project), the request (echoed in the response).

**The relevance signal is the rerank score.** In hybrid search the `score` field is an RRF
rank-fusion value and does not measure relevance.

### 9.3 Calibration

- A **labelled set** of queries with the sources they should return, grown by
  `/ragnart expect "<query>" <path>` and by agent-proposed entries the operator confirms.
- `ragnart calibrate` runs the set over ranges of knob values and reports the hit rate in the
  top 5, the rank of the first correct source, and pollution indicators; it suggests settings.
- The operator applies settings explicitly; there is no automatic tuning.
- The same set is the gate for the next pipeline version (`p2`): it may not lower the top-5 hit rate
  beyond an agreed tolerance.

## 10. Failure and consistency

### 10.1 Authority

| Store | Role | Recovery |
|---|---|---|
| Files and session stores on the NAS | The knowledge itself | Synology snapshots (hourly) and offsite copy |
| `rag_state.db` | Bookkeeping | Snapshot pair, or a NAS snapshot plus reconciliation |
| Qdrant | Derived index | Daily snapshot pair; full re-embedding as last resort |

NAS backups are for emergencies; normal operation never relies on them.

### 10.2 Deletion rules

**Status.** Rules 1, 3 and 5 are built. Rule 2 (tombstone and grace period) is v0.2. Rule 4 (purge
record) is not yet scheduled (§13.2).

Deletion is the dangerous operation. The sync applies these rules:

1. Nothing is deleted because it was **not seen**, unless the scan that missed it is complete. A
   root is incomplete when it is not a directory, or when it yields zero files while the queue still
   holds rows under it, because an unmounted mount point looks the same. If any root is incomplete,
   the whole deletion phase is skipped: no rows and no Qdrant points are deleted, the sync logs an
   error and exits with code 1, and the queueing done by the scan stands. The check covers only the
   owners the sync scans: with `--user`, only that owner's rows. An OpenCode session row is kept while
   the OpenCode API cannot be read. An unreadable `.ragignore` stops the sync before this point
   (§1.2, deviation G).
2. A vanished item is to be **tombstoned** (`missing_since`), and its chunks purged after a grace
   period. In the current sync, its chunks are deleted in the run that finds it missing. Tombstone and
   grace period are v0.2 (§13.2).
3. Deletion is limited to `rag.sync.max_delete_fraction` (0.10 in `ragnart.yaml`) of the **completed**
   rows. When more completed rows would be deleted than that fraction of all completed rows, the
   deletion phase is skipped, the sync logs an error and exits with code 1. `--allow-mass-delete`
   runs the deletion for that run.
4. Every purge is to be recorded with its key, chunk count, reason and time. The sync log names each
   deleted path and its reason. The chunk count and a purge record are not kept yet (§13.2).
5. Qdrant deletes wait for Qdrant to confirm them (`wait=true`), and a queue row is removed only after
   its points are gone. If a delete fails, the sync stops, the row survives, and the next sync retries.

### 10.3 Operating rules

**Status.** The single-writer lock covering every writer of the index, and idempotent ingestion, are
built. Reconciliation is part of v0.2; notifications are not yet scheduled.

- **Single writer.** Every process that writes `rag_state.db` rows or Qdrant points holds the
  ingestion lock (`RAGNART--databases/rag-ingest.lock`, `rag.ingest_lock`, required in `ragnart.yaml`)
  while it writes: `utils/rag_sync_nfs.py`, `utils/rag_ingest_daemon.py`,
  `utils/rag_purge_rescan.py --apply`, `utils/rag_purge_corrupt.py --confirm`, `utils/rag_reset.py`
  without `--dry-run`, and `scripts/sqlite-qdrant-snapshot.sh`. `scripts/ragnart-ingest-run.sh` holds
  the lock for its whole run (sync, daemon, WAL checkpoint, snapshot), and the steps inside the run
  run under that hold. The one exception is `utils/rag_sparse_migrate.py`, a one-off migration that is
  run with the timer stopped. The lock is fail-fast: a second writer exits with status 75 at once and
  prints the holder (pid, host, command line, start time), which is also written into the lock file.
  It never waits. The planned v0.3 verbs `add`, `add --wait`, `rm` and `gc` will take the same lock.
  `rag_state.db` is opened from Server 2 only.
- **Idempotency.** Every operation (ingest a file, sync, backfill, `add --wait`, `gc`) can be
  re-run safely after a crash.
- **Failed sync.** A sync that refuses deletion or crashes does not stop the ingestion run. The
  daemon, the WAL checkpoint and the snapshot step still run. The run then exits with the sync's exit
  code, so `ragnart-ingest.service` is marked failed.

- **Plan-then-apply (not yet built).** Each sync is to compute its moves, tombstones, purges and new
  items as a plan, log it (also printed with `--dry-run`), then apply it.
- **Reconciliation.** A scheduled check compares the two stores: each completed item's
  `chunk_count` against its points, and every key in Qdrant against a row. It **repairs**
  automatically (re-queues missing items, deletes orphaned points under the deletion rules)
  and **fails loudly** in the logs on what it cannot repair. It is also the standard step after
  any partial restore.
- **Notifications** (failed run, guard tripped, snapshot failed, reconciliation problems) are
  part of the design; the channel and implementation are not defined (§13).

### 10.4 Snapshot policy

- One restore pair per day (Qdrant snapshot + matching copy of `rag_state.db`), taken by the
  ingestion run when the newest pair is older than 20 hours (normally the 01:00 run);
  `RAG_FORCE_SNAPSHOT=1` forces one.
- Seven pairs kept on the NAS (`RAGNART--databases/qdrant-snapshots/documents/`).
- Qdrant writes the snapshot to its local `snapshots_path` and the script downloads it to the
  NAS and deletes the local copy; stale local snapshots and partial downloads are removed
  before each run; the run aborts below 60 GB free on the NAS.
- Restore procedures: `docs/RAG-strategy.md` §9.3 (full) and §9.4 (state database only).

### 10.5 OpenCode database

**Status.** Decided; built in v0.2.

`opencode.db` stays on Server 2's local disk. A nightly online copy (`sqlite3 .backup`) is
written to `RAGNART--databases/opencode-backups/<hostname>-<ip>/opencode-<YYYY-MM-DD>.db`,
with the last 7 copies kept, by a timer installed with RAGNART's units.

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
| HTTP API (Server 2, port 8100) | `GET /v1/search`: hybrid search and reranking (§5.5); answers 503 when Qdrant is unreachable or fails. `GET /health`: `ok` when Qdrant's `/healthz` answers within 2 seconds, otherwise `degraded`. Planned: project listing (`GET /v1/projects` returns, per project, `id`, `path`, `name`, `parent`, `owner`, `mode`, `tags` and item count, filtered by `user` the same way search is) and resolution, item listing for metadata queries and for the project catalogue (§7.4), grant management |
| `ragnart` CLI | Read-only commands built (§1.2): `check-ignore`, `ls-files`, `status`. Planned (v0.3): `init`, `capture`, `add`, `add --wait`, `drop`, `note`, `rm`, `mv`, `diff`, `fsck`, `gc`, `where`, `expect`, `calibrate`, `grant`, `revoke`, `transfer`. In project work the CLI is plumbing, called by the agent (§7.3). Until then, the operations tools are the separate `utils/rag_*.py` scripts. |
| `/ragnart` skill and commands (Claude Code, OpenCode) | Planned (v0.3); today the `/rag` command. Scope and grounding (§8), librarian behaviour, the action vocabulary and the capture policy (§7.3) |
| Session-start hook and status line (Claude Code, OpenCode) | Planned (v0.3). Project detection, and the project's state in every session (§7.4) |
| Kotaemon | Browser UI: project picker as scope, uploads as resources, answers by its own model through the SoHoAI gateway |

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

- **Tombstone and grace period** (§10.2, rule 2). Not built; v0.2.
- **Purge record** (§10.2, rule 4). The sync log names each deleted path and its reason. The chunk
  count and a purge record are not kept.
- **Reconciliation** (§10.3). Not built; v0.2.

### 13.3 Undefined parts of the design

- **Project actions from hosts other than Server 2.** An add or a keep changes the state database,
  which only Server 2 opens, and the API is read-only. The write path is not defined.
- **Session-start hook in OpenCode.** The Claude Code hook is defined (§7.4); its OpenCode
  equivalent is not verified.
- **Where a session's scope and grounding are stored**, so that the status line can read them.
- **Origin of a capture that is not Markdown.** A captured page carries its URL in its front
  matter; where a captured PDF in the inbox records its URL is not defined.
- **Video transcripts.** Capture has no transcript fetcher.
- **Interfaces**: API versioning, CLI / skill / API division, Kotaemon fit, including Kotaemon as
  the view of a project's resources and inbox.
- **Sharing and ownership transfer**: mechanism in §6.4, not built.
- **Authentication**: OAuth with group claims, when a second user exists.
- **Notifications**: channel and events.
- **Qdrant placement**: after the corpus cleanup (§11).

### 13.4 Archive limits

- Grace period before purging a vanished file's chunks: value to be set.
- The Archive keeps the current version of each item only; no document history.
- Symlinks: the real path is the key; an alias in another project is a link row; details to be
  specified.
- An active Claude Code session is re-embedded in full on every change while it grows.
- Secrets in sessions and files are embedded and returned; there is no secret filter.
- The static policy runs every 6 hours; curated projects use `add --wait`.
- Inputs longer than 2048 bge-m3 tokens are rejected by Ollama, not truncated (§5.6).

### 13.5 Smaller items

- SoHoAI's `/v1/rag/search` redirect stub is removed on or after 2026-11-16, after checking the
  gateway log for remaining callers.
- The Claude Code `rag` command still describes itself as "Search local SoHoAI RAG".
- The name RAGNART spells Archive, Runtime, Transient: the three lifecycle tiers of §7.1.

## 14. Decision register

| ID | Decision | Rationale | Consequence |
|---|---|---|---|
| D1 | Item identity = natural key per type; content hash (MD5) as attribute; no surrogate id | Path is unique and is the locator; hash collides and changes on edit; surrogate adds nothing inside RAGNART | Moves rewrite the key everywhere in one step; sessions keyed by id survive file moves |
| D1b | Schema version (data model) and pipeline version (`p1`, per item), documented explicitly | Separate "shape of the data" from "how content was processed" | Explicit migrations; selective re-embedding |
| D1c | Payload carries filterable metadata; identical copies collapse; metadata and time filters; metadata-only queries from the state DB | Operator needs "PDF modified last month", "OpenCode sessions last week"; duplicates are noise | Indexes on type, time, hash; listing endpoint |
| D2 | Everything is a project; grants to persons and groups; inheritance with opt-out; grants in the DB; roles `read`, `admin`; owner = metadata | One access concept instead of three; filesystem permissions unusable; NAS cannot hold access rules | `project_id` on every chunk; auth added later without backfill |
| D2b | Projects addressed by path; `name` required; immutable `id` in `project.toml`; free tags, DB-only | KISS; moves must not touch Qdrant; tags change often | Tag edits are row updates |
| D3 | Tiers by lifecycle: Transient (ephemeral cache, not indexed), Archive, Runtime; one store, one pipeline, two policies; `curated` default | Captured material is noise until kept; sessions already archive the work | No transient collection; a keep places a file and indexes it; `add --wait` |
| D3b | Project context = an on-demand listing of resources, collateral and inbox, with no index file; context implicit by location, explicit by path | No re-seeding. A file would duplicate the state database and the inbox folder, and could disagree with them | The session-start hook injects the listing |
| D4 | Scope × grounding, orthogonal; defaults `project+strict` / `all+strict`; changes only by the operator, always shown; natural-language scope translated aloud | Librarian answers; no silent widening | Status line; `/ragnart` commands; required `scope` in the API |
| D4d | Default scope and grounding are tunable at three levels. `strict` governs every informational answer. `open` reaches the model's knowledge and, outermost, the web | A code repository and a knowledge project need different defaults; an answer given without a search must not escape `strict` | `scope` and `grounding` in `project.toml`; three kinds of statement under `open` (§8.1) |
| D4b | Retrieval-only | Librarian answers; clients already have models | No model gateway; client enforces strict grounding |
| D4c | Predictable, not rigid; knobs at three levels; calibration with a labelled set; no auto-tuning | Corpus pollution is a permanent hazard; tuning needs primitives | `max_hits_per_source`, `type_weights`, `min_relevance`, `ragnart calibrate` |
| D5 | NAS is the authority; reconciliation repairs and fails loudly; deletion rules; single writer | Mirror SoHoAI's robustness; NAS backups cover disasters | Guards in the sync; scheduled reconciliation |
| D5b | `opencode.db` local, nightly copy to the NAS per server | SQLite in WAL mode is unsafe on NFS | Backup timer installed with RAGNART |
| D5c | Daily snapshot pair, 7 kept; local transient `snapshots_path` | Balance restore points against NAS space; avoid double NAS writes | NAS snapshot area about 150 GB |
| D6 | Mirrored trees (`AWS--fotel`, `Gitlab--fotel`) are both indexed. The duplicated `_AWS` subtree is absent from the NAS | The remaining contents of the two trees differ | No deduplication rule in the scanner. A file reached through several links is queued once (§1.2, deviation C) |
| D7 | RAGNART mirrors git in ingestion, exclusion and lifecycle. It uses git's command names where the mapping is exact, and its own terms elsewhere (§1.2). Deviations A to G are recorded in §1.2 with their reasons | Git already answers the questions this system asks: what is on disk, what is excluded and why, what would change, what is indexed. Its names are widely known, so one vocabulary serves the CLI and the docs | CLI verbs follow §12. `.ragignore` follows gitignore syntax and precedence. Each deviation carries its reason |
| D8 | A pipeline version changes only on a major event: incompatibility with stored items, or a significant change of behaviour. Bug fixes and improvements stay in the current version (§4.1) | A version is a signal to the operator and to the calibration gate (§9.3). A new version per fix would make the signal meaningless and send every fix through the gate | Items affected by a fix are re-checked or re-ingested, with the version unchanged. A real new version re-embeds only the items still on the old version |
| D9 | Project work follows the dynamics of git. `.ragnart/` is the project; a global `/ragnart` skill, a session-start hook and the status line make the harness aware of it; the `ragnart` CLI is plumbing behind plain-language requests. Capture is ambient and goes to an unindexed inbox that expires after 30 days; a keep or an operator add is the only way into the index; distill produces collateral, indexed as derived material (§7.3 to §7.5) | Gathering happens inside a conversation, so a command per item would interrupt it. Transient material must not cause writes or deletes in the shared index | Deviation H (§1.2). No workflow states beyond scope and grounding. `capture`, `drop` and `note` in the CLI; no `distill` or `inbox` command; no `MEMORY.md` |
| D10 | The `init` invariant: `ragnart init` never changes what is indexed. A folder with indexed content gets `crawl`, `open` and capture off; a folder without gets `curated`, `strict` and capture on (§4.5) | A `curated` default would remove an existing folder's content from the index at `init` | `init` reads the index state of the folder, and prints the settings it writes |
