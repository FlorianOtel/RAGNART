---
title: "RAGNART — Design"
created_at: 2026-11-13--23-55
created_by: Claude Code (Claude Opus 5.5)
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
automatically, built up by gathering material and distilling it into references.

**Principles.** Every design decision below follows from these.

| Principle | Meaning |
|---|---|
| **Librarian answers** | Answers are facts, references and quotes from the material at hand. Nothing is extrapolated or guessed unless the operator explicitly opens the grounding (§8). |
| **Retrieval-only** | RAGNART returns evidence; it never writes answers and contains no model gateway. Clients (Claude Code, OpenCode, Kotaemon) compose answers. |
| **The NAS is the authority** | Files and session stores on the NAS are the truth. The state database is bookkeeping; the vector index is derived. |
| **Explicit over implicit** | The current scope and grounding are always visible; every widening or narrowing is a deliberate act of the operator, loud and visible, never silent. |
| **KISS** | Use what exists (paths, folders, files, one pipeline, one store) before adding mechanisms. Add primitives where tuning is needed, not cleverness. |
| **Predictable, not rigid** | Same input on the same data gives the same result; behaviour changes only when the data or the settings change, and the settings are knobs the operator controls. |
| **Current-state documentation** | Documents describe how things are; history is in git and in the implementation logs. |

### 1.1 Status of this design

This document describes the **target design**. Parts of it run today; the rest is built in
the versions of `docs/Implementation-plan.md` §6. Sections whose content is not yet built carry
a **Status** line. Summary:

| Area | Status |
|---|---|
| Hosts, services, data root, snapshot policy (§2, §10.4) | Built |
| Pipeline `p1` (§5) | Built (it is what produced the current index) |
| Item identity by path for NAS files, `opencode://` keys (§4.2) | Built; `claude-session://` keys, content hash and move detection: v0.2 |
| State-database metadata and payload fields beyond today's (§4.3, §4.4), schema version (§4.1), per-item pipeline version | v0.2 |
| Projects, `project.toml`, tags, grants, `resolve_scope()` / `authorize()` (§4.5, §6) | v0.2 (data model); authentication and grant management: v0.5 |
| Scope and grounding in the API and the `/ragnart` skill and commands (§8) | API scope and project listing: v0.2; skill, commands and `ragnart` CLI: v0.3 |
| Project mode, compact, `MEMORY.md`, ingest-now (§7.3, §7.4) | v0.3, after the Transient lifecycle is defined (§13.3) |
| Retrieval-quality knobs and calibration (§9) | Not yet scheduled |
| Deletion rules, reconciliation (§10.2, §10.3) | Partly v0.2 (fix-first guards, tombstone and grace); reconciliation not yet scheduled |
| OpenCode database backup (§10.5) | Not yet scheduled |
| Kotaemon (§12) | v0.4 |

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
| **Ingestion policy** | Per kind of material | **Static**: global include/exclude rules, unattended, every 6 hours. **Curated**: only what a project deliberately keeps (§7) |
| **Curation (lifecycle)** | Per tier | Archive: blunt, mirrors the filesystem. Runtime: session records. Transient: material in flight (§7) |

## 4. Data model

### 4.1 Versions

Two version numbers govern the data. They are independent.

- **Schema version — the data model.** One value for the whole system, stored in the state
  database. It names the set of tables and columns and the Qdrant payload layout. Each change
  to the data model is a numbered migration that runs once and records the new version.
- **Pipeline version — how an item's content became chunks and vectors.** Stored **per item**.
  The current pipeline is **`p1`**, defined exactly in §5. Any change that alters stored chunks
  or vectors defines a new version (`p2`, …). Because the version is per item, the collection
  can hold a mix, and a change re-embeds only the items still on the old version, in batches,
  instead of the whole corpus.

### 4.2 Identity of an item

An **item** is one ingestable thing: a file, a session, a captured page or an upload. It is
split into many chunks (Qdrant points).

**The identity of an item is its natural key**, chosen per type:

| Type | Key |
|---|---|
| NAS file | Full path, e.g. `/mnt/nfs/Florian/…/report.pdf` |
| Claude Code session | `claude-session://<session uuid>` |
| OpenCode session | `opencode://<session id>` |
| Captured page, upload | Its path in the project's inbox; the URL is an attribute |

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
| `source_type` | `file`, `claude_session`, `opencode_session`, `web_capture`, `upload` |
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
mode = "curated"                   # optional: curated (default) | crawl (§7.4)
kind = "customer"                  # optional tags: any key = value
segment = "ISV"
```

- **Addressing.** A project is addressed **by its path** (`~/Gin-AI/projects/AYA`). The path
  is unambiguous and needs no name resolution.
- **`id` is the immutable, unique project identifier.** It lives inside the folder, so it
  travels with any move or rename: moving a project changes one row in the state database and
  nothing in Qdrant, although every chunk carries `project_id`.
- **Parent** is the nearest ancestor directory that is also a project. Each NFS root is a
  top-level project; unmarked directories belong to the enclosing project.
- **Tags** are arbitrary `key = value` pairs (every key except the reserved `name`, `id`,
  `inherit_access`, `mode`). They are stored as rows `(project_id, key, value)`. On each sync
  (and on the `ragnart` command) the tag set read from `project.toml` replaces the stored set:
  changing `industry = "ISV"` to `segment = "ISV"` deletes one row and adds one, for the same
  `project_id`. A tag filter is resolved to a set of project ids in the database before the
  search, so tag changes never touch Qdrant.

Tables: `projects` (id, parent, path, name, owner, inherit_access, mode), `project_tags`,
`project_links` (a project referencing another), `source_links` (one item referenced from
another project; an alias symlink becomes such a link), `grants` (§6).

## 5. Pipeline `p1`

`p1` is the exact processing that produced every chunk now in the index. A change to any
element in §5.1–§5.4 is a new pipeline version.

### 5.1 Parsing

| Input | Parser |
|---|---|
| `pdf`, `docx`, `pptx` | docling 2.89.0, `DocumentConverter` with default options, exported to Markdown |
| `pptx` when docling fails | python-pptx 1.0.2 (after LibreOffice conversion for OLE2 files) |
| `pdf`, `docx` when docling fails | **raw read of the file bytes as UTF-8 with replacement** (a known defect, §13.1) |
| `ipynb` | RAGNART's notebook parser (cells as text) |
| Claude Code session (`.jsonl`) | RAGNART's parser: user and assistant text turns only; tool calls and tool results are dropped |
| OpenCode session | Read from the OpenCode server API: user and assistant text |
| All other included extensions | Read as UTF-8 text with replacement |

### 5.2 Quality gate

A parsed text is rejected (`status = ignored`) when fewer than **85%** of its characters
satisfy Python's `str.isprintable()`.

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
| Dense (unnamed) | **bge-m3** served by Ollama on Server 2 | Model digest `790764642607`, F16, 566.7 M parameters, 1024 dimensions, cosine distance. Ollama runs with `OLLAMA_CONTEXT_LENGTH=768`: input beyond 768 bge-m3 tokens is truncated. |
| Sparse `sparse_text` | **fastembed 0.8.0, `Qdrant/bm25`** | Qdrant IDF modifier |

### 5.5 Not part of `p1`

Search-time processing does not change stored data and is not part of the pipeline version:
hybrid fusion (Qdrant RRF), reranking, scope resolution, collapsing and grouping.

### 5.6 Known properties of `p1`

- Chunk sizes are counted in `cl100k_base` tokens, the embedder's limit in bge-m3 tokens.
  Children (250) stay below the limit; flat 512-token chunks of code, YAML or non-English text
  can exceed 768 bge-m3 tokens and are truncated before embedding.
- The raw-read fallback and the quality gate admit binary content (§13.1).

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
`authorize()`. No other code builds a scope filter. Today one principal exists and is granted
everything; authentication (OAuth with group claims) is added when a second user exists.
Multi-tenancy is designed now and built later: the data model (project ids on every chunk,
grants shaped for persons and groups) is in place from v0.2 so nothing has to be backfilled.

## 7. Tiers and lifecycle

### 7.1 Three tiers, distinguished by lifecycle

| Tier | Lifecycle | Where | Vector-indexed | Leaves the tier by |
|---|---|---|---|---|
| **Transient** | Material in flight: gathered, not yet judged | Files in a project's `.ragnart/inbox/`, or an attached folder | **No.** The agent reads the files directly. | Compact (becomes an Archive reference), drop, or expiry |
| **Archive** | Persistent | NFS roots (static crawl) and project references (curated) | Yes | The file is removed (after the grace period, §10.2) |
| **Runtime** | Persistent session record; a specific case of Archive | Claude Code and OpenCode sessions | Yes | — |

There is **one persistent store** (the `documents` collection, an alias of `documents_new`)
and **one pipeline**. The Transient tier is an ephemeral cache, not a store: material there is
used while work is in progress and becomes persistent only by being curated.

### 7.2 Two ingestion policies

| Policy | What enters | Trigger |
|---|---|---|
| **Static** | Every file under the NFS roots matching the global include/exclude rules in `ragnart.yaml`, plus every Claude Code and OpenCode session | `ragnart-ingest.timer`, every 6 hours |
| **Curated** | A project's references (`sources/`, `notes/`) and its `MEMORY.md` | Compact, then the next ingestion run, or "ingest now" for that project |

Archive curation is blunt: a file that exists is indexed; a file that is removed has its
chunks removed. Nothing else decides.

### 7.3 Project mode: gather → distill

Project mode is RAGNART's reason to exist. Work in a project has two phases:

| | Gather | Distill |
|---|---|---|
| Character | Wide, fast, no commitment | Narrow, deliberate |
| Input | Links, documents, video transcripts, notes, search results | Everything gathered |
| Lives in | The Transient tier | — |
| Output | — | The project's **reference set** and its **project memory** |

**Compact** is the step from gather to distill, the deterministic counterpart of a session
`/compact`. It is split in two:

- **Selection is deterministic.** Which items persist is decided by explicit rules (in
  `project.toml`) and explicit keep/drop decisions, recorded. The same inputs and rules give
  the same persisted set.
- **The summary is generated and contained.** The agent writes or updates the project memory;
  it is a file, edited in place, whose entries point to the references they come from.

Persisting means placing files: a kept item moves from `inbox/` to the project's references;
the memory file is written. The Archive mechanics then index them like any other file.

### 7.4 Project context

Working in a project gives the same experience as a Claude Code session in a repository with
its `CLAUDE.md`, docs and memory files: nothing has to be re-seeded, and the project's
materials are seen in relation to one another.

| Layer | Content | Use |
|---|---|---|
| **Project memory** — `.ragnart/MEMORY.md` | An **index**: one line per reference or decision, with a pointer and a short hook, e.g. `- [Kickoff meeting](sources/2026-10-02-kickoff.md) — scope agreed; see decision D4` | Loaded automatically when working in the project; itself indexed and citable |
| **Reference set** | The project's persisted references | Searched first (§8) |
| **Working set** | The Transient tier: inbox and attached folders | Read directly by the agent |

- **Context is implicit by location** (the nearest `.ragnart/` above the working directory)
  and **explicit by path** from anywhere: "structure the docs the way
  `~/Gin-AI/projects/AYA` does" loads AYA's memory and searches AYA's references.
- **Context is alive.** Each session adds to it; compact runs whenever work is consolidated.
- In a code repository the project's own `CLAUDE.md`, docs and git history are references;
  RAGNART's memory lives under `.ragnart/` and never modifies tracked files.

**Ingestion mode per project** (`mode` in `project.toml`):

| Mode | Indexed | Default for |
|---|---|---|
| `curated` | Only the project's references and `MEMORY.md`; everything else in the folder is excluded from the static crawl | Projects created by `ragnart init` |
| `crawl` | The whole folder, as the static policy | Projects that should mirror a folder in full |

Curated is the default because gathered material is noise until it is judged, and the session
in which it was used is already archived and resumable in the Runtime tier.

Folders that must be searchable by meaning before curation are attached as reference material
(which indexes them). Kotaemon uploads are deliberate acts and enter directly as references,
followed by "ingest now".

### 7.5 Transient lifecycle

The detailed lifecycle of the Transient tier is defined in its own design step (§13.3), with
the questions of a cache: admission, states and transitions, eviction, invalidation,
write-back (compact), actors and access through the interfaces.

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
| `project` | The current project: its `MEMORY.md`, references and sub-projects, plus projects it links to |
| `<selection>` | Projects named explicitly: paths (`~/Gin-AI/projects/AYA`), subtrees (`~/Customers/`), tag filters (`kind=customer`, `segment=ISV`), in any combination |
| `all` | Everything the principal is granted: the static crawl, all sessions, all accessible projects |
| `none` | No retrieval |

**Grounding — what an answer may rest on**

| Value | Meaning |
|---|---|
| `strict` | Only retrieved material. Every statement cites its source. When the evidence is not there, the answer says so and stops. |
| `open` | Retrieved material first, cited; beyond it the model's own knowledge. Cited statements come from RAGNART; uncited ones do not. |

### 8.2 Defaults

Smallest first: the librarian is always on duty.

| Where the session starts | Scope | Grounding |
|---|---|---|
| Inside a project | `project` | `strict` |
| Outside any project | `all` | `strict` |

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

### 8.5 Examples

| Situation | Status line and behaviour |
|---|---|
| In `~/Gin-AI/projects/RAGNART`: "what is our snapshot retention?" | `scope project … · grounding strict`; answer quotes `docs/RAG-strategy.md` with citations |
| In RAGNART: "structure the docs the way AYA does" | Agent states "scope: `~/Gin-AI/projects/AYA`"; loads AYA's `MEMORY.md`, searches AYA |
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
- **Pollution is visible.** A corpus health report lists sources that appear in the results of
  many unrelated queries, items with abnormal chunk counts, and low-quality text.

### 9.2 Knobs

| Knob | Controls |
|---|---|
| `min_relevance` | Rerank score below which a hit is not a "good hit" |
| `max_hits_per_source` | Per-source cap in one result list |
| `type_weights` | Multipliers or exclusions by `file_type` / `source_type` |
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
- The same set is the gate for a new pipeline version: a `p2` may not lower the top-5 hit rate
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

**Status.** Target rules. Today the sync deletes every queue row it did not see, without
waiting for Qdrant to confirm, with one guard (OpenCode rows are kept when the OpenCode API is
unreachable). Rules 1 and 3 are v0.2 fix-first item 1; rule 2 (tombstone and grace) is part of
v0.2; rules 4 and 5 are not yet scheduled (§13.2).

Deletion is the dangerous operation:

1. Nothing is deleted because it was **not seen**, unless the scan that missed it is proven
   complete: every root in scope accessible and non-empty, no scan errors, the source API
   reachable.
2. A vanished item is first **tombstoned** (`missing_since`); its chunks are purged after the
   grace period.
3. A sync that would delete more than `max_delete_fraction` of the rows in scope skips its
   deletion phase and fails loudly; an explicit override exists for genuine mass deletions.
4. Every purge is logged: key, chunk count, reason, time.
5. Qdrant deletes are confirmed before the state row is removed.

### 10.3 Operating rules

**Status.** Single writer (the lock) and idempotent ingestion are built. Reconciliation and
notifications are not yet built or scheduled.

- **Single writer.** One process at a time changes the index, holding the ingestion lock
  (`RAGNART--databases/rag-ingest.lock`). Ingest-now, compact, keep and drop take the lock or
  hand their work to the ingestion run. `rag_state.db` is opened from Server 2 only.
- **Idempotency.** Every operation (ingest a file, sync, backfill, compact, ingest-now) can be
  re-run safely after a crash.
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

**Status.** Decided, not yet built or scheduled.

`opencode.db` stays on Server 2's local disk. A nightly online copy (`sqlite3 .backup`) is
written to `RAGNART--databases/opencode-backups/<hostname>-<ip>/opencode-<YYYY-MM-DD>.db`,
with the last copies kept, by a timer installed with RAGNART's units.

## 11. Capacity and placement

| | Server 1 (.93) | Server 2 (.95) |
|---|---|---|
| RAM | 23 GB; Qdrant uses 17.2 GB | 27 GB; Ollama, llama-swap and the GPU models share it |
| Other duties | AYA Windows VM host (its passthrough boot entry reserves 8 GB of hugepages) | Ingestion, search, embedding, reranking |

Qdrant keeps vectors in RAM. A clean corpus (§13.1) is about 400,000 points instead of about
3 million, roughly 2–3 GB instead of 17 GB, which removes the memory pressure on Server 1. The
placement of Qdrant is decided after the cleanup, from measured memory use.

## 12. Interfaces

| Interface | Role |
|---|---|
| HTTP API (Server 2, port 8100) | `GET /v1/search` and `GET /health`; planned: project listing and resolution, item listing for metadata queries, grant management |
| `ragnart` CLI | Planned (v0.3); today the operations tools are separate `utils/rag_*.py` scripts. Operations and project work: `init`, `where`, `add`, `inbox`, `keep`, `drop`, `compact`, `ingest-now`, `expect`, `calibrate`, `grant`, `revoke`, `transfer` |
| `/ragnart` skill and commands (Claude Code, OpenCode) | Planned (v0.3); today the `/rag` command. Scope and grounding (§8), librarian behaviour, project context loading, compact |
| Kotaemon | Browser UI: project picker as scope, uploads as references, answers by its own model through the SoHoAI gateway |

API versioning, the division of work between CLI, skill and API, and Kotaemon's fit are open
(§13).

## 13. Open issues

### 13.1 Corpus pollution — pipeline fragility

Measured on the live index (14,712 items with points, 2,988,440 points):

| Category | Points | Share | Items |
|---|---|---|---|
| PDF/DOCX read as raw binary bytes (about 4,200 points per MB of file) | 1,243,155 | 41.6% | 211 |
| Very large `.txt` (Android R8 mapping files in `P4OC/mappings/`, nine versions of about 80 MB; VPN client logs; schema dumps) | 987,376 | 33.0% | 15 |
| Large CSV exports (data dumps, scoring sheets, course datasets) | 362,643 | 12.1% | 23 |
| The actual corpus | 395,253 | 13.2% | 14,462 |
| Item no longer on disk | 13 | 0.0% | 1 |

Causes, all in the ingestion code:

1. When docling fails on a PDF or DOCX, the parser reads the file's bytes as text.
2. The quality gate counts `str.isprintable()` characters, and the replacement character
   produced when decoding binary bytes counts as printable, so decoded binary passes. The
   purge utility `utils/rag_purge_corrupt.py` uses the same test.
3. There is no maximum file size and no maximum chunk count per item.
4. The gate runs only at ingestion; unchanged files keep their chunks indefinitely.
5. The static policy takes every `.txt` and `.csv`; mirrored folder trees (`AWS--fotel` and
   `Gitlab--fotel`) index the same files twice.

Remedy direction: a pipeline `p2` (no raw-read fallback for binary types, a binary check before
parsing, size and chunk caps that mark items `ignored` with a visible reason), a one-off purge
of the affected items after a snapshot pair, and a corrected purge utility, done before the
v0.2 metadata backfill. Open decisions: whether CSV and similar data files belong in the static
policy, the cap values, and whether mirrored trees are both indexed. The cleanup is being
handled in a separate working session.

### 13.2 Known defects in sync and tooling

Listed with code locations, fixes and tests in `docs/Implementation-plan.md` §6 v0.2
("Fix first"): an unreachable NFS root deletes its tree from the index; a `--user` sync
deletes other owners' rows; status and reset tools do not resolve the `documents` alias;
`/health` blocks and search returns 503 until restart when Qdrant was down. Also: sync deletes
Qdrant points without waiting for confirmation, which can leave orphaned points; there is no
tombstone or grace period, no `max_delete_fraction`, no purge log and no reconciliation yet
(§10.2, §10.3).


### 13.3 Undefined parts of the design

- **Transient lifecycle** ("cache dynamics"): admission, states and transitions, eviction,
  invalidation, compact rules, actors, interface-level access. To be defined before v0.3.
- **Interfaces**: API versioning, CLI / skill / API division, Kotaemon fit.
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
- The static policy runs every 6 hours; curated projects use ingest-now.
- `p1` truncates inputs longer than 768 bge-m3 tokens (§5.6).

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
| D3 | Tiers by lifecycle: Transient (ephemeral cache, not indexed), Archive, Runtime; one store, one pipeline, two policies; `curated` default | Gathered material is noise until judged; sessions already archive the work | No transient collection; compact places files; ingest-now |
| D3b | Project memory = indexed, citable `MEMORY.md` index; context implicit by location, explicit by path | The AYA-in-Claude-Code experience: no re-seeding | Skill loads the memory on entry |
| D4 | Scope × grounding, orthogonal; defaults `project+strict` / `all+strict`; changes only by the operator, always shown; natural-language scope translated aloud | Librarian answers; no silent widening | Status line; `/ragnart` commands; required `scope` in the API |
| D4b | Retrieval-only | Librarian answers; clients already have models | No model gateway; client enforces strict grounding |
| D4c | Predictable, not rigid; knobs at three levels; calibration with a labelled set; no auto-tuning | Corpus pollution is a permanent hazard; tuning needs primitives | `max_hits_per_source`, `type_weights`, `min_relevance`, `ragnart calibrate` |
| D5 | NAS is the authority; reconciliation repairs and fails loudly; deletion rules; single writer | Mirror SoHoAI's robustness; NAS backups cover disasters | Guards in the sync; scheduled reconciliation |
| D5b | `opencode.db` local, nightly copy to the NAS per server | SQLite in WAL mode is unsafe on NFS | Backup timer installed with RAGNART |
| D5c | Daily snapshot pair, 7 kept; local transient `snapshots_path` | Balance restore points against NAS space; avoid double NAS writes | NAS snapshot area about 150 GB |
