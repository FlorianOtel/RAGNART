---
title: "RAGNART — RAG strategy"
created_at: 2026-03-30--00-00
created_by: Florian Otel
updated_by: Claude Code (Claude Opus 5); Claude Code (Claude Opus 5.5, 2026-11-04 -- OpenCode 2.x ingestion, scan-failure deletion bug, venv exclusions, orchestrator ingest endpoints removed, state DB single-host rule); Claude Code (Claude Haiku 5.5, 2026-11-13 -- RAGNART v0.1 move: name and path corrections only, content unchanged); Claude Code (Claude Haiku 5.5, /brain pipeline -- v0.1 post-review doc fixes); Claude Code (Claude Opus 5.5, 2026-11-13 -- post-v0.1 ops: Qdrant local config, daily snapshot retention, data root RAGNART--databases); Claude Code (Claude Opus 5.5, 2026-11-13 -- rewritten as a current-state reference); Claude Code (Claude Haiku 5.5, 2026-11-14 -- .ragignore inclusion and exclusion, admission, sync guards, rescan tool); Claude Code (Claude Haiku 5.5, 2026-11-14 -- Ollama context length 2048 and rejection of over-length input); Claude Code (Claude Haiku 5.5, 2026-11-14 -- global and per-directory exclusion files paragraph); Claude Code (Claude Haiku 5.5, 2026-11-14 -- ls-files accepts <path> arguments in §8.3); Claude Code (Claude Haiku 5.5); Claude Code (Claude Haiku 5.5, 2026-11-15 -- v0.2 session A: continuous worker pool, max_hits_per_source, OpenCode database backup §9.6); Claude Code (Claude Haiku 5.5, 2026-11-15 -- v0.2 session B: schema version and v2 columns §5.1, payload fields §4.3, Claude Code session keys §2.4, metadata backfill runbook §5.7; one item per session uuid §2.4 and §5.3; collision handling and snapshot step §5.7); Claude Code (Claude Haiku 5.5, 2026-11-16 -- v0.2 session C: ragnart project markers and attachment §5.1, plan-then-apply sync, moves and tombstones §5.3, scope and GET /v1/projects §6.4, collapse by content_hash §6.1, reconciliation §5.8, La-Familia in every scheduled run §2.1 and §8.2); Claude Code (Claude Fable 5.2, 2026-11-16 -- §-table of state tables: grants version renumbered to v0.6); Claude Code (Claude Opus 5.5, 2026-11-18 -- operator-approved v0.3 session A proposal: §1.3 and §9 restore triples with hub_registry.db, copies inspected with immutable=1); Claude Code (Claude Opus 5.5, 2026-11-19 -- v0.3 session B2: NAS ingestion on the ingestion API, two locks, reconciliation on the ingestion side, intake worker; operator-approved edits); Claude Code (Claude Opus 5.5, 2026-11-19 -- v0.3 session B1 proposal revised after B2: endpoint host, spool, units, §8.8 sources and secrets, §9.4 restore order)
updated_at: 2026-11-19--20-48
context: >
  Current-state reference for RAGNART, the self-hosted RAG retrieval service
  (repository /mnt/nfs/Florian/Gin-AI/projects/RAGNART). It describes how the
  system works today: hosts and components, the corpus and its scanner rules,
  chunking and embeddings, the Qdrant collection and payload schema, the
  ingestion pipeline and its SQLite state database, hybrid retrieval with
  cross-encoder reranking, the ragnart.yaml keys, systemd operations, the
  snapshot policy with full and SQLite-only restore procedures, and the
  ownership model as implemented. Every statement is checked against the code
  in ragnart/, utils/ and scripts/, and against the live Qdrant, search
  service and data root. Change history lives in git; incident write-ups live
  in docs/RAG-troubleshoot.md.
---

# RAGNART — RAG strategy

This document describes how RAGNART works **now**. It is a reference, not a log: for
incidents and their fixes see `docs/RAG-troubleshoot.md`; for the versioned roadmap see
`docs/Implementation-plan.md`; for change history use `git log`.

---

## 1. Overview and architecture

RAGNART is a retrieval-only service. It indexes NAS documents, Claude Code session
transcripts and OpenCode sessions into one Qdrant collection, and answers
`GET /v1/search` with ranked chunks and their sources. Generation is left to the client.

### 1.1 Hosts and components

| Host | Component | Address / path |
|---|---|---|
| **Server 1** — um690, 192.168.1.93 | Qdrant 1.17.1 (`qdrant.service`, binary `/usr/local/bin/qdrant`) | `http://192.168.1.93:6333` (gRPC 6334) |
| | Qdrant config (local copy, installed from `scripts/qdrant/qdrant-config.yaml`) | `/etc/qdrant/config.yaml` |
| | Qdrant storage (local NVMe) | `/var/lib/qdrant/storage` |
| | Qdrant snapshot working space (transient, normally empty) | `/var/lib/qdrant/snapshots` |
| **Server 2** — NucBoxK11, 192.168.1.95 | Search service `ragnart-search.service` (uvicorn, `ragnart.service:app`) | `http://192.168.1.95:8100` — `GET /v1/search`, `GET /health` |
| | Ingestion `ragnart-ingest.timer` → `ragnart-ingest.service` | 01:00, 07:00, 13:00, 19:00 local time |
| | Hub intake `ragnart-intake.timer` → `ragnart-intake.service` | every 10 minutes |
| | Ingestion API `ragnart-ingest-api.service` | port 8101, always on |
| | Ollama, `bge-m3` dense embeddings | `http://192.168.1.95:11434/api/embeddings` |
| | Reranker `bge-reranker-v2-m3` behind llama-swap | `http://192.168.1.95:8000/v1/rerank` |
| | OpenCode 2.x server (`opencode-v2-server.service`) — a corpus source | `http://192.168.1.95:4096` |
| | The only host that opens `rag_state.db` (§5.6 below) | — |
| **NAS** — 192.168.1.96 | NFSv4.1 export `/volume1/Disks`, mounted at `/mnt/nfs` on both servers with `local_lock=none` | — |
| **Endpoint host** — the operator's Mac, 192.168.1.3 (M3Max.local) | Endpoint runner `python -m ragnart.endpoint run --config ~/.ragnart/endpoint.yaml`, from the LaunchAgent `com.ragnart.ingest` (`scripts/macos/com.ragnart.ingest.plist`); sources `MBP` and `claude-MBP` | every 6 hours (`StartInterval` 21600); state `~/.ragnart/state.db`, secrets `~/.ragnart/secrets/`, logs `~/.ragnart/logs/` |

The repository itself lives on NFS (`/mnt/nfs/Florian/Gin-AI/projects/RAGNART`), so both
servers run the same code. The Python environment is the shared venv
`~/Gin-AI/.Gin-AI-python-3.12` (qdrant-client 1.17.1, fastembed 0.8.0, docling 2.89.0).

### 1.2 Data flow

```
                     ingestion (Server 2, ragnart-ingest.timer 4x/day for the NAS side; ragnart-intake.timer every 10 min for the hub)
 NAS files ──────┐
 Claude Code     ├─▶ rag_sync_nfs.py ──▶ ingestion API (:8101) ──▶ intake worker
 .jsonl sessions │     (scan, diff, plan,   request table +        (every 10 min, 3 workers)
 OpenCode API ───┘      reconcile, submit)   spool, hub registry     parse → chunk →
                        rag_state.db                                 bge-m3 dense (Ollama)
                        (its state)                                  + BM25 sparse (fastembed)
                                                                          │ upsert
                                                                          ▼
                                                        Qdrant alias `documents` (Server 1)
                                                                          ▲
 client ── GET /v1/search ──▶ ragnart-search (Server 2) ── hybrid query ──┘
        ◀── ranked chunks ──        └──▶ cross-encoder rerank (llama-swap :8000)
```

After each ingestion run, once per day, the wrapper takes a Qdrant snapshot plus a matching
copy of `rag_state.db` and keeps it on the NAS as a restore pair (§9).

### 1.3 Data root

`db_base_path` in `ragnart.yaml` (Python tools) and `RAGNART_DATA_ROOT` (shell scripts, same
default) both point to `/mnt/nfs/__Backups/RAGNART--databases/`:

```
/mnt/nfs/__Backups/RAGNART--databases/
├── rag-ingest.lock                     ← index lock: held by the intake worker and hub maintenance tools (§5.6)
├── rag-nas-ingest.lock                 ← NAS-ingestion lock: held by the NAS run (§5.6)
├── spool/                              ← uploaded bytes of submitted requests, removed once the request is completed or ignored (owner-only; not in the restore triple)
├── sqlite/
│   ├── rag_state.db                    ← ingestion queue (SQLite, WAL mode)
│   └── hub_registry.db                 ← hub registry: sources, ragnart project tables and the intake table of submitted requests (SQLite, WAL mode)
├── logs/
│   ├── rag-ingest.log                  ← ingestion + snapshot log (rotated daily, 7 kept)
│   └── rag-ingest.log.N.gz
└── qdrant-snapshots/
    └── documents/
        ├── documents_new-<id>-<YYYY-MM-DD-HH-MM-SS>.snapshot   ← Qdrant archive (~21 GB)
        ├── rag_state-<YYYY-MM-DD-HH-MM-SS>.db                  ← matching state DB (~250 MB)
        └── hub_registry-<YYYY-MM-DD-HH-MM-SS>.db               ← matching hub registry (~50 KB)
```

---

## 2. Corpus and sources

Three source types feed the same queue and the same collection. Each is identified by its
`file_path` / `source_path` string.

| Source | Discovered by | Key | Owner |
|---|---|---|---|
| NAS files | `scan_nfs_roots()` — walks `users.*.nfs_roots` and `shared.nfs_roots` | absolute NFS path | from the root it was found under |
| Claude Code sessions | `scan_claude_chats()` — walks `claude_chats.roots[].path` | absolute path of the `.jsonl` | `claude_chats.roots[].owner` |
| OpenCode sessions | `scan_opencode_sessions()` — pages through the OpenCode API | synthetic `opencode://<session_id>` | constant `opencode.owner` |

All three scanners live in `ragnart/scanner.py`; `utils/rag_sync_nfs.py` runs them in that order.

### 2.1 NFS roots and owners

Configured in `ragnart.yaml`:

| Section | Owner | Root | State |
|---|---|---|---|
| `users."florian.otel@gmail.com"` | `florian` | `/mnt/nfs/Florian` | active |
| `shared` | `la-familia` | `/mnt/nfs/La-Familia` | active; scanned by every sync run, the scheduled one included (§5.3) |
| `users` entries for `eva`, `annika`, `laura` | — | `/mnt/nfs/Eva`, `/mnt/nfs/Annika`, `/mnt/nfs/Laura` | commented out |

To add a user, uncomment (or add) a `users:` block with `owner` and `nfs_roots`. The sync scans
every configured root in one run, so the scheduled run needs no change. The email key is only a
label; nothing authenticates against it (§10).

### 2.2 Inclusion and exclusion rules

Inclusion and exclusion are separate rules. They apply to NAS roots only; the Claude Code and
OpenCode scanners have their own rules.

- **Inclusion** is `rag.scanner.include_extensions` in `ragnart.yaml`. A file is a candidate
  only if its lower-cased suffix is in the list: `.pdf .pptx .docx .md .sh .txt .yaml .yml
  .ipynb .html .jsonl`. `.csv` is listed in `ragnart.yaml` but commented out, with the reason
  written next to it.
- **Exclusion** uses gitignore semantics. `.ragignore` files inside the crawled roots, and the
  global `config/ragignore` (lowest precedence, applies under every root), exclude paths. The
  syntax, precedence and the deviations from git are described in `docs/RAGNART-design.md` §1.2.
- `ragnart check-ignore -v <path>` prints the file and line of the rule that excludes a path.
  Use it to see why a path is or is not ingested.

**Global and per-directory exclusion files.** Two files hold exclusion rules. `config/ragignore`
is the live global list. It applies to every NAS root on every scan, has the lowest precedence,
and lives in the repository, so it is reviewed and versioned with the code, as `ragnart.yaml` is.
It holds system-wide rules (`.git/`, `node_modules/`, `.venv/`, and similar). A `.ragignore` in a
directory holds that directory's own rules and belongs to its owner. Precedence, lowest first:
`config/ragignore`, then the `.ragignore` files from the root downward, where a deeper file wins.
`config/ragignore.example` is an inert template (every line commented out, excludes nothing; example
lines start with `#> `, remove the marker to activate one); copy it as `.ragignore` into a directory
to start local rules. This mirrors git's `core.excludesFile` against `.gitignore`. The git parallel
and the deviations from git are recorded in `docs/RAGNART-design.md` §1.2.

Notes on the rules in `config/ragignore`:

- A pattern with a trailing `/` matches directories only. A pattern without one (for example
  `._*`) also matches a directory whose name fits it, and excludes that directory's contents.
- `._*` (macOS AppleDouble sidecars) and `~$*` (Office lock files) match only a name that starts with `._` or
  `~$`. A mid-name `._` or `~$`, as in `Geoffrey_A._Moore_-_Crossing_the_Chasm.pdf`, is admitted.
- An excluded name does not cover a sibling with a longer name (`.Gin-AI-python-3.12/` does
  not match `.Gin-AI-python-3.12.old`). `site-packages/`, `.uv-python/` and `.cache/`
  exclude what makes a directory unwanted, whatever the venv is called.
- `.claude/` and `dot.claude-Linux/` (the real directory behind the `~/.claude` symlink) keep
  Claude Code's internal state out of the generic scan; session transcripts come in through
  `claude_chats` instead (§2.4).
- `**/Microsoft--flotel/Documents/` holds IRM/DRM-encrypted presentations that no parser can read.

To change a rule, edit `config/ragignore` (every root) or the `.ragignore` in the directory the
rule governs. The next sync applies it. A newly excluded file counts as gone: the sync tombstones it, and
its Qdrant points and queue row are purged 24 hours later (§5.3). No code change or manual Qdrant cleanup
is needed.

### 2.3 Symlink handling

`os.walk(..., followlinks=True)` follows symlinked directories. Two sets, shared across all
roots of one scan, prevent duplicates:

- `visited_real_dirs` — the real path of every directory entered. A directory whose real
  path was already visited (circular link, or the same directory reachable via two paths) is
  skipped with its subtree.
- `visited_real_files` — the real path of every eligible file. A file alias is skipped; the
  first path encountered in walk order (directory listing order, not sorted) is the one
  stored in SQLite and Qdrant.

Known symlinks under `/mnt/nfs/Florian`:

| Symlink | Target | Effect |
|---|---|---|
| `Dropbox`, `AWS--fotel`, `Gitlab--fotel`, `Microsoft--flotel` | backups under `/mnt/nfs/__Backups/…` | scanned once, under the `Florian` path |
| `Gin-AI/LLMs-cache` | `/mnt/nfs/Temp/Gin-AI--cache/LLMs-cache` | excluded by `LLMs-cache/` |
| `Gin-AI/certifications--training` | `certifications--training/__private/Gin-AI` | walked once (`visited_real_dirs`) |
| `Gin-AI/chats/*.md` | files under `certifications--training/` and `Gin-AI/tools/` | ingested once (`visited_real_files`) |
| `.venv` links | `Gin-AI/.Gin-AI-python-3.12` | excluded by `config/ragignore` |

### 2.4 Claude Code sessions (`claude_chats`)

`scan_claude_chats()` walks each `claude_chats.roots[].path` (configured:
`/home/florian/.claude/projects`, owner `florian`) with `followlinks=True`, takes every
`.jsonl` (main sessions, `<uuid>/subagents/agent-*.jsonl` and
`<uuid>/subagents/workflows/wf_*/agent-*.jsonl`), dedups by real path, and applies none of the
`rag.scanner` rules.

Each transcript is queued under a session key, not its path. `claude_jsonl_key()` in
`ragnart/scanner.py` computes the key, and the real `.jsonl` path is stored in `origin_path`:

| Transcript | Queue key (`file_path`) |
|---|---|
| `<uuid>.jsonl` | `claude-session://<uuid>` |
| `<uuid>/subagents/agent-<id>.jsonl` | `claude-session://<uuid>/agent-<id>` |
| `<uuid>/subagents/workflows/wf_<w>/agent-<id>.jsonl` | `claude-session://<uuid>/agent-<id>` |
| `<uuid>/subagents/workflows/wf_<w>/journal.jsonl` | not queued: a workflow orchestration log, not a conversation |

A workflow `journal.jsonl` is never discovered, so a sync tombstones any row it had, subject to the
guards in §5.3. A `claude_chats` root that is missing, or that yields no transcripts while the
queue still holds rows under it, blocks moves and tombstones for the whole run (§5.3).

A session uuid can be found in more than one transcript file: for example in two Claude Code project folders after a session was copied, or in a git worktree. Within one scan, all copies of one key compete: the newest copy by modification time wins, and an exact tie goes to the greatest path. The key is queued once, from the winning copy, and `origin_path` follows the winner. The other copies are duplicate locations and are not indexed.

`_parse_claude_chat()` in `ragnart/ingest.py` reads the transcript at `origin_path` and turns it
into text:

- keeps user and assistant text blocks; skips meta entries, `tool_use`, `tool_result`,
  thinking blocks, XML-tagged command strings and file-history snapshots;
- prefixes a header (`Session`, `Project: <basename of cwd> (<cwd>)`, `Date`, `Model`,
  `Branch`, `Version`);
- sets payload fields `session_id`, `project` (basename of the session's cwd) and
  `session_title`. The title comes from the human-named symlinks in `~/.claude/chats/`
  (read-only, never ingested; UUID-named links are ignored); without one, it is the first
  real user message, cut to 60 characters. Subagent transcripts get the parent session's
  title plus ` [subagent]`.

Points get `file_type = "claude_chat"`, and `file_name` is set to the session title. The first
non-empty top-level `cwd` of the transcript is the session's launch directory. It is stored
verbatim in the `launch_dir` column (§5.1) and not in the payload.

### 2.5 OpenCode sessions (`opencode`)

OpenCode sessions are rows in OpenCode's own database, reachable only through its HTTP API.
They have no file on disk: the key `opencode://<session_id>` is synthetic, so path-based
rules (scanner exclusions, per-root owner assignment) do not apply to them.

- **Authentication.** OpenCode 2.x requires HTTP Basic auth, user `opencode`, password
  `OPENCODE_SERVER_PASSWORD`. `ragnart/opencode_client.py` reads the password from the
  server's own environment file (`opencode.password_env_file`, default
  `~/.config/opencode/opencode-server.env`, present on Server 2), so a password change on
  the server needs no RAGNART change. `get_json()` rejects any answer that is not
  `application/json`, because unknown routes on 2.x return HTTP 200 with HTML.
- **Discovery.** `GET /api/session?limit=100`, following `cursor.next` until it is empty
  (cap: 1,000 pages). This lists every session, regardless of the directory OpenCode was
  launched from. The queue mtime is `time.updated / 1000`.
- **Parsing.** `_parse_opencode_session()` reads `GET /api/session/{id}` (title, created
  time, `location.directory`) and walks `GET /api/session/{id}/message?order=asc&limit=200`
  by `cursor.next` until an empty page. It keeps `type: "user"` text (except slash
  commands) and the `type: "text"` parts of `type: "assistant"` content; reasoning, tool
  parts and all other message types are skipped. The header carries `Session`,
  `Launched from`, `Date`, `Title`; the launch directory is context, not a project.
  Payload: `session_id`, `session_title` (from the API, else the first user message, else the
  id); no `project` field. `file_type = "opencode"`, `file_name` = session title. HTTP, auth
  and non-JSON errors are raised, so the item is marked failed and retried; a session with no
  user or assistant text is marked completed with no points.
- **Unreachability guard.** If the API cannot be scanned (unreachable, HTTP error, missing
  password file), the scanner returns `existing_paths=None` and `rag_sync_nfs.py` counts
  every queued `opencode://` row as still present (`StateDB.paths_with_prefix()`), so their
  Qdrant points are preserved. A 401/403 is logged at ERROR.
- **Owner.** Every OpenCode point gets the single constant `opencode.owner` (`florian`).
  That is correct while one person uses one OpenCode server (§10).

The routes `/session/{id}` and `/session/{id}/message` (without `/api`) must not be used
against OpenCode 2.x: they answer 200 with HTML.

### 2.6 Parsers by file type

| Type | Parser | `file_type` stored |
|---|---|---|
| `.pdf`, `.docx` | docling → Markdown; on docling failure the file fails (no raw read): it is retried, then `ignored` with `skip_reason` `docling failed: …` | `pdf`, `docx` |
| `.pptx` | docling; on failure `_parse_pptx()`: OLE2 files (magic `D0 CF 11 E0`) are converted with `libreoffice --headless --convert-to pptx` first, then python-pptx extracts slide text (`Slide N` headers). If that also fails, the file fails — no raw read | `pptx` |
| `.ipynb` | `_parse_ipynb()`: markdown cells as prose, code cells as fenced `python` blocks, outputs ignored; raw read only if the JSON is invalid | `ipynb` |
| `.md`, `.txt`, `.csv`, `.yaml`, `.yml`, `.sh`, `.html` | raw UTF-8 read (`errors="replace"`), after the admission checks of `docs/RAGNART-design.md` §5 | the extension |
| Claude Code `.jsonl` | `_parse_claude_chat()` (§2.4) | `claude_chat` |
| OpenCode session | `_parse_opencode_session()` (§2.5) | `opencode` |

docling runs with one thread-local `DocumentConverter` per worker thread, built with explicit
CPU accelerator options. IRM/DRM-encrypted PowerPoint files (OLE2 with an `EncryptedPackage`
stream) cannot be converted; they exhaust their retries and stay `ignored`.

**Quality gate.** After parsing, the printable ratio is the fraction of characters that are
printable, or tab, newline or carriage return, and not U+FFFD (replacement character). Blank text
counts as 1.0. A document below 0.85 is marked `ignored` with `skip_reason = "binary content: N% printable chars"`
and gets no points. An empty document is marked `completed` with no points. The size cap, the
NUL sniff, the U+FFFD ratio and the chunk cap are described in `docs/RAGNART-design.md` §5.

---

## 3. Chunking and embeddings

### 3.1 Chunking

Token counts use tiktoken `cl100k_base` (`ragnart/ingest.py`).

| Strategy | When | Sizes (tokens) | Stored |
|---|---|---|---|
| **Parent-child** | default | parent 1000 / overlap 100; each parent split into children of 250 / overlap 20 | each child is one point; `text` = child (embedded), `parent_text` = its parent (returned to the client) |
| **Flat** | `file_type` in `pptx`, `yaml`, `yml`, `csv`, or the whole document is under 600 tokens | 512 / overlap 50 | `text` = `parent_text` = the chunk |

`chunk_index` is the point's position in the document's chunk list. The parent text is kept in
every child's payload (duplicated across siblings) so one Qdrant query returns everything —
there is no separate docstore.

### 3.2 Dense embeddings — bge-m3 via Ollama

- Model `bge-m3` (1024 dimensions, 8192-token context), served by Ollama on Server 2.
  One key, `rag.ollama_url`, feeds both ingestion and search, so queries and documents are
  always embedded by the same model.
- API: `POST /api/embeddings` with `{"model", "prompt"}`, one text per request (no batch
  endpoint). `embed_batch()` runs up to 5 requests concurrently per item (`_BATCH_CONCURRENCY` in
  `ragnart/embeddings.py`) and calls a progress callback every 50 completions.
- `embed_text()` has a 120 s HTTP timeout. Search queries share Ollama with a running ingest
  and can queue behind its embedding requests.
- Server 2's Ollama runs with `OLLAMA_CONTEXT_LENGTH=2048`, set by the systemd drop-in
  `/etc/systemd/system/ollama.service.d/ragnart-context-length.conf`. Chunks are sized in cl100k
  tokens (flat chunks up to 512), and bge-m3's XLM-R tokenizer produces more tokens than cl100k on
  code, YAML and non-English text. A pathological chunk can still exceed 2048 bge-m3 tokens. Ollama
  rejects such an input with HTTP 500 (`the input length exceeds the context length`); it does not
  truncate it. The item is retried, then ends `ignored` (`docs/RAG-troubleshoot.md` §2.13). Any
  other Ollama used for embedding must run the same model with the same context length, or its
  vectors will differ for those chunks.

### 3.3 Sparse vectors — BM25 via fastembed

`embed_sparse()` uses fastembed `Qdrant/bm25` (CPU, in-process, loaded on first use) and
returns token indices and weights. Ingestion adds a sparse vector to every point when the
collection has the `sparse_text` space (checked once per process); search computes one for
the query in hybrid mode. Qdrant applies IDF at query time (`modifier: idf`).

---

## 4. Qdrant collection and payload schema

### 4.1 Deployment

Qdrant runs on Server 1 as `qdrant.service` (`User=florian`,
`ExecStart=/usr/local/bin/qdrant --config-path /etc/qdrant/config.yaml`, `Restart=on-failure`).

- Storage must be on local disk: Qdrant refuses to start with its `storage_path` on NFS
  (inconsistent file locking corrupts its segment store). `storage_path` and
  `snapshots_path` are both on Server 1's NVMe.
- The config is a local copy so a slow NFS mount at boot cannot make Qdrant start with
  built-in defaults. After editing `scripts/qdrant/qdrant-config.yaml`, re-install it on
  Server 1: `sudo bash /mnt/nfs/Florian/Gin-AI/projects/RAGNART/scripts/qdrant/install-qdrant.sh`
  (copies config and unit, creates `/var/lib/qdrant/snapshots`, restarts Qdrant, waits for
  `/healthz`; search answers 503 for those seconds).
- Python code talks to Qdrant over HTTP with `qdrant-client` (`get_client()`, default
  timeout 60 s; the sync uses 120 s for its deletes).

### 4.2 The `documents` collection

Code always addresses `documents` (`ragnart/collection.py: DOCUMENTS_COLLECTION`). In the
live deployment `documents` is an **alias** for the real collection `documents_new`:

| Property | Value |
|---|---|
| Alias | `documents` → `documents_new` (only collection on the server) |
| Points | ~2.99 million |
| Dense vector | unnamed, 1024 dimensions, cosine |
| Sparse vector | `sparse_text`, `modifier: idf` |
| Payload indexes | keyword: `source_path`, `project_id`, `owner`, `source_type`, `file_type`, `content_hash`; datetime: `modified_at`, `ingested_at` (`ensure_payload_indexes()`, `ragnart/collection.py`) |
| HNSW | `m 16`, `ef_construct 100`, in RAM |
| Payload storage | on disk (`on_disk_payload: true`) |

`ensure_collection()` runs at search-service startup, at intake-worker startup and on every search
and ingest call. If neither a collection nor an alias named `documents` exists, it creates a
real `documents` collection with the dense and sparse spaces above. It also creates the
payload indexes listed above that are missing. Every per-file delete filters on `source_path`,
and without that index such deletes scan the whole collection and time out.

Qdrant 1.17 cannot add a sparse space to an existing collection. `utils/rag_sparse_migrate.py`
handles that case: `migrate` copies all points into `documents_new` with computed sparse
vectors (safe while live), `swap --confirm` deletes `documents` and creates the alias
(stop search and ingestion first), `status` is read-only.

```bash
python utils/rag_sparse_migrate.py status      # alias target, point count, sparse space present
```

### 4.3 Payload schema

Field names are constants in `ragnart/schema.py`; no other module uses string literals for them.

| Field | Content | Present on |
|---|---|---|
| `text` | child chunk — what was embedded and what the reranker scores | all |
| `parent_text` | parent chunk (or the flat chunk) — returned as `content` | all |
| `owner` | owner string (`florian`, `la-familia`, …) | all |
| `source_path` | NFS path, `opencode://<id>`, or `claude-session://<uuid>[/agent-<id>]` (the queue key, not the transcript path) — the provenance returned to clients | all |
| `file_name` | file name; session title for `claude_chat` and `opencode` | all |
| `file_type` | extension, `claude_chat` or `opencode` | all |
| `page` | always `0` (no page tracking) | all |
| `chunk_index` | position in the document's chunk list | all |
| `tag` | `""` unless the submitting source sends a tag (the NAS-side sources send none) | all |
| `session_id` | session UUID / OpenCode id | `claude_chat`, `opencode` |
| `project` | basename of the session's cwd | `claude_chat` only |
| `session_title` | human-readable session title | `claude_chat`, `opencode` |
| `project_id` | id of the closest enclosing ragnart project (§5.1), or `global` when no ragnart project encloses the item | all |
| `source_type` | `file`, `claude_session` or `opencode_session` | all |
| `modified_at` | ISO 8601 UTC of the queue row's `last_modified` (file mtime, or OpenCode `time.updated`) | all |
| `ingested_at` | ISO 8601 UTC of the time the points were written; the same value as the row's `completed_at` | all |
| `content_hash` | MD5 hex digest of the source bytes. No value for `opencode`: `null` on points written by the new ingest code, absent on points written by the backfill (§5.7) | all |

Point IDs are random UUIDs; per-file delete-before-insert (§5.4) makes deterministic IDs
unnecessary.

---

## 5. Ingestion pipeline and state DB

### 5.1 `rag_state.db` — the ingestion queue

`/mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db`, SQLite in WAL mode, managed by
`ragnart/state.py` (`StateDB`). It holds six tables: `ingestion_queue` (below) and five project
tables (after the block).

Schema version is tracked in SQLite's `PRAGMA user_version`. `ragnart/state.py`'s `_MIGRATIONS` is an
ordered `(version, statements)` list. At `StateDB()` open, every migration above the stored version
runs in ascending order, and the stored version is set after each one. A `StateDB(..., read_only=True)` runs no migration. Each statement runs on its own,
and an `OperationalError` (for example, a column that already exists) is ignored, so a migration
re-runs safely after a crash. The current version is 2. Version 1 is the `skip_reason` column and the
`max_retries` floor. Version 2 is the columns and tables below.

```sql
CREATE TABLE ingestion_queue (
    file_path       TEXT PRIMARY KEY,      -- NFS path, claude-session://<uuid>[/agent-<id>], or opencode://<id>
    owner           TEXT NOT NULL,
    last_modified   REAL NOT NULL,         -- mtime (or OpenCode time.updated) at discovery
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending | processing | completed | ignored
    error_msg       TEXT,                  -- last error while still retrying
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 5,
    started_at      TEXT,                  -- ISO UTC, set on → processing
    completed_at    TEXT,                  -- ISO UTC, set on → completed
    progress_detail TEXT,                  -- starting | parsing | chunking (N chunks) | embedding i/N | upserting
    skip_reason     TEXT,                  -- why the row is ignored
    -- Version 2 columns (12). Discovery fills origin_path, source_type, file_type and first_seen_at.
    -- Ingest and the backfill fill the rest.
    project_id       TEXT NOT NULL DEFAULT 'global',  -- 'global' = outside every ragnart project
    source_type      TEXT,                  -- file | claude_session | opencode_session
    file_type        TEXT,                  -- extension, claude_chat or opencode
    origin_path      TEXT,                  -- where the bytes are read: the file (NAS), the .jsonl (claude-session://); NULL for opencode://
    size_bytes       INTEGER,               -- NULL for opencode://
    content_hash     TEXT,                  -- MD5 hex; NULL for opencode://
    hashed_at        TEXT,                  -- ISO UTC of the hash; NULL for opencode://
    first_seen_at    TEXT,                  -- ISO UTC, set on insert by discovery; rows completed before v2 take completed_at
    pipeline_version TEXT,                  -- 'p1' once written by ingest or the backfill; NULL = not migrated
    chunk_count      INTEGER,               -- points written
    missing_since    TEXT,                  -- tombstone: set when the item vanished, cleared on reappearance, purged 24 h later (§5.3)
    launch_dir       TEXT                   -- session launch directory, verbatim; NULL for files
);
```

The five project tables are created by version 2. `projects` holds the reserved row `global`
(name `Global`, no path, no parent, `inherit_access` 0), inserted with `INSERT OR IGNORE`, and one
row per ragnart project, written by each sync (below). Only `projects` and `project_tags` are written
by code today. The other three are created empty and no code writes them yet:

| Table | Holds |
|---|---|
| `projects` | The reserved `global` row, plus one row per ragnart project found by the last sync |
| `project_tags` | `key = value` tags per project, from the non-reserved keys of its marker |
| `project_links` | Links between projects (not written yet) |
| `source_links` | Source paths attached to a project (not written yet; a rename moves its rows) |
| `grants` | Role per principal per project (not written yet; grants are v0.6) |

The purpose of the `global` project and of these tables is in `docs/RAGNART-design.md` §4.6.

**ragnart projects and attachment.** A directory that holds `.ragnart/project.toml` is a ragnart
project. The model is in `docs/RAGNART-design.md` §4.5 and §4.6. The NAS scan reads each marker
during its walk (`ragnart/scanner.py`). A marker missing `name` or `id`, or not valid TOML, is
logged at ERROR and ignored: its directory stays an ordinary directory (`ragnart/projects.py`). Each
sync upserts the marker's project and tags. A project whose marker is gone is removed, unless a root
was skipped in that run (§5.3). The first ragnart project is this repository (`.ragnart/project.toml`,
name `RAGNART`).

Every completed item carries a `project_id`: the closest enclosing ragnart project, or `global`. A file
is matched by its key (its path). A session is matched by its `launch_dir`. Matching is verbatim string
comparison, with no `realpath`, so a session launched through a symlinked path does not attach. Ingestion
sets the value when it writes the points. Each sync re-evaluates it for every completed row and corrects
both SQLite and the points (§5.3, step 7).

The `StateDB()` constructor writes (`PRAGMA journal_mode=WAL`, `CREATE TABLE IF NOT EXISTS`,
the migrations above, commit) — opening the DB with it is never read-only.

### 5.2 Status transitions

| From → to | Trigger |
|---|---|
| (new) → `pending` | the sync's scan discovers a path not in the table |
| `pending` → `processing` | the sync submits the item and the hub accepts the request (`request_id` set) |
| `processing` → `completed` | the next run's poll reads the hub's status `completed` (the hub ingested it, or the document was empty or produced no chunks) |
| `processing` → `pending` | the poll reads `failed` and `retry_count + 1 < max_retries` (`error_msg` set); the hub not knowing the request (404) counts as `failed`; the retries are counted over runs, not attempts within a run |
| `processing` → `ignored` | the poll reads `ignored` (the hub's admission rules, `skip_reason` = the rule), a failure on the last retry, or the hub refused the upload at the door with 413 |
| `pending` stays `pending` | the hub answered 422 (a request error): nothing is deleted, the row is left, an ERROR is logged |
| any → `pending` | the scan sees `last_modified` on disk newer than the stored value (clears retries, errors, `skip_reason`), or the reconciliation finds a completed row missing on the hub or older than the hub's pipeline version |

`ignored` rows are never retried while the file is unchanged. A `failed` status is not set by
any code path; if set by hand, the next sync resets it to `pending` with zero retries.

### 5.3 Sync — `utils/rag_sync_nfs.py`

One sync run polls the outcome of its earlier requests, scans every configured root, plans every change, logs the plan, reconciles against the hub's listing, then submits.
A file that vanishes is tombstoned, not deleted: its points stay searchable for 24 hours. The code is
`utils/rag_sync_nfs.py` (the run), `ragnart/sync_plan_core.py` (the plan), `ragnart/sync_submit.py` (the plan
turned into API requests, in its order), `ragnart/sync_poll.py` (the outcome of earlier requests) and
`ragnart/sync_reconcile.py` (reconciliation and rebuild).

**One call for all roots.** A run scans the NAS roots (`la-familia` included), the Claude Code
transcripts and the OpenCode sessions, in one call. The scheduled run passes no `--user`. `--user OWNER`
limits the scan and the plan to that owner's roots and rows: the plan never removes a project, tombstones
or purges another owner's data.

1. **Scan.** The three scanners (§2) call `discover_or_update()` per path: new → `pending`,
   newer mtime → `pending`, otherwise no change. A new NAS file is hashed here, so a move can be
   recognised in the same run. Each scanner reports its skipped roots and any project markers (§5.1).
   A `claude_chats` scan queues one copy per session key: the newest copy wins (§2.4).
2. **Plan.** `build_sync_plan()` reads the queue and writes nothing. It works out:
   - **Project upserts and removals.** Each marker is upserted with its tags. A project whose marker
     is gone is removed, and its items are re-attached in the same plan.
   - **Moves.** A completed row that vanished and a new file with the same non-empty `content_hash`,
     one of each in this run. The key is rewritten; nothing is re-embedded. The empty-file hash never
     pairs, and a session key never moves.
   - **Newly missing.** Every other completed row that vanished. It gets `missing_since` and keeps its points.
   - **Reappearances.** A tombstoned row that the scan found again. Its `missing_since` is cleared.
   - **Purges.** A tombstone whose `missing_since` is more than 24 hours old and that is still gone.
     Its points are deleted, then its row.
   - **Stale rows.** Non-completed rows (pending, processing, failed, ignored) whose source is gone.
     They have no points, so their queue row is removed.
   - **Attachment.** Each completed row whose `project_id` differs from its closest enclosing ragnart
     project (§5.1). Moves are applied first, so the change is keyed by the new key.

   Every vanished item is classified with its reason (for example "file missing on disk" or "extension
   no longer in rag.scanner.include_extensions"). The reason is printed with the plan.
3. **Submit.** `submit_plan()` sends the plan to the ingestion API in this order: projects
   (`POST /v1/projects`, `POST /v1/projects/remove`), moves (`change_key`), purges (`delete`), attachment
   (`change_metadata`), then the uploads of pending rows. A move, a purge or a project removal changes the
   local row only after the hub accepted the request. Reappearances, tombstones and stale rows are local.
   The sync never writes Qdrant. A crash between the request and the local change is repaired by the next
   run: the reconciliation compares the hub's listing with the rows.

**Guards.** Each guard refuses one part of the plan. Each logs an ERROR, and the run exits 1.

- **Skipped roots.** An NFS root that is not a directory, or that is empty while the queue holds
  rows under it, counts as unmounted. A `claude_chats` root that is not a directory, or that yields zero
  transcripts while the queue holds rows under it, is skipped too. If any root is skipped, the plan
  makes no moves, no tombstones and no project removals. Project upserts and attachment still run, and so
  do the purges: those rows were tombstoned by an earlier sync that saw every root.
- **Mass delete.** If the vanished completed rows that are not moves exceed `rag.sync.max_delete_fraction`
  (`ragnart.yaml`, `0.10`) of all completed rows, the run makes no tombstones and removes no stale
  rows. Moves are not counted and still apply. Purges still run. `--allow-mass-delete` proceeds anyway,
  and logs a WARNING with the counts.

Diagnosis and recovery are in `docs/RAG-troubleshoot.md` §2.12 (guards) and §2.15 (plan, moves,
tombstones).

**Dry run.** `python utils/rag_sync_nfs.py --dry-run` runs the scan and the plan on a private copy of
the live database, made with SQLite's backup API from a read-only connection. It prints the plan and
exits 0. It takes no lock, makes no Qdrant client, and writes nothing to the live database. The copy is
removed afterwards.

**The printed plan.** The sync logs the plan before it applies it, and `--dry-run` prints the same text.
It opens with two header lines: whether roots were skipped, and whether the mass-delete guard tripped.
Then one block per kind, each with its count and one entry per item: project upserts, project removals,
moves (`old -> new`), newly missing (tombstoned, points kept), reappeared, purge, stale queue rows removed,
attachment changes.

**Log lines** (applied changes, `ragnart/sync_submit.py`):

| Line | Meaning |
|---|---|
| `Moved <old> -> <new> (content unchanged, no re-embed)` | Key rewritten |
| `Tombstoned (<reason>), points kept until the grace period ends: <path>` | `missing_since` set |
| `Reappeared, tombstone cleared: <path>` | Tombstone cleared |
| `purged <path> chunks=<n> reason=tombstone grace period expired missing_since=<time>` | Delete request sent and the row removed; this is the purge record |
| `submitted <key> (request <id>)` | An upload accepted by the hub |
| `refused by the hub <key>: <detail>` | 413: the old points were deleted and the row is `ignored` |
| `<key> refused by the hub as a bad request (422), left pending: <detail>` (ERROR) | Nothing deleted; the row stays pending |
| `Queue row removed (<reason>): <path>` | Stale row removed |

**Exit codes.** `0`: the sync applied its plan, or a dry run finished. `1`: the deletion phase was
refused, by a skipped root or by the mass-delete guard. The queueing of the scan still stands in that
case. `75`: another NAS run holds the NAS-ingestion lock (§5.6), and the run did nothing.

**Grace period.** 24 hours, `DEFAULT_GRACE_HOURS` in `ragnart/sync_plan_core.py`. It is measured from the
first sync that saw the item missing. A later sync that still does not see it keeps that first time.

### 5.4 Worker loop — `ingest_content()`

`ragnart/intake_worker.py` drains the intake table with a pool of three workers (`--workers`). A worker
claims the oldest `received` request (`registry.next_intake()`, one `UPDATE ... RETURNING`), applies it, and
claims the next. A request is not claimed while an earlier request for the same item (same source and key,
or key or new key of a move) is still `received` or `processing`, so one item's requests are applied in
order. An upsert calls `ingest_content()` with the item's bytes or rendered text. The pool stops when no
request is claimable. Per upsert:

| Step | Action |
|---|---|
| 0 | `ensure_collection()`; delete every Qdrant point with this `source_path` (synchronous) |
| 1 | mark the intake request `processing` |
| 2 | admission checks for text-like files (size cap, NUL sniff, U+FFFD ratio; `ignored` with a reason, `docs/RAGNART-design.md` §5); then parse (§2.6), off the event loop (for a connector-native format the item arrives as rendered text and is not parsed); empty text → `completed`; printable ratio < 0.85 → `ignored` |
| 3 | chunk (§3.1); no chunks → `completed`; more chunks than `rag.admission.max_chunks_per_item` → `ignored` with `too large: N chunks > … chunk cap` |
| 4 | embed child texts with bge-m3, 5 requests in flight |
| 5 | build points: random UUID, dense vector (plus BM25 sparse vector when the collection has `sparse_text`), payload (§4.3) |
| 6 | upsert in batches of 256 points (keeps each request under Qdrant's HTTP body limit) |
| 7 | mark the intake request `completed` with its chunk count |

Any exception marks the request `failed` (the ingestion side's poll decides the retry, §5.2), and the worker moves on.

**Metadata (v2).** Before step 5, `ingest_content()` takes the content hash of what it received: the md5 of
the bytes of a file, the md5 of the rendered text of a Claude Code session, and none for an OpenCode session
(its `content_hash` is null). A Claude Code point written before session B2 carries the md5 of the `.jsonl`
file until the session is submitted again. The ingestion side keeps its own change-detection value (size, md5
of the source file, hash time) in `rag_state.db`; it records the hub's `chunk_count` and `pipeline_version`
when the poll reads `completed`. Every point carries `project_id` (§5.1), `source_type`, `modified_at`,
`ingested_at`, `owner`, `tag` and `content_hash`, as the request supplied them. `launch_dir` is the
session's launch directory, verbatim; it stays in the state database and is never sent to the hub or written
to a payload.

**Idempotency.** Step 0 makes every attempt start from zero points for that file. A retry
after a partial upsert, a re-ingest of a modified file, or a re-run after a crash between
steps 6 and 7 all end with exactly one set of points. A file in `ignored` has no points.

### 5.5 Intake worker flags

`python -m ragnart.intake_worker [--workers N]`. `--workers` (default 3) is the number of requests applied at
once; docling parsing runs off the event loop and overlaps other items' embedding. For a gentler load on
Ollama run it with `--workers 1` (a manual run, with `ragnart-intake.timer` stopped). The number of
concurrent Ollama requests per item is 5 (`_BATCH_CONCURRENCY` in `ragnart/embeddings.py`) and has no flag.
The NAS ingestion run takes no worker or batch setting; the resubmission cap per run is
`rag.nas_ingest.resubmit_batch_size` (200).

### 5.6 Concurrency rules

**One writer per lock, across hosts.** There are two locks, both taken with `fcntl.lockf(LOCK_EX | LOCK_NB)`
(POSIX record lock, `F_SETLK`; code in `ragnart/lock.py`):

- the **index lock** `rag.ingest_lock` (`/mnt/nfs/__Backups/RAGNART--databases/rag-ingest.lock`), held by the
  process that writes Qdrant points: the intake worker, and the maintenance tools that write the index or the hub
  registry;
- the **NAS-ingestion lock** `rag.nas_ingest.lock` (`/mnt/nfs/__Backups/RAGNART--databases/rag-nas-ingest.lock`),
  held by the NAS run, which writes `rag_state.db` and submits requests to the ingestion API.

With NFSv4.1 and `local_lock=none` the NAS lock manager arbitrates, so both locks hold across machines. The kernel
releases a lock when its holder exits. `flock()` is not used: on this NAS a non-blocking `flock` blocks
indefinitely. Both keys are required in `ragnart.yaml`; a missing key is an error, and there is no host-local
fallback.

Index lock:

- `ragnart/intake_worker.py` (every run)
- `utils/rag_reset.py` without `--dry-run`
- `utils/rag_metadata_backfill.py --apply`, `utils/rag_source_backfill.py --apply`, `utils/rag_registry_migrate.py --apply`
- `utils/rag_source_register.py --apply`, `utils/rag_source_remove.py --apply`

NAS-ingestion lock:

- `utils/rag_sync_nfs.py` (every run except `--dry-run`, including `--rebuild-from-hub`)
- `scripts/ragnart-ingest-run.sh` (the whole run: sync, WAL checkpoints, snapshot)

Both locks, index lock first: `scripts/sqlite-qdrant-snapshot.sh` (the whole run, standalone or from the run script).

A step started by a lock holder finds the lock held by its ancestor and does not take it again. The snapshot script,
started by the run script, therefore finds the NAS lock held and takes only the index lock.
`utils/rag_sparse_migrate.py` is the one exception: a one-off migration, run with both timers stopped.
`utils/rag_purge_corrupt.py`, `utils/rag_purge_rescan.py`, `utils/rag_status.py`, the dry runs and the read-only
`ragnart` CLI write nothing and take no lock.

A lock is not waited for. A process that finds it held prints one line to stderr and exits with status 75
(`EX_TEMPFAIL`):

```
ragnart: another writer holds the ingestion lock (<path>): pid N on HOST, <command>, since <time>
```

The holder writes the same `pid N on HOST, <command>, since <time>` line into the lock file after it takes the lock.
A contender never rewrites the file. The file is not cleared when the holder exits, so when the lock is free it shows
the last holder's line. Its mtime is the last acquisition.

The index lock exists because the index has exactly one writer process. Two intake workers could apply requests for
the same item at once, and an upsert's delete-then-insert would leave duplicate points. Inside one worker process the
claim (`registry.next_intake()`) is a single SQL statement, and a request is not claimed while an earlier request for
the same item is still waiting or in progress. The NAS-ingestion lock keeps two NAS runs from submitting the same rows
twice. The snapshot takes both locks, so neither the index nor `rag_state.db` changes under it.

The ingestion API writes `hub_registry.db` (the intake table and published project records) without either lock;
SQLite arbitrates those writes. The registry copy in a restore triple therefore carries a weaker guarantee than the
`rag_state.db` copy and the Qdrant snapshot: a request may arrive while it is taken.

| Scenario | Outcome |
|---|---|
| Two holders of the same lock, any hosts | the second exits 75 at once, names the holder, and writes nothing |
| Timer fires while its run is active | systemd does not start a oneshot that is still running |
| Manual `scripts/ragnart-ingest-run.sh` while the service runs | exits 75 at once, names the holder; no step runs |
| Manual sync while a NAS run holds the NAS lock, or a manual intake worker while the timer's holds the index lock | exits 75 at once, names the holder; nothing is written |
| A timer run starts while a manual holder has its lock | the unit exits 75 and systemd counts it as success (`SuccessExitStatus=75` in `ragnart-ingest.service` and `ragnart-intake.service`); nothing is written, and the next slot runs normally |
| NAS run and intake worker at the same time | both proceed: they hold different locks; the run submits, the worker applies |

**`rag_state.db` is opened from Server 2 only.** In WAL mode, SQLite keeps the WAL index in
`rag_state.db-shm`, memory-mapped as shared memory. Two hosts map it through their own page
caches over NFS with nothing keeping the copies coherent; NFS file locks do not cover that
index. A connection from a second host while Server 2 writes — including a read-only
`sqlite3 "file:…?mode=ro"` — can return wrong results or make the writer fail with
`database disk image is malformed`. Therefore the timer, `rag_sync_nfs.py`, the
snapshot script, `rag_status.py`, `rag_reset.py`, `rag_purge_corrupt.py`, `rag_purge_rescan.py` and
ad-hoc `sqlite3` sessions all run on Server 2. Of these,
the sync and the run script take the NAS-ingestion lock, the snapshot script takes both, and `rag_reset.py`
(without `--dry-run`) takes the index lock. `rag_status.py`, the dry runs, `rag_purge_corrupt.py`,
`rag_purge_rescan.py` and the read-only `ragnart` CLI never take one.

**The search service opens the state DB too, read-only, on Server 2 only.** `ragnart/service.py` holds
one `StateDB(..., read_only=True)` connection for the whole process, opened at startup. It reads the
projects and tags for scope resolution and for `GET /v1/projects` (§6.4), and it never writes. This is
safe for the same reason the writer is safe: both processes run on Server 2, so both map the same
`rag_state.db-shm` in one kernel, and the WAL index is coherent. The rule above still applies to every
other host: a second host must never open the state DB, read-only or not. If the file cannot be opened
when the service starts, the service still runs, and the scope-dependent endpoints answer 503
`Queue database unavailable` until the service is restarted.

### 5.7 Metadata backfill — `utils/rag_metadata_backfill.py`

A one-time, payload-only migration. It gives every item ingested before v2 its metadata (§4.3, §5.1):
`set_payload` in Qdrant and SQLite column updates. It never re-embeds and never touches vectors.
Rows written by the v2 ingest code already carry `pipeline_version = 'p1'`, so the backfill skips them.

**Row kinds** (`classify()`):

| Kind | Rows | What it does |
|---|---|---|
| `nas` | NAS files | Hashes the file. Payload set under its unchanged key. |
| `claude` | Transcripts under a `claude_chats` root | Hashes the file and reads its first `cwd`. Re-keys from the `.jsonl` path to its `claude-session://` key, in Qdrant and SQLite. `origin_path` keeps the `.jsonl` path. |
| `claude_journal_skip` | Workflow `journal.jsonl` | Nothing. Left unchanged. |
| `opencode` | `opencode://` sessions | Reads `launch_dir` from `GET /api/session/<id>`. No hash. |

**Candidates:** rows with `status = 'completed'` and `pipeline_version IS NULL`, ordered by `file_path`.
`ignored` rows are not candidates. `--prefix PATH` is a plain string match on `file_path`. End it with
`/` so that a sibling directory is not matched.

**Modes:**

- **Dry-run** (the default; `--dry-run` is accepted too). Opens the DB read-only, takes no lock, writes
  nothing, and calls neither Qdrant nor the OpenCode API. It prints the count per kind, the hashing volume,
  and the estimated time at `--assumed-mb-per-sec` (default 50).
- **Apply** (`--apply`). Takes the index lock (§5.6) before it opens the DB. A lock that is held
  exits 75 and names the holder.

**Per-row order** (`process_one()`), committed row by row:

1. Read the file (hash, first `cwd`) or the OpenCode API. Nothing is written yet.
2. Qdrant `set_payload`, filtered on the row's **current** key, `wait=True`. For a Claude row the payload's
   `source_path` is the new key.
3. Count the points under the new key (NAS and Claude rows; OpenCode rows skip this step).
4. One SQLite `UPDATE` (re-key and metadata), committed. This is the step that marks the row migrated.

A crash before step 4 leaves `pipeline_version` NULL, and a re-run repeats the row. A crash after step 2 is
harmless: the old-key filter then matches no points, and step 3 still counts the new key.

**Per-row failures.** A failure in any step (vanished file, read error, Qdrant error, OpenCode API error)
is logged with the key and the reason, and counted. The row keeps
`pipeline_version` NULL, and the run continues with the next row. The run exits 1 if any row failed, 0
otherwise. A re-run retries the failed rows.

**Collisions.** One session uuid can be found in two transcript files (§2.4). If the old-key row's new key already has a row, the two copies are resolved, not failed. The newer file by modification time wins; an exact tie goes to the greatest path. Every Qdrant point under either key is deleted, the old-key row is removed, and the row under the new key keeps its place with `origin_path` set to the winner and status `pending`, so the next NAS run submits the winning copy. The run summary counts these as `collisions resolved: N`.

**Checkpoint.** `{db_base_path}/sqlite/metadata_backfill.checkpoint` (default; `--checkpoint-file`
overrides it). One line every 100 rows: UTC time, `done/total` and the last `file_path`. The file is
appended to, and the counts restart at 0 on each run. It is for the operator to watch. Nothing reads it back.

**Runbook** (run on Server 2, where `rag_state.db` lives):

1. Snapshot the pair: run `bash scripts/sqlite-qdrant-snapshot.sh` directly. It takes both locks itself (exit 75 if a NAS run or the intake worker holds one), then takes a pair unconditionally.
   `RAG_FORCE_SNAPSHOT` is read only by `scripts/ragnart-ingest-run.sh`, which also runs the sync, so this step does not use it.
2. Keep `ragnart-ingest.timer` and `ragnart-intake.timer` stopped, and do not run any sync, intake worker or run
   script, from the code change until step 6 completes.
   `sudo systemctl stop ragnart-ingest.timer ragnart-ingest.service ragnart-intake.timer ragnart-intake.service`
   Why: the new scanner keys Claude transcripts `claude-session://`. A sync before the backfill would see
   the old `.jsonl`-keyed rows as deleted and queue new keys, which re-ingests the sessions and duplicates
   their points.
3. Dry-run, and check the report. The `claude_journal_skip` count should be small.
   `python utils/rag_metadata_backfill.py --dry-run`
4. Pilot, one prefix at a time, checking the result after each. Examples:
   `python utils/rag_metadata_backfill.py --apply --prefix /mnt/nfs/Florian/Gin-AI/projects/RAGNART/`,
   `python utils/rag_metadata_backfill.py --apply --prefix /home/florian/.claude/projects/-mnt-nfs-Florian-Gin-AI-projects-RAGNART/`,
   `python utils/rag_metadata_backfill.py --apply --prefix opencode://ses_1`.
5. Full run, as a transient user unit so it survives the shell:
   `systemd-run --user --unit=ragnart-metadata-backfill --working-directory=/mnt/nfs/Florian/Gin-AI/projects/RAGNART ~/Gin-AI/.Gin-AI-python-3.12/bin/python utils/rag_metadata_backfill.py --apply`
   Follow it with `journalctl --user -u ragnart-metadata-backfill.service -f`, and read its state with
   `systemctl --user show -p ActiveState --value ragnart-metadata-backfill.service`.
6. Restart the timers only after a run that exited 0:
   `sudo systemctl start ragnart-ingest.timer ragnart-intake.timer`.
   After a run that exited 1, a Claude row that remains NULL is still old-keyed, and a timer started
   now would sync it as deleted. Re-run the backfill first.
   A stalled or failed run is diagnosed in `docs/RAG-troubleshoot.md` §2.14.

Checks: point count in `documents` before and after, and the payload of a sample of points.

### 5.8 Reconciliation — `ragnart/sync_reconcile.py`

Reconciliation makes the ingestion side's rows and the hub's items agree. It runs in every NAS run, after the
scan and before the submit, once per source (`nas`, `claude-linux-shared-nfs`, `opencode-k11`), and the Mac
runner does the same for its sources. It reads the hub's listing of the source's items, `GET /v1/items` (per
key: content hash, chunk count, pipeline version, owner, project id), and `GET /v1/info`. It never touches
Qdrant. A source whose listing cannot be read is skipped for that run. The listing of `nas` takes about 9 s
(8,703 items), `claude-linux-shared-nfs` about 5 s, `opencode-k11` under 1 s.

| Rule | Meaning | Action |
|---|---|---|
| (a) orphan | A hub item with no local row of any status | A `delete` request. Skipped when the local state holds no completed row for the source, and when the orphans exceed `rag.sync.max_delete_fraction` of the source's own completed rows (a WARNING names the count) |
| (b) missing | A completed row whose item is still present, and the hub lists no points for it | The row is set `pending` and the submit sends it again. A row with `chunk_count` 0 is skipped (it has no points to list); a NULL `chunk_count` is eligible |
| (c) older version | A completed present row whose hub `pipeline_version` is older than `GET /v1/info`'s | Set `pending`; at most `rag.nas_ingest.resubmit_batch_size` (200) per run |
| (d) drift | A completed row whose hub `owner` or `project_id` differs from the local value | A `change_metadata` request with the local value |

- **Dry run.** `python utils/rag_sync_nfs.py --dry-run` reads the live hub (listing, info, statuses), counts
  each rule (`orphan_would_delete`, `orphan_guard_tripped`, `orphan_skipped_no_completed_rows`,
  `missing_would_resubmit`, `version_would_resubmit`, `version_deferred`, `drift_would_fix`,
  `inconsistent_items`) and the listing time per source, sends nothing, writes nothing and takes no lock. A
  dry run on a settled corpus shows zero for each.
- **Rebuild.** `python utils/rag_sync_nfs.py --rebuild-from-hub [--state-db <path>]` repopulates an empty
  `rag_state.db` from the listings: a NAS file whose md5 equals the hub's hash is restored as completed; other
  files are queued by the next scan; a Claude Code session is restored when its hub hash equals the md5 of the
  rendered text or of the `.jsonl` bytes; OpenCode sessions are not restored and are submitted again (about
  433 sessions, 14,000 points). It refuses unless the state database has no rows, and refuses when a crawl
  root is not a directory. On Server 2 it takes about 10 minutes and about 8 GB of memory (it hashes every NAS
  file). `--state-db` writes a scratch file for a check.
- **Exit codes.** The run's exit code is the sync's: `0`, `1` (a guard refused a deletion, or the hub could
  not be read), `75` (the NAS lock is held).

---

## 6. Retrieval

### 6.1 Pipeline — `search_rag()` (`ragnart/search.py`)

```
query
  ├─ bge-m3 dense (Ollama)       → Qdrant prefetch, dense_fetch = 30  ─┐
  └─ BM25 sparse (fastembed)     → Qdrant prefetch, sparse_fetch = 30 ─┤  same filter on both
                                                                        ▼
                                                  RRF fusion (Qdrant) → candidate pool
                                                                        ▼
                                   cross-encoder rerank on child `text` (bge-reranker-v2-m3)
                                                                        ▼
                                   collapse identical copies by content_hash (each hit: locations)
                                                                        ▼
                                   per-source cap (max_hits_per_source)
                                                                        ▼
                                                               first top_k results
```

1. Embed the query (dense; plus sparse in hybrid mode).
2. Filter: `owner IN [user, "la-familia"]` when `user` is given; `file_type IN file_types`
   when given; `project_id IN <the projects selected by scope>` (§6.4) on every query, unless
   `scope` includes `all`, which applies no project filter.
3. Query Qdrant — hybrid (two prefetches fused with `FusionQuery(fusion=Fusion.RRF)`) when
   hybrid is enabled and the collection has `sparse_text`; dense-only otherwise.
4. Rerank the pool when reranking is enabled.
5. Collapse identical copies. Hits with the same `content_hash` from different `source_path` values
   become one hit. The kept hit is the best-ranked copy, because collapse runs after rerank. Its
   `locations` lists its own path, then the paths of the copies it absorbed. Chunks of one file share
   a hash, but they are not copies: each is kept. A NULL hash or the empty-file hash never collapses.
6. Apply the per-source cap (`max_hits_per_source`, default 3): at most that many hits per `source_path`,
   counted after collapse.
7. Return the first `top_k` results.

The order is rerank, then collapse, then cap, then `top_k`. A collapsed copy therefore never uses up a
cap slot, and the `top_k` cut is taken from the capped list.

Pool sizes:

| Mode | Rerank on | Rerank off |
|---|---|---|
| Hybrid | `min(dense_fetch + sparse_fetch, fetch_cap)` = 30 | `min(dense_fetch + sparse_fetch, 50)` = 50 |
| Dense-only | `min(top_k × fetch_multiplier, fetch_cap)` = `min(6·top_k, 30)` | `min(3·top_k, 50)` |

Defaults (`ragnart.yaml`): hybrid on, rerank on. `rag.hybrid.rrf_k` is not read by the code;
Qdrant applies its built-in RRF constant (fused scores such as `0.625 = 1/2 + 1/8` match
k = 2).

### 6.2 Scores

| Key | Meaning |
|---|---|
| `score` | hybrid: Qdrant's RRF fusion score; dense-only: cosine similarity (0–1). Rank-only in hybrid mode — not a similarity. |
| `rerank_score` | present only when reranking ran; the cross-encoder's raw logit (unbounded, roughly −11 … +8). Only its order is meaningful; never mix or compare it with `score`. Candidates the reranker returned no score for get `0.0`. |

`score_threshold` applies to the dense-only path only (Qdrant `score_threshold`); in hybrid
mode it is accepted and ignored. Callers that want one ranking key use
`r.get("rerank_score") or r.get("score")`.

### 6.3 Reranker

`ragnart/rerank.py` posts `{"model", "query", "documents": [child texts]}` to
`rag.rerank.server_url` and sorts candidates by `relevance_score`, descending.

- Served by llama-swap on Server 2, port 8000, which routes `/v1/rerank` by the `model`
  field: `rag.rerank.model` must equal the llama-swap model key (`bge-reranker-v2-m3`). The
  backing llama-server runs with `-c 768 -b 768 -ub 768 --reranking --pooling rank`; the client
  does not truncate.
- **Fallback.** Any HTTP error, timeout (`timeout_seconds`, 10 s), parse error, or a
  candidate without `text` returns the pool unchanged in Qdrant order, with one WARN log
  naming the cause: `reranker failed: …` (HTTP error or timeout), `reranker response parse
  failed: …`, or `rerank: candidate missing … field — skipping reranker entirely`. A query/document pair longer than
  768 tokens makes the server answer HTTP 500, which takes the same path. When llama-swap has
  unloaded the model, the first request waits for it to load and may hit the timeout.
- The search response does not say whether reranking ran; absence of `rerank_score` means it
  did not.

### 6.4 HTTP API — `ragnart/service.py`

**`GET /v1/search`**

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `q` | string | required | query text |
| `scope` | repeated string | required, no default | `all`, `<path>` (the ragnart project at exactly that path), `<path>/` (that project, if any, and every ragnart project below it), or `tag:<key>=<value>` (every ragnart project with that tag). Repeats are unioned. `all` also searches `global` items. Each term is validated, even with `all`. A missing `scope`, an empty term, a malformed tag, or a path or tag that matches no ragnart project is **422**, before any embedding or Qdrant call. The resolved selection is echoed in `scope` |
| `user` | string | none | owner scope (§10); none = search everything |
| `top_k` | int 1–100 | 5 | |
| `file_types` | repeated string | none | e.g. `&file_types=pdf&file_types=claude_chat` |
| `score_threshold` | float 0–1 | 0.0 | dense-only path only |
| `rerank` | bool | config (`true`) | `rerank=false` skips the cross-encoder |
| `max_hits_per_source` | int ≥1 | config (`3`) | per-source cap, applied after collapse, before top_k; echoed in the response |

Response: `{"query", "user", "scope", "max_hits_per_source", "results": [...]}`. `scope` is
`{"all": bool, "global": bool, "projects": [{"id", "path", "name"}]}`: the projects that were searched,
and whether `global` was included. Each result has `content` (parent text), `source_path`, `locations`,
`score`, `file_name`, `file_type`, `session_title` (empty for NAS files), `text` (child chunk) and, when
reranked, `rerank_score`. `locations` lists every `source_path` the hit stands for: its own path, then the
paths of identical copies collapsed into it (§6.1, step 5). When Qdrant is unreachable a search
answers **503** `Qdrant unavailable`. The next search after
Qdrant recovers answers normally, with no service restart. Qdrant errors with a 4xx status are
not mapped to 503. When the queue database could not be opened at service start, a search answers
**503** `Queue database unavailable` until the service is restarted (§5.6).

**`GET /v1/projects`** lists `global` and every ragnart project, with tags and item counts. Each entry
has `id`, `path` (`null` for `global`), `name`, `parent`, `owner`, `mode`, `tags` (an object) and
`item_count` (completed queue items attached to the project). `global` is always listed first; the other
entries follow in `path` order. With `user`, a project is listed when its owner is that user or
`la-familia`; without `user`, every project is listed.

**`GET /health`** → `{"status": "ok" | "degraded", "qdrant": true | false}`. It probes Qdrant's
`/healthz` with a 2 s timeout, so it answers within about 2 s, with `degraded` when Qdrant is down.

```bash
curl 'http://192.168.1.95:8100/v1/search?q=AWS+certifications&user=florian&top_k=5&scope=all'
curl 'http://192.168.1.95:8100/v1/search?q=AWS+certifications&user=florian&rerank=false&scope=all'
curl 'http://192.168.1.95:8100/v1/projects?user=florian'
curl 'http://192.168.1.95:8100/health'
```

Typical latency: ~0.15–0.2 s with `rerank=false`; with reranking, about 0.4–3 s, longer
when the reranker is cold or Ollama is busy with an ingest.

### 6.5 Determinism

Search results are **not** deterministic at the level of order:

- Qdrant's RRF fusion breaks exact score ties inconsistently between calls, so tied results
  can swap rank between two identical requests.
- Reranker scores shift by roughly 1e-3 … 1e-2 depending on which candidates share the
  batch, so near-ties can flip.

Never compare result lists by exact ordered equality. `utils/rag_compare_endpoints.py` checks
score multisets and, within each group of equal scores, the set of `source_path` values.

---

## 7. Configuration reference — `ragnart.yaml`

`ragnart.yaml` sits at the repository root; every tool and the service locate it relative to
their own file, not the working directory. The service and the intake worker read it once at start
— restart `ragnart-search.service` after a change that affects search.

| Key | Value | Read by | Notes |
|---|---|---|---|
| `db_base_path` | `/mnt/nfs/__Backups/RAGNART--databases` | sync, intake worker, status, reset, purge | state DB at `{db_base_path}/sqlite/rag_state.db`. Shell scripts use `RAGNART_DATA_ROOT` (same default) — keep them equal |
| `rag.embedding_model` | `bge-m3` | ingest, search | |
| `rag.ollama_url` | `http://192.168.1.95:11434/api/embeddings` | ingest, search | one key for both paths |
| `rag.opencode_api_url` | `http://192.168.1.95:4096` | — | overwritten at runtime by `opencode.api_url`; keep equal |
| `rag.top_k` | `5` | — | not read; the API default is `top_k=5` |
| `rag.qdrant_collection` | `documents` | `qdrant_status.py` only | all other code uses the constant `documents` |
| `rag.qdrant_url` | `http://192.168.1.93:6333` | all | |
| `rag.ingest_lock` | `/mnt/nfs/__Backups/RAGNART--databases/rag-ingest.lock` | the intake worker and the hub maintenance tools | required, no default; §5.6 |
| `rag.nas_ingest.hub_url` | `http://127.0.0.1:8101` | NAS run | the ingestion API |
| `rag.nas_ingest.lock` | `/mnt/nfs/__Backups/RAGNART--databases/rag-nas-ingest.lock` | NAS run, snapshot | required; §5.6 |
| `rag.nas_ingest.resubmit_batch_size` | `200` | NAS run | most re-submissions of older-version items per run |
| `rag.nas_ingest.secrets.<source-id>` | `/home/florian/.ragnart/secrets/<source-id>` | NAS run | one file per source; mode 0600, never printed |
| `rag.rerank.enabled` | `true` | search | per call: `rerank=false`, `--no-rerank` |
| `rag.rerank.server_url` | `http://192.168.1.95:8000/v1/rerank` | search | llama-swap |
| `rag.rerank.model` | `bge-reranker-v2-m3` | search | must match the llama-swap model key |
| `rag.rerank.fetch_multiplier` | `6` | search | dense-only pool |
| `rag.rerank.fetch_cap` | `30` | search | pool cap when reranking |
| `rag.rerank.timeout_seconds` | `10.0` | search | |
| `rag.hybrid.enabled` | `true` | search | |
| `rag.hybrid.sparse_vector_name` | `sparse_text` | — | not read; the name is the constant in `ragnart/schema.py` |
| `rag.hybrid.dense_fetch` | `30` | search | |
| `rag.hybrid.sparse_fetch` | `30` | search | |
| `rag.hybrid.rrf_k` | `60` | — | not read (§6.1) |
| `rag.search.max_hits_per_source` | `3` | search | default cap; `docs/RAGNART-design.md` §9.2 |
| `rag.admission.max_text_file_size_bytes` | `5000000` | ingest | text-like files above this size are `ignored`; `docs/RAGNART-design.md` §5 |
| `rag.admission.max_chunks_per_item` | `5000` | ingest | items producing more chunks are `ignored`; `docs/RAGNART-design.md` §5 |
| `rag.admission.fffd_ratio_threshold` | `0.005` | ingest | text-like files above this U+FFFD ratio are `ignored`; `docs/RAGNART-design.md` §5 |
| `rag.sync.max_delete_fraction` | `0.10` | sync | mass-delete guard (§5.3) |
| `rag.scanner.include_extensions` | §2.2 | sync | inclusion only; exclusions are `.ragignore` and `config/ragignore` (§2.2) |
| `users.<email>.owner`, `.nfs_roots` | §2.1 | sync | |
| `shared.owner`, `.nfs_roots` | `la-familia`, `/mnt/nfs/La-Familia` | sync | |
| `claude_chats.roots[]` | `{path: /home/florian/.claude/projects, owner: florian}` | sync | |
| `opencode.api_url` | `http://192.168.1.95:4096` | sync, ingest | use the IP, not `localhost` |
| `opencode.owner` | `florian` | sync | constant owner of all OpenCode points |
| `opencode.password_env_file` | `~/.config/opencode/opencode-server.env` | sync, ingest | holds `OPENCODE_SERVER_PASSWORD` |

---

## 8. Operations

All commands run from the repository root in the shared venv:

```bash
source ~/Gin-AI/.Gin-AI-python-3.12/bin/activate
cd /mnt/nfs/Florian/Gin-AI/projects/RAGNART
```

Anything that touches `rag_state.db` runs on **Server 2** (§5.6).

### 8.1 Units and installation

| Unit (host) | Type | Notes |
|---|---|---|
| `ragnart-search.service` (Server 2) | simple, `Restart=on-failure` | `uvicorn ragnart.service:app --host 0.0.0.0 --port 8100`, `User=florian` |
| `ragnart-ingest.timer` (Server 2) | timer | `OnCalendar=*-*-* 01,07,13,19:00:00`, `AccuracySec=10min`, `Persistent=true` (a slot missed while down runs at next boot) |
| `ragnart-ingest.service` (Server 2) | oneshot | runs `scripts/ragnart-ingest-run.sh` as `florian`; `TimeoutStartSec=7200` kills a stuck run (rows it left `processing` keep their request id, and the next run's poll closes them); `SuccessExitStatus=75` |
| `ragnart-opencode-backup.service` (Server 2) | oneshot | runs `scripts/opencode-db-backup.sh` as `florian`; backs up `opencode.db` (§9.6) |
| `ragnart-opencode-backup.timer` (Server 2) | timer | `OnCalendar=*-*-* 03:30:00`, `AccuracySec=10min`, `Persistent=true` (a slot missed while down runs at next boot) |
| `ragnart-ingest-api.service` (Server 2) | simple, `Restart=on-failure` | `uvicorn ragnart.ingest_api:app --host 0.0.0.0 --port 8101`, `User=florian`; separate from search; enabled by hand |
| `ragnart-intake.timer` (Server 2) | timer | `OnCalendar=*:0/10` (every 10 minutes), `Persistent=false` (the next slot drains the queue) |
| `ragnart-intake.service` (Server 2) | oneshot | `python -m ragnart.intake_worker` as `florian`; `SuccessExitStatus=75` (a held index lock is a skipped slot) |
| `qdrant.service` (Server 1) | simple | §4.1 |
| `/etc/logrotate.d/ragnart-ingest` (Server 2) | logrotate | §8.6 |

Install or refresh on Server 2 (idempotent; refuses while an ingestion run is active;
enables the timer and the search service):

```bash
sudo bash /mnt/nfs/Florian/Gin-AI/projects/RAGNART/scripts/install-units.sh
```

`scripts/install-units.sh` copies the units of the hub (`ragnart-ingest-api.service`, `ragnart-intake.service`,
`ragnart-intake.timer`) and enables none of them; `ragnart-ingest.service` is installed with
`SuccessExitStatus=75`.

Qdrant on Server 1 has its own installer, `scripts/qdrant/install-qdrant.sh` (§4.1).

### 8.2 What one NAS ingestion run does — `scripts/ragnart-ingest-run.sh`

1. `rag_sync_nfs.py` with no `--user`: poll the outcome of the earlier run's requests, scan every configured
   root (La-Familia included), the Claude Code transcripts and the OpenCode sessions, plan (§5.3), reconcile
   against the hub's listings (§5.8), and submit to the ingestion API (`http://127.0.0.1:8101`, one secret per
   source).
2. `PRAGMA wal_checkpoint(TRUNCATE)` on `rag_state.db` (non-fatal on failure).
3. The same on `hub_registry.db` when it exists.
4. Snapshot (§9) if the newest `.snapshot` on the NAS is older than `RAG_SNAPSHOT_MIN_AGE_H` hours, if none
   exists, or if `RAG_FORCE_SNAPSHOT=1`. The snapshot step takes the index lock as well as the NAS lock; if
   the intake worker holds the index lock, the snapshot is skipped (exit 75) and logged.

The run submits; it does not parse, embed or write Qdrant. The intake worker (`ragnart-intake.timer`, every 10
minutes, §5.4) applies the requests, so a changed file is searchable after the next intake run following the
submit.

A sync that exits non-zero (a guard in §5.3 tripped, or a crash) is logged as `WARN: sync exited <rc>`. Steps
2 to 4 still run, and the unit ends failed with the first non-zero sync exit code.

The wrapper takes the NAS-ingestion lock before step 1 and holds it until the run ends (§5.6). If another NAS
run holds it, the run exits 75 before any step starts, and systemd counts that as success. The wrapper is the
single entry point for both the timer and manual runs.

| Env variable | Default | Purpose |
|---|---|---|
| `RAG_LOGFILE` | `${RAGNART_DATA_ROOT}/logs/rag-ingest.log` | log file |
| `RAGNART_DATA_ROOT` | `/mnt/nfs/__Backups/RAGNART--databases` | data root |
| `RAG_SNAPSHOT_MIN_AGE_H` | `20` | minimum age of the newest NAS snapshot before a new one is taken |
| `RAG_FORCE_SNAPSHOT` | unset | `1` = snapshot on this run regardless of age |

The run takes no worker or batch setting; the intake worker's `--workers` is the nearest setting (§5.5).

The run's stdout/stderr also go to the journal (`journalctl -u ragnart-ingest.service`). The intake
worker logs only to its journal (`journalctl -u ragnart-intake.service`).

### 8.3 Status and monitoring

```bash
systemctl list-timers ragnart-ingest.timer           # last and next run
systemctl show -p ActiveState ragnart-ingest.service # "activating" = a run (or its snapshot) is in progress
journalctl -u ragnart-ingest.service -f              # live output of the current run
tail -f /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log

python utils/rag_status.py                           # queue counts by status + Qdrant point count
python utils/rag_status.py --user florian            # one owner
python utils/rag_status.py --ignored                 # ignored rows with retry count and skip_reason
python utils/rag_status.py --list-pending [N]        # pending paths, one per line
python utils/rag_status.py --watch /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log
                                                     # live: per-file chunk progress, rate, ETA

ragnart status --ignored                             # queue and point counts, ignored rows with skip_reason
ragnart ls-files --ignored [<path>...]               # on-disk files excluded by a .ragignore or config/ragignore rule, optionally under <path>
ragnart check-ignore -v <path>                       # file and line of the rule that excludes a path

python utils/qdrant_status.py [--watch [SECS]] [--verbose]   # collection status, optimizer lag, segments
curl -s http://192.168.1.93:6333/collections/documents | python3 -m json.tool
curl -s http://192.168.1.95:8100/health
```

`systemctl is-active` reports a running oneshot as inactive; use `ActiveState` as above.

### 8.4 Manual and debug runs

Manual run, same code path as the timer:

```bash
bash scripts/ragnart-ingest-run.sh
RAG_FORCE_SNAPSHOT=1 bash scripts/ragnart-ingest-run.sh        # force today's snapshot pair
```

Run the steps by hand:

```bash
sudo systemctl stop ragnart-ingest.timer ragnart-intake.timer   # no automatic start while debugging
sudo systemctl stop ragnart-ingest.service ragnart-intake.service

python utils/rag_sync_nfs.py --dry-run        # read the hub, print the plan and counts, send nothing
python utils/rag_sync_nfs.py --user florian   # idempotent; submits
python -m ragnart.intake_worker --workers 1   # apply the waiting requests, gentler on Ollama

sudo systemctl start ragnart-ingest.timer ragnart-intake.timer
```

The NAS run resumes where a killed run stopped: rows left in `processing` are closed by the next poll, and a
request the hub does not know is marked `failed` and sent again. The intake worker resets a request that a
crashed worker left `processing` to `received`, and it exits 75 when the index lock is held. To embed with a
different Ollama (e.g. Server 1's CPU), change `rag.ollama_url` and run the intake worker with `--workers 1`.
That Ollama must serve the same `bge-m3` with
`OLLAMA_CONTEXT_LENGTH=2048` (§3.2), and search uses the same key.

### 8.5 Maintenance tasks

**Retry ignored files.** `ignored` rows are not retried while the file is unchanged, and
they have no Qdrant points, so no cleanup is needed. After fixing the cause (e.g. a timed-out
upload — `httpx.ReadTimeout` leaves an empty `skip_reason`), reset
them on Server 2 and run ingestion:

```bash
python utils/rag_status.py --ignored
# one file
sqlite3 /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db \
  "UPDATE ingestion_queue SET status='pending', retry_count=0, skip_reason=NULL WHERE file_path='<path>'"
# only those with an empty skip_reason (timeouts)
sqlite3 /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db \
  "UPDATE ingestion_queue SET status='pending', retry_count=0, skip_reason=NULL
   WHERE status='ignored' AND (skip_reason IS NULL OR skip_reason='')"
# all ignored rows
sqlite3 /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db \
  "UPDATE ingestion_queue SET status='pending', retry_count=0, skip_reason=NULL WHERE status='ignored'"

bash scripts/ragnart-ingest-run.sh
```

Files rejected by the quality gate (`binary content: …`) come back as `ignored` again unless
the file itself changes.

**Add or remove an exclusion.** Add a pattern to `config/ragignore` (every root) or to the
`.ragignore` in the directory it governs. Check it with `ragnart check-ignore -v <path>` (§2.2).
The next sync purges or queues the affected files (§5.3).

**Re-ingest everything.** `python utils/rag_reset.py [--user OWNER] [--yes]` resets queue rows
to `pending` (all, or one owner's) and deletes Qdrant points: the owner's points with `--user`,
or the whole collection for a full reset. A full reset refuses when `documents` is a Qdrant
alias: it exits non-zero and deletes nothing. Restore per §9.3 instead. `--dry-run` (either
mode) prints the points and rows it would delete and writes nothing. Never use it to retry a
few files — it re-queues every completed row.

**Find corrupt points.** `utils/rag_purge_corrupt.py` is a read-only report. It scrolls the points of
every source (`--source <id>` limits it to one) and flags sources whose `parent_text` printable ratio is below
`--threshold` (0.85); it reports whether each source is still in the queue (read-only SQLite connection). It
deletes nothing and has no `--confirm` or `--force`; it takes no lock.

```bash
python utils/rag_purge_corrupt.py --user florian
python utils/rag_purge_corrupt.py --save /tmp/corrupt.txt
```

To remove what it finds, fix or exclude the file and let the sync delete it (a `delete` request), or delete
the item's points through the ingestion API of its source.

**Re-check completed rows against the admission rules.** `utils/rag_purge_rescan.py` is a report: it
re-checks `completed` rows against the admission rules in force and against the file on disk (§5.4 /
`docs/RAGNART-design.md` §5.2), and lists the candidates (`--save FILE` writes them as TSV).
`--requeue-transient` lists the `ignored` rows whose reason is a transient 500 or timeout. There is no
`--apply`: to re-queue rows, update them with `sqlite3` as in the retry recipe above, or use
`utils/rag_reset.py`.

**Search tests.**

```bash
python utils/rag_search_cli.py --query "AWS certifications" --user florian [--top-k 10]
    [--file-types pdf claude_chat] [--score-threshold 0.5] [--no-rerank]
python utils/rag_search_cli.py --query "family album" --no-filter    # no owner filter
python utils/rag_smoke_test.py --query "AWS certifications" --user florian --expect "AWS"
python utils/rag_rerank_bench.py --user florian [--mode dense|rerank|both|hybrid|hybrid+rerank] [--top-k N]
python utils/rag_compare_endpoints.py --old-url … --new-url …        # tie-aware result comparison (§6.5)
```

`rag_search_cli.py` and `rag_rerank_bench.py` call `search_rag()` in-process (they need Ollama,
Qdrant and the reranker reachable); `rag_smoke_test.py` calls the HTTP service
(`--server`, default `http://192.168.1.95:8100`). The benchmark queries are in
`utils/rag_bench_queries.txt` (`query | expected path substring`).

### 8.6 Logs and rotation

- `logs/rag-ingest.log` on the NAS receives the wrapper's lines (UTC timestamps) and the sync's and the
  snapshot's output. The intake worker logs to the journal of `ragnart-intake.service`; the API to
  `ragnart-ingest-api.service`.
- `/etc/logrotate.d/ragnart-ingest` on Server 2 (from `scripts/ragnart-ingest-logrotate`):
  daily, 7 rotations, compressed, `copytruncate`, `missingok`, `notifempty`, and
  `su florian florian` — required because the NAS log directory is group-writable and
  logrotate refuses it otherwise.
- `ragnart-search.service` logs to the journal: `journalctl -u ragnart-search.service`.

### 8.7 Moving ingestion to another host

The NAS ingestion (timer, sync, snapshot) and the hub (ingestion API, intake worker) run on one host; the
scripts are on NFS, so only the systemd units are host-local. The ingestion API's URL is
`rag.nas_ingest.hub_url`; the secrets are in `/home/florian/.ragnart/secrets/`. Because `rag_state.db` must have a single
host (§5.6), move all of it together:

```bash
# old host
sudo systemctl disable --now ragnart-ingest.timer ragnart-intake.timer ragnart-ingest-api.service
# new host: install the units, then disable the search service there if it should stay on Server 2
sudo bash /mnt/nfs/Florian/Gin-AI/projects/RAGNART/scripts/install-units.sh
```

The new host needs the NFS mount (NFSv4.1, `local_lock=none`), the shared venv under
`$HOME`, `sqlite3`, network access to Qdrant, Ollama, the reranker and OpenCode, and the
OpenCode password file at `opencode.password_env_file`, plus the secrets directory `/home/florian/.ragnart/secrets/` and the hub units
(`ragnart-ingest-api.service`, `ragnart-intake.service`, `ragnart-intake.timer`). From then on run the state-DB tools
there only.

### 8.8 Sources and secrets

Every source is registered in the hub registry and submits its items through the ingestion API with its own
secret. The registry keeps only the secret's SHA-256.

- **Register.** From the repository root on Server 2:
  `python utils/rag_source_register.py --id <id> --connector filesystem|claude_code --owner <owner>` is a dry run.
  Add `--apply --secret-out <path>` to register under the index lock. The secret is written to `<path>` (mode 0600,
  created exclusively) and is never printed.
- **Rotate.** `python utils/rag_source_register.py --rotate-secret --id <id> --apply --secret-out <path>` gives an
  existing source of any kind a new secret, under the index lock; the old secret stops working at once.
- **Secret files.** The NAS-side sources (`nas`, `claude-linux-shared-nfs`, `opencode-k11`) read theirs from
  `/home/florian/.ragnart/secrets/<id>` (mode 0600, directory 0700), named in `ragnart.yaml` under
  `rag.nas_ingest.secrets`. An endpoint host keeps its secrets in `~/.ragnart/secrets/<id>` (mode 0600). After
  copying a secret file into place, remove the copy written by `--secret-out` with `shred -u`. Do not print,
  display or paste a secret.
- **Endpoint runner.** On the endpoint host:
  `python -m ragnart.endpoint run --config ~/.ragnart/endpoint.yaml [--dry-run] [--grace-hours N] [--source ID]
  [--resubmit-batch-size N] [--rebuild-from-hub]`. The LaunchAgent `com.ragnart.ingest` runs it every 6 hours;
  loading the agent submits nothing until the first interval. The package is installed with
  `pip install --no-deps` into `~/.python-3.12` and does not run from the NFS checkout.
- **Removal.** `python utils/rag_source_remove.py --source <id>` lists one source's items (dry run). With
  `--apply`, under the index lock, it deletes that source's points, intake rows and spool files, and keeps the
  registry row and secret hash. A source that holds ragnart projects (`nas`) is refused unless
  `--allow-projects-source` is given.

---

## 9. Backup, snapshot retention and recovery

### 9.1 Policy

`rag_state.db`, `hub_registry.db` and the Qdrant collection must be restored as a **triple** taken at the
same logical point: the set of `completed` rows must match the points in Qdrant, and the hub registry
must be the copy taken at that point.

| Item | Value |
|---|---|
| When | at the end of an ingestion run, if the newest `.snapshot` on the NAS is older than 20 h — normally the 01:00 run; `RAG_FORCE_SNAPSHOT=1` forces one. No cron job. |
| What | one Qdrant snapshot of `documents` (stored as `documents_new-<id>-<ts>.snapshot`, ~21 GB) + `rag_state-<ts>.db` (~250 MB) + `hub_registry-<ts>.db` (~50 KB), same `<ts>` |
| Where | `/mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/` |
| Retention | 7 triples (about one week, ~150 GB) |
| Server 1 NVMe | `/var/lib/qdrant/snapshots` holds one snapshot only while it is being downloaded; normally empty |
| Recovery point | at most ~24 h old; the next ingestion run re-ingests anything newer |

`<ts>` is Qdrant's creation time in **UTC** (`YYYY-MM-DD-HH-MM-SS`).

`scripts/sqlite-qdrant-snapshot.sh [--keep N]` (default 7; the wrapper passes 7):

1. Safety net 1 — delete any snapshot Qdrant still holds for the collection (API).
2. Safety net 2 — delete stale `*.tmp` partial downloads on the NAS.
3. Safety net 3 — abort before creating anything if the NAS has less than 60 GB free.
4. `PRAGMA wal_checkpoint(TRUNCATE)` on `rag_state.db` and on `hub_registry.db`.
5. `POST /collections/documents/snapshots` — Qdrant writes the archive to its local
   `snapshots_path`.
6. Safety net 4 — an EXIT trap deletes the Qdrant-local snapshot and the partial download on
   any failure from here on.
7. Download to `<name>.tmp` on the NAS (`curl --max-time 7200`), check it is non-empty,
   rename to `<name>`.
8. `DELETE` the snapshot from Qdrant.
9. Copy `rag_state.db` to `rag_state-<ts>.db`, and `hub_registry.db` to `hub_registry-<ts>.db`, next to it.
10. Safety net 5 — keep the newest N `.snapshot` files and delete `rag_state-*.db` and
    `hub_registry-*.db` copies whose snapshot is gone.

The triple is consistent for Qdrant and `rag_state.db` because the snapshot step holds both the index lock
and the NAS-ingestion lock: neither the intake worker nor a NAS run writes while it runs, and the search
service never writes. The ingestion API does not take either lock and can write `hub_registry.db` (the intake
table, published project records) during the snapshot, so the registry copy carries no such guarantee.
Nothing in the index changes between steps 5 and 9 because the API never writes Qdrant. Download time depends on Server 1's link (a few minutes on gigabit;
slow downloads point at that link).

### 9.2 Choosing a triple

```bash
ls -lt /mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/
```

Pick the newest `<ts>` (UTC) at or before the point to restore to; use the `.snapshot`, the
`rag_state-<ts>.db` and the `hub_registry-<ts>.db` with that same `<ts>`.

Inspect a copy with `immutable=1`, for example `sqlite3 'file:<path>?immutable=1'`. Each copy is
checkpointed before it is taken, so it has no WAL file, and `immutable=1` reads it exactly and creates
no `-shm` or `-wal` file beside it (a `mode=ro` read does create them). Do not use `immutable=1` on the
live `rag_state.db` or `hub_registry.db`: both run in WAL mode, so `immutable=1` ignores changes still in
the WAL and shows stale data.

### 9.3 Full restore (Qdrant storage lost or corrupt)

Restores both halves from one pair. Needs shell access to both servers.

**1. Stop everything that writes or creates the collection (Server 2).** The search service
calls `ensure_collection()` on every query and would create an empty real `documents`
collection on a fresh Qdrant.

```bash
sudo systemctl stop ragnart-ingest.timer ragnart-intake.timer ragnart-ingest.service ragnart-intake.service ragnart-ingest-api.service ragnart-search.service
systemctl show -p ActiveState ragnart-ingest.service    # must be "inactive"
```

**2. Reset Qdrant storage (Server 1).**

```bash
sudo systemctl stop qdrant
sudo rm -rf /var/lib/qdrant/storage
sudo install -d -o florian -g florian /var/lib/qdrant/storage
sudo systemctl start qdrant
for i in $(seq 60); do curl -sf http://192.168.1.93:6333/healthz && break; sleep 2; done
```

**3. Recover the collection from the snapshot (Server 1).** Copy the snapshot to Qdrant's
local snapshot directory, recover it into `documents_new`, and recreate the alias:

```bash
SNAP=documents_new-<id>-<ts>.snapshot
cp /mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/$SNAP /var/lib/qdrant/snapshots/

curl -X PUT 'http://192.168.1.93:6333/collections/documents_new/snapshots/recover' \
  -H 'Content-Type: application/json' \
  -d "{\"location\": \"file:///var/lib/qdrant/snapshots/$SNAP\"}"

curl -X POST 'http://192.168.1.93:6333/collections/aliases' \
  -H 'Content-Type: application/json' \
  -d '{"actions": [{"create_alias": {"collection_name": "documents_new", "alias_name": "documents"}}]}'

watch -n 5 'curl -sf http://192.168.1.93:6333/collections/documents | python3 -m json.tool | grep -E "\"status\"|points_count|optimizer_status"'
rm /var/lib/qdrant/snapshots/$SNAP
```

Wait for `status: green` and `optimizer_status: ok`. If a real collection named `documents`
exists (something called `ensure_collection()` in between), delete it
(`curl -X DELETE http://192.168.1.93:6333/collections/documents`) before creating the alias.

**4. Restore the state DB and the hub registry from the same triple (Server 2).**

```bash
cp /mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/rag_state-<ts>.db \
   /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db
cp /mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/hub_registry-<ts>.db \
   /mnt/nfs/__Backups/RAGNART--databases/sqlite/hub_registry.db
rm -f /mnt/nfs/__Backups/RAGNART--databases/sqlite/{rag_state,hub_registry}.db-{wal,shm}
```

**5. Verify (Server 2).**

```bash
sudo systemctl start ragnart-search.service
python utils/rag_status.py                      # completed count vs Qdrant point count
python utils/rag_sparse_migrate.py status       # alias -> documents_new, sparse space present
python utils/rag_smoke_test.py --query "AWS certifications" --user florian --expect "AWS"
curl -s 'http://192.168.1.95:8100/v1/projects'   # the listing must match the restored hub registry
```

**6. Catch up and re-enable (Server 2).** Start the API and the search service, then run the NAS
ingestion: a sync re-queues everything that changed after `<ts>` (newer mtime, or not in the restored queue)
and removes what was deleted after `<ts>`, and the reconciliation sends again the items the hub lists
differently from the restored rows. The intake worker applies the requests with delete-before-insert.

```bash
sudo systemctl start ragnart-ingest-api.service ragnart-search.service
bash scripts/ragnart-ingest-run.sh          # sync, submit the gap
python -m ragnart.intake_worker             # or wait for ragnart-intake.timer
sudo systemctl start ragnart-ingest.timer ragnart-intake.timer
systemctl list-timers ragnart-ingest.timer
```

### 9.4 State-DB-only restore (Qdrant healthy)

Use when `rag_state.db` or `hub_registry.db` is corrupt but Qdrant is intact. All steps on Server 2.

```bash
sudo systemctl stop ragnart-ingest.timer ragnart-intake.timer ragnart-ingest.service ragnart-intake.service ragnart-ingest-api.service

cp /mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/rag_state-<ts>.db \
   /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db
cp /mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/hub_registry-<ts>.db \
   /mnt/nfs/__Backups/RAGNART--databases/sqlite/hub_registry.db
rm -f /mnt/nfs/__Backups/RAGNART--databases/sqlite/{rag_state,hub_registry}.db-{wal,shm}

sudo systemctl start ragnart-ingest-api.service
bash scripts/ragnart-ingest-run.sh          # sync, submit the gap
python -m ragnart.intake_worker             # or wait for ragnart-intake.timer
sudo systemctl start ragnart-ingest.timer ragnart-intake.timer
```

The sync decides by mtime and by presence, and the reconciliation by the hub's listing: files changed or
added after `<ts>` are re-queued and re-ingested (the intake worker removes their existing points first);
files deleted after `<ts>` still have rows, are found gone and tombstoned, and the purge deletes their points
after the grace period; a hub item with no row is deleted as an orphan, under the guards of §5.8. Qdrant ends
up with one set of points per source either way. Requests that the restored registry does not know (intake
rows after `<ts>`) are lost with it; the ingestion side's poll marks their rows `failed` and sends them
again. Endpoint sources (the Mac) send again the items the restored registry's listing lacks, at their next
run. A lost `rag_state.db` with no copy can be rebuilt from the hub's listings:
`python utils/rag_sync_nfs.py --rebuild-from-hub` (§5.8).

The spool (`RAGNART--databases/spool/`) is transient: it holds uploaded bytes until their request is completed or
ignored, and it is not part of the restore triple. After a restore, a spool file that no intake row names is an
orphan and may be deleted by hand.

### 9.5 On-demand snapshot

```bash
RAG_FORCE_SNAPSHOT=1 bash scripts/ragnart-ingest-run.sh   # preferred: runs after the sync
bash scripts/sqlite-qdrant-snapshot.sh                    # direct; takes both locks itself, exits 75 while a NAS run or the intake worker holds one
```

Each extra triple counts toward the 7 kept, so forcing snapshots shortens the history.

### 9.6 OpenCode database backup

`scripts/opencode-db-backup.sh` takes a nightly backup of the local OpenCode database
(`opencode.db`; default `/home/florian/.local/share/opencode/opencode.db`, overridable with
`OPENCODE_DB_PATH`). `ragnart-opencode-backup.timer` starts `ragnart-opencode-backup.service`
(§8.1). Both are installed on Server 2 by `scripts/install-units.sh`.

| Item | Value |
|---|---|
| When | 03:30 local time daily (`OnCalendar=*-*-* 03:30:00`); `Persistent=true`, so a slot missed while the host was down runs at the next boot |
| Method | `sqlite3 .backup` (SQLite's online backup API); OpenCode keeps writing to the live database during the run |
| Where | `RAGNART--databases/opencode-backups/<hostname>-<ip>/opencode-<YYYY-MM-DD>.db` under the data root (§1.3); the date is UTC |
| Write | a temp file `.opencode-<YYYY-MM-DD>.db.tmp` in the same directory, renamed to the final name only when non-empty; an empty result is an error and is removed. A stale temp file from a crashed run is deleted first |
| File mode | 0600 (`umask 077`): the copies hold private session data |
| Retention | the newest 7 `opencode-*.db` in each `<hostname>-<ip>` directory; older copies are deleted |
| Index writer | no. It does not take `rag.ingest_lock` (§5.6): it reads OpenCode's database and writes only its own backup directory |
| Duration | a 2.2 GiB `opencode.db` backs up in about 21 s |

---

## 10. Access and multi-tenancy

What is implemented:

- **Ownership at ingestion.** NAS files take the owner of the configured root they were
  found under: `scan_nfs_roots()` in `ragnart/scanner.py` takes the `owner` literal of
  each `users.*` / `shared` root in `ragnart.yaml`. (`derive_owner()` in `ragnart/schema.py`
  is defined but not called.) Claude Code sessions take `claude_chats.roots[].owner`.
  OpenCode sessions all take the constant `opencode.owner`.
- **Ownership at search.** `GET /v1/search?user=<owner>` restricts results to
  `owner IN [<owner>, "la-familia"]`. Without `user`, nothing is filtered and the whole
  collection is searched.
- **No authentication.** The service listens on `0.0.0.0:8100` with no token; the `user`
  parameter is trusted as given. Any client on the LAN can search as any owner, or as nobody.
  The `users:` keys in `ragnart.yaml` are email labels only.
- **OpenCode is single-tenant.** One OpenCode server with one constant owner; per-user
  isolation of OpenCode content would need an owner per session or per server, which does not
  exist.
- **Coverage.** Only `florian` is an active user. The scheduled sync scans every configured root,
  so `la-familia` content is indexed like any other owner's (§2.1, §5.3).
- **Clients.** Clients call `GET /v1/search` directly and manage their own retrieval loops.
  SoHoAI's gateway on Server 1 answers its own `GET /v1/rag/search` with a 307 redirect to
  this service.
