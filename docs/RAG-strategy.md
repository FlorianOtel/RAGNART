---
title: "RAGNART — RAG strategy"
created_at: 2026-03-30--00-00
created_by: Florian Otel
updated_by: Claude Code (Claude Opus 5); Claude Code (Claude Opus 5.5, 2026-11-04 -- OpenCode 2.x ingestion, scan-failure deletion bug, venv exclusions, orchestrator ingest endpoints removed, state DB single-host rule); Claude Code (Claude Haiku 5.5, 2026-11-13 -- RAGNART v0.1 move: name and path corrections only, content unchanged); Claude Code (Claude Haiku 5.5, /brain pipeline -- v0.1 post-review doc fixes); Claude Code (Claude Opus 5.5, 2026-11-13 -- post-v0.1 ops: Qdrant local config, daily snapshot retention, data root RAGNART--databases); Claude Code (Claude Opus 5.5, 2026-11-13 -- rewritten as a current-state reference); Claude Code (Claude Haiku 5.5, 2026-11-14 -- .ragignore inclusion and exclusion, admission, sync guards, rescan tool); Claude Code (Claude Haiku 5.5, 2026-11-14 -- Ollama context length 2048 and rejection of over-length input); Claude Code (Claude Haiku 5.5, 2026-11-14 -- global and per-directory exclusion files paragraph); Claude Code (Claude Haiku 5.5, 2026-11-14 -- ls-files accepts <path> arguments in §8.3); Claude Code (Claude Haiku 5.5)
updated_at: 2026-11-15--19-11
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
| | Ollama, `bge-m3` dense embeddings | `http://192.168.1.95:11434/api/embeddings` |
| | Reranker `bge-reranker-v2-m3` behind llama-swap | `http://192.168.1.95:8000/v1/rerank` |
| | OpenCode 2.x server (`opencode-v2-server.service`) — a corpus source | `http://192.168.1.95:4096` |
| | The only host that opens `rag_state.db` (§5.6 below) | — |
| **NAS** — 192.168.1.96 | NFSv4.1 export `/volume1/Disks`, mounted at `/mnt/nfs` on both servers with `local_lock=none` | — |

The repository itself lives on NFS (`/mnt/nfs/Florian/Gin-AI/projects/RAGNART`), so both
servers run the same code. The Python environment is the shared venv
`~/Gin-AI/.Gin-AI-python-3.12` (qdrant-client 1.17.1, fastembed 0.8.0, docling 2.89.0).

### 1.2 Data flow

```
                     ingestion (Server 2, ragnart-ingest.timer, 4x/day)
 NAS files ──────┐
 Claude Code     ├─▶ rag_sync_nfs.py ──▶ rag_state.db (queue) ──▶ rag_ingest_daemon.py
 .jsonl sessions │     (scan, diff,        SQLite on NAS            parse → chunk →
 OpenCode API ───┘      delete gone)                                bge-m3 dense (Ollama)
                                                                    + BM25 sparse (fastembed)
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
├── rag-ingest.lock                     ← NFS lock held by the running index writer; holds the holder's line (§5.6)
├── sqlite/
│   └── rag_state.db                    ← ingestion queue (SQLite, WAL mode)
├── logs/
│   ├── rag-ingest.log                  ← ingestion + snapshot log (rotated daily, 7 kept)
│   └── rag-ingest.log.N.gz
└── qdrant-snapshots/
    └── documents/
        ├── documents_new-<id>-<YYYY-MM-DD-HH-MM-SS>.snapshot   ← Qdrant archive (~21 GB)
        └── rag_state-<YYYY-MM-DD-HH-MM-SS>.db                  ← matching state DB (~250 MB)
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
| `shared` | `la-familia` | `/mnt/nfs/La-Familia` | configured; scanned only by a sync run without `--user` (§5.3) |
| `users` entries for `eva`, `annika`, `laura` | — | `/mnt/nfs/Eva`, `/mnt/nfs/Annika`, `/mnt/nfs/Laura` | commented out |

To add a user, uncomment (or add) a `users:` block with `owner` and `nfs_roots`, and add the
owner to `RAG_SYNC_USERS` in `scripts/ragnart-ingest-run.sh` (§8.2). The email key is only a
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
  `*._*`) also matches a directory whose name fits it, and excludes that directory's contents.
- An excluded name does not cover a sibling with a longer name (`.Gin-AI-python-3.12/` does
  not match `.Gin-AI-python-3.12.old`). `site-packages/`, `.uv-python/` and `.cache/`
  exclude what makes a directory unwanted, whatever the venv is called.
- `.claude/` and `dot.claude-Linux/` (the real directory behind the `~/.claude` symlink) keep
  Claude Code's internal state out of the generic scan; session transcripts come in through
  `claude_chats` instead (§2.4).
- `**/Microsoft--flotel/Documents/` holds IRM/DRM-encrypted presentations that no parser can read.

To change a rule, edit `config/ragignore` (every root) or the `.ragignore` in the directory the
rule governs. The next sync applies it. A newly excluded file counts as gone: the sync deletes
its Qdrant points and its queue row (§5.3). No code change or manual Qdrant cleanup is needed.

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
`.jsonl` (main sessions and `<uuid>/subagents/agent-*.jsonl`), dedups by real path, and
applies none of the `rag.scanner` rules.

`_parse_claude_chat()` in `ragnart/ingest.py` turns a transcript into text:

- keeps user and assistant text blocks; skips meta entries, `tool_use`, `tool_result`,
  thinking blocks, XML-tagged command strings and file-history snapshots;
- prefixes a header (`Session`, `Project: <basename of cwd> (<cwd>)`, `Date`, `Model`,
  `Branch`, `Version`);
- sets payload fields `session_id`, `project` (basename of the session's cwd) and
  `session_title`. The title comes from the human-named symlinks in `~/.claude/chats/`
  (read-only, never ingested; UUID-named links are ignored); without one, it is the first
  real user message, cut to 60 characters. Subagent transcripts get the parent session's
  title plus ` [subagent]`.

Points get `file_type = "claude_chat"`, and `file_name` is set to the session title.

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
  endpoint). `embed_batch()` runs up to `--batch` requests concurrently per file and calls a
  progress callback every 50 completions, which logs `Embedding progress: N/M <file>` (read by
  `rag_status.py --watch`).
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
| Payload index | `source_path` (keyword) — the only indexed field |
| HNSW | `m 16`, `ef_construct 100`, in RAM |
| Payload storage | on disk (`on_disk_payload: true`) |

`ensure_collection()` runs at search-service startup, at daemon startup and on every search
and ingest call. If neither a collection nor an alias named `documents` exists, it creates a
real `documents` collection with the dense and sparse spaces above. It also creates the
`source_path` keyword index if missing — every per-file delete filters on that field, and
without the index such deletes scan the whole collection and time out.

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
| `source_path` | NFS path, or `opencode://<id>` — the provenance returned to clients | all |
| `file_name` | file name; session title for `claude_chat` and `opencode` | all |
| `file_type` | extension, `claude_chat` or `opencode` | all |
| `page` | always `0` (no page tracking) | all |
| `chunk_index` | position in the document's chunk list | all |
| `tag` | always `""` (the daemon passes no tag) | all |
| `session_id` | session UUID / OpenCode id | `claude_chat`, `opencode` |
| `project` | basename of the session's cwd | `claude_chat` only |
| `session_title` | human-readable session title | `claude_chat`, `opencode` |

Point IDs are random UUIDs; per-file delete-before-insert (§5.4) makes deterministic IDs
unnecessary.

---

## 5. Ingestion pipeline and state DB

### 5.1 `rag_state.db` — the ingestion queue

`/mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db`, SQLite in WAL mode, managed by
`ragnart/state.py` (`StateDB`). One table:

```sql
CREATE TABLE ingestion_queue (
    file_path       TEXT PRIMARY KEY,      -- NFS path, .jsonl path, or opencode://<id>
    owner           TEXT NOT NULL,
    last_modified   REAL NOT NULL,         -- mtime (or OpenCode time.updated) at discovery
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending | processing | completed | ignored
    error_msg       TEXT,                  -- last error while still retrying
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 5,
    started_at      TEXT,                  -- ISO UTC, set on → processing
    completed_at    TEXT,                  -- ISO UTC, set on → completed
    progress_detail TEXT,                  -- starting | parsing | chunking (N chunks) | embedding i/N | upserting
    skip_reason     TEXT                   -- why the row is ignored
);
```

The `StateDB()` constructor writes (`PRAGMA journal_mode=WAL`, `CREATE TABLE IF NOT EXISTS`,
column migrations, commit) — opening the DB with it is never read-only.

### 5.2 Status transitions

| From → to | Trigger |
|---|---|
| (new) → `pending` | sync discovers a path not in the table |
| `pending` → `processing` | daemon picks the row (`started_at`, `progress_detail = starting`) |
| `processing` → `completed` | all steps succeeded, or the document was empty / produced no chunks |
| `processing` → `pending` | a step failed and `retry_count + 1 < max_retries` (`error_msg` set) |
| `processing` → `ignored` | a step failed on the last retry (`skip_reason` = last error), or the quality gate rejected the text (§2.6) |
| any → `pending` | sync sees `last_modified` on disk newer than the stored value (clears retries, errors, `skip_reason`) |
| `processing` → `pending` | daemon startup (`crash_recovery()`) resets rows left in `processing` by a killed run |

`ignored` rows are never retried while the file is unchanged. A `failed` status is not set by
any code path; if set by hand, the next sync resets it to `pending` with zero retries.

### 5.3 Sync — `utils/rag_sync_nfs.py`

One run reconciles the queue with the sources:

1. Runs the three scanners (§2), each calling `discover_or_update()` per path: new → `pending`,
   newer mtime → `pending`, otherwise no change. The NAS scan also reports its skipped roots.
2. Builds the set of paths seen in this run (plus all queued `opencode://` rows if the
   OpenCode API could not be scanned) and calls `find_deleted()`, limited to the owners whose
   sources were scanned. A queue row of those owners that is not in the set is gone: deleted
   from disk, excluded by a `.ragignore` or `config/ragignore` rule, or with an extension
   outside `include_extensions`. Each gone path is logged with its reason.
3. Runs the guards below. If one trips, steps 4 and 5 do not run.
4. For gone rows in `completed`, deletes their Qdrant points by `source_path` filter with
   `wait=True`: the call returns only after Qdrant has applied the delete, so no points are left
   without a queue row to retry them. Any exception aborts the run.
5. Only then removes all gone rows from SQLite (`purge_deleted()`). A run killed between 4
   and 5 leaves the rows in place, and the next sync repeats the deletes.

**Sync guards.** Two guards skip the whole deletion phase (steps 2 to 5 for deletion; the
queueing of step 1 still stands). Each logs an ERROR and the run exits with code 1:

- **Skipped roots.** An NFS root that is not a directory, or that is empty while the queue holds
  rows under it, counts as unmounted. If any in-scope root is skipped, nothing is deleted.
- **Mass delete.** If the completed rows to delete exceed `rag.sync.max_delete_fraction`
  (`ragnart.yaml`, `0.10`) of all completed rows, nothing is deleted. `--allow-mass-delete`
  proceeds anyway and logs a WARNING with the counts.

Diagnosis and recovery for both guards are in `docs/RAG-troubleshoot.md` §2.12.

`--user OWNER` limits the scan to that owner's NFS roots and `claude_chats` roots (and
OpenCode if `opencode.owner` matches), and limits deletion to that owner's rows: a `--user`
run never deletes another owner's rows, including `la-familia`'s. Without `--user` the deletion
check covers every owner. The scheduled run uses `--user florian` (§8.2).

### 5.4 Worker loop — `ingest_file()`

`utils/rag_ingest_daemon.py` fetches pending rows 10 at a time (oldest first) and runs
`ingest_file()` on up to `--workers` of them concurrently, until no row is `pending`. Per file:

| Step | Action |
|---|---|
| 0 | `ensure_collection()`; delete every Qdrant point with this `source_path` (synchronous) |
| 1 | `mark_processing()` |
| 2 | admission checks for text-like files (size cap, NUL sniff, U+FFFD ratio; `ignored` with a reason, `docs/RAGNART-design.md` §5); then parse (§2.6), off the event loop; empty text → `completed`; printable ratio < 0.85 → `ignored` |
| 3 | chunk (§3.1); no chunks → `completed`; more chunks than `rag.admission.max_chunks_per_item` → `ignored` with `too large: N chunks > … chunk cap` |
| 4 | embed child texts with bge-m3, `--batch` requests in flight |
| 5 | build points: random UUID, dense vector (plus BM25 sparse vector when the collection has `sparse_text`), payload (§4.3) |
| 6 | upsert in batches of 256 points (keeps each request under Qdrant's HTTP body limit) |
| 7 | `mark_completed()` |

Any exception → `mark_failed()` (retry or `ignored`, §5.2), and the daemon moves on.

**Idempotency.** Step 0 makes every attempt start from zero points for that file. A retry
after a partial upsert, a re-ingest of a modified file, or a re-run after a crash between
steps 6 and 7 all end with exactly one set of points. A file in `ignored` has no points.

### 5.5 Daemon flags

Both flags are required.

| Flag | Controls | Scheduled value | CPU-embedding value |
|---|---|---|---|
| `--workers N` | files processed concurrently (docling parse overlaps other files' embedding) | 3 | 1 |
| `--batch M` | concurrent Ollama requests per file | 20 | 5 |
| `--log-file PATH` | also log to this file (needed by `rag_status.py --watch`) | `logs/rag-ingest.log` | — |

Lower `--batch` reduces Ollama queue depth: fewer `httpx.ReadTimeout` failures on files with
many chunks, and shorter waits for search queries during an ingest.

### 5.6 Concurrency rules

**One index writer at a time, across hosts.** Every process that writes `rag_state.db` rows or
Qdrant points takes an exclusive lock on `rag.ingest_lock`
(`/mnt/nfs/__Backups/RAGNART--databases/rag-ingest.lock`) before its first write, with
`fcntl.lockf(LOCK_EX | LOCK_NB)` (POSIX record lock, `F_SETLK`; code in `ragnart/lock.py`). With
NFSv4.1 and `local_lock=none` the NAS lock manager arbitrates, so the lock holds across machines.
The kernel releases it when the holder exits. `flock()` is not used: on this NAS a non-blocking
`flock` blocks indefinitely. The key is required in `ragnart.yaml`; a missing key is an error, and
there is no host-local fallback.

Writers that take the lock:

- `utils/rag_sync_nfs.py` (every run)
- `utils/rag_ingest_daemon.py` (every run)
- `utils/rag_purge_rescan.py --apply` (a dry run never takes it)
- `utils/rag_purge_corrupt.py --confirm` (a dry run never takes it)
- `utils/rag_reset.py` without `--dry-run`
- `scripts/sqlite-qdrant-snapshot.sh` (the whole run, standalone or from the run script)
- `scripts/ragnart-ingest-run.sh` (the whole run: sync, daemon, WAL checkpoint, snapshot)

The steps that the run script starts find the lock held by their ancestor and do not take it again.
`utils/rag_sparse_migrate.py` is the one exception: a one-off migration, run with the timer stopped.

The lock is not waited for. A writer that finds it held prints one line to stderr and exits with
status 75 (`EX_TEMPFAIL`):

```
ragnart: another writer holds the ingestion lock (<path>): pid N on HOST, <command>, since <time>
```

The holder writes the same `pid N on HOST, <command>, since <time>` line into the lock file after it
takes the lock. A contender never rewrites the file. The file is not cleared when the holder exits,
so when the lock is free it shows the last holder's line. Its mtime is the last acquisition.

The lock is needed because `fetch_pending_full()` and `mark_processing()` are not one atomic
step; two daemons could pick the same rows and both upsert, leaving duplicate points. The same
reasoning covers the other writers: nothing may change the index under a sync, a purge, a reset or
a snapshot.

| Scenario | Outcome |
|---|---|
| Two writers, any hosts | the second exits 75 at once, names the holder, and writes nothing |
| Timer fires while a run is active | systemd does not start a oneshot that is still running |
| Manual `scripts/ragnart-ingest-run.sh` while the service runs | exits 75 at once, names the holder; no step runs |
| Manual sync or daemon while a run holds the lock | exits 75 at once, names the holder; nothing is written |
| Timer run starts while a manual writer holds the lock | the unit exits 75 and shows as failed; nothing is written, and the next slot runs normally |

**`rag_state.db` is opened from Server 2 only.** In WAL mode, SQLite keeps the WAL index in
`rag_state.db-shm`, memory-mapped as shared memory. Two hosts map it through their own page
caches over NFS with nothing keeping the copies coherent; NFS file locks do not cover that
index. A connection from a second host while Server 2 writes — including a read-only
`sqlite3 "file:…?mode=ro"` — can return wrong results or make the writer fail with
`database disk image is malformed`. Therefore the timer, `rag_sync_nfs.py`, the daemon, the
snapshot script, `rag_status.py`, `rag_reset.py`, `rag_purge_corrupt.py`, `rag_purge_rescan.py` and
ad-hoc `sqlite3` sessions all run on Server 2. Of these, the sync, the daemon, the snapshot script,
the run script, `rag_reset.py` (without `--dry-run`), `rag_purge_corrupt.py --confirm` and
`rag_purge_rescan.py --apply` take the ingestion lock. `rag_status.py`, the dry runs and the
read-only `ragnart` CLI never take it. The search service never opens the state DB.

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
                                                               first top_k results
```

1. Embed the query (dense; plus sparse in hybrid mode).
2. Filter: `owner IN [user, "la-familia"]` when `user` is given; `file_type IN file_types`
   when given; no filter otherwise.
3. Query Qdrant — hybrid (two prefetches fused with `FusionQuery(fusion=Fusion.RRF)`) when
   hybrid is enabled and the collection has `sparse_text`; dense-only otherwise.
4. Rerank the pool when reranking is enabled.
5. Return the first `top_k`.

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
| `user` | string | none | owner scope (§10); none = search everything |
| `top_k` | int 1–100 | 5 | |
| `file_types` | repeated string | none | e.g. `&file_types=pdf&file_types=claude_chat` |
| `score_threshold` | float 0–1 | 0.0 | dense-only path only |
| `rerank` | bool | config (`true`) | `rerank=false` skips the cross-encoder |

Response: `{"query", "user", "results": [...]}`, each result with `content` (parent text),
`source_path`, `score`, `file_name`, `file_type`, `session_title` (empty for NAS files),
`text` (child chunk) and, when reranked, `rerank_score`. When Qdrant is unreachable a search
answers **503** `Qdrant unavailable`. The next search after
Qdrant recovers answers normally, with no service restart. Qdrant errors with a 4xx status are
not mapped to 503.

**`GET /health`** → `{"status": "ok" | "degraded", "qdrant": true | false}`. It probes Qdrant's
`/healthz` with a 2 s timeout, so it answers within about 2 s, with `degraded` when Qdrant is down.

```bash
curl 'http://192.168.1.95:8100/v1/search?q=AWS+certifications&user=florian&top_k=5'
curl 'http://192.168.1.95:8100/v1/search?q=AWS+certifications&user=florian&rerank=false'
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
their own file, not the working directory. The service and the daemon read it once at start
— restart `ragnart-search.service` after a change that affects search.

| Key | Value | Read by | Notes |
|---|---|---|---|
| `db_base_path` | `/mnt/nfs/__Backups/RAGNART--databases` | daemon, sync, status, reset, purge | state DB at `{db_base_path}/sqlite/rag_state.db`. Shell scripts use `RAGNART_DATA_ROOT` (same default) — keep them equal |
| `rag.embedding_model` | `bge-m3` | ingest, search | |
| `rag.ollama_url` | `http://192.168.1.95:11434/api/embeddings` | ingest, search | one key for both paths |
| `rag.opencode_api_url` | `http://192.168.1.95:4096` | — | overwritten at runtime by `opencode.api_url`; keep equal |
| `rag.top_k` | `5` | — | not read; the API default is `top_k=5` |
| `rag.qdrant_collection` | `documents` | `qdrant_status.py` only | all other code uses the constant `documents` |
| `rag.qdrant_url` | `http://192.168.1.93:6333` | all | |
| `rag.ingest_lock` | `/mnt/nfs/__Backups/RAGNART--databases/rag-ingest.lock` | every index writer | required, no default; §5.6 |
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
| `ragnart-ingest.service` (Server 2) | oneshot | runs `scripts/ragnart-ingest-run.sh` as `florian`; `TimeoutStartSec=7200` kills a stuck run (crash recovery resets its rows on the next start) |
| `qdrant.service` (Server 1) | simple | §4.1 |
| `/etc/logrotate.d/ragnart-ingest` (Server 2) | logrotate | §8.6 |

Install or refresh on Server 2 (idempotent; refuses while an ingestion run is active;
enables the timer and the search service):

```bash
sudo bash /mnt/nfs/Florian/Gin-AI/projects/RAGNART/scripts/install-units.sh
```

Qdrant on Server 1 has its own installer, `scripts/qdrant/install-qdrant.sh` (§4.1).

### 8.2 What one ingestion run does — `scripts/ragnart-ingest-run.sh`

1. `rag_sync_nfs.py --user <u>` for each entry of `RAG_SYNC_USERS` (`("florian")`).
2. `rag_ingest_daemon.py --workers $RAG_WORKERS --batch $RAG_BATCH --log-file $LOGFILE`
   (runs under the ingestion lock that the wrapper holds for the whole run, §5.6).
3. `PRAGMA wal_checkpoint(TRUNCATE)` on `rag_state.db` (non-fatal on failure).
4. Snapshot (§9) if the newest `.snapshot` on the NAS is older than
   `RAG_SNAPSHOT_MIN_AGE_H` hours, if none exists, or if `RAG_FORCE_SNAPSHOT=1`.

A sync that exits non-zero (a guard in §5.3 tripped, or a crash) is logged as
`WARN: sync exited <rc>`. Steps 2 to 4 still run, and the unit ends failed with the first
non-zero sync exit code.

The wrapper takes the ingestion lock before step 1 and holds it until the run ends (§5.6). If
another writer holds the lock, the run exits 75 before any step starts. The wrapper is the single
entry point for both the timer and manual runs.

| Env variable | Default | Purpose |
|---|---|---|
| `RAG_WORKERS` | `3` | daemon `--workers` |
| `RAG_BATCH` | `20` | daemon `--batch` |
| `RAG_LOGFILE` | `${RAGNART_DATA_ROOT}/logs/rag-ingest.log` | log file |
| `RAGNART_DATA_ROOT` | `/mnt/nfs/__Backups/RAGNART--databases` | data root |
| `RAG_SNAPSHOT_MIN_AGE_H` | `20` | minimum age of the newest NAS snapshot before a new one is taken |
| `RAG_FORCE_SNAPSHOT` | unset | `1` = snapshot on this run regardless of age |

Daemon stdout/stderr also go to the journal (`journalctl -u ragnart-ingest.service`).

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
RAG_WORKERS=1 RAG_BATCH=5 bash scripts/ragnart-ingest-run.sh   # gentler on Ollama
RAG_FORCE_SNAPSHOT=1 bash scripts/ragnart-ingest-run.sh        # force today's snapshot pair
```

Run the steps by hand:

```bash
sudo systemctl stop ragnart-ingest.timer      # no automatic start while debugging
sudo systemctl stop ragnart-ingest.service    # stop an active run (SIGTERM)

python utils/rag_sync_nfs.py --user florian   # idempotent
python utils/rag_ingest_daemon.py --workers 1 --batch 5 --log-file /tmp/rag-debug.log

sudo systemctl start ragnart-ingest.timer     # re-enable
```

The daemon resumes where a killed run stopped; rows left in `processing` go back to `pending`
on its next start. It exits with `No pending files. Run rag_sync_nfs.py first.` when the
queue is empty.

To embed with a different Ollama (e.g. Server 1's CPU), change `rag.ollama_url` and run with
`RAG_WORKERS=1 RAG_BATCH=5`. That Ollama must serve the same `bge-m3` with
`OLLAMA_CONTEXT_LENGTH=2048` (§3.2), and search uses the same key.

### 8.5 Maintenance tasks

**Retry ignored files.** `ignored` rows are not retried while the file is unchanged, and
they have no Qdrant points, so no cleanup is needed. After fixing the cause (e.g. lowering
`--batch` for files that timed out — `httpx.ReadTimeout` leaves an empty `skip_reason`), reset
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

RAG_BATCH=2 bash scripts/ragnart-ingest-run.sh
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

**Purge corrupt points.** `utils/rag_purge_corrupt.py` scrolls all points and flags sources
whose `parent_text` printable ratio is below `--threshold` (0.85); it reports whether each
source is still in the queue (read-only SQLite connection).

```bash
python utils/rag_purge_corrupt.py --user florian            # dry run (default)
python utils/rag_purge_corrupt.py --save /tmp/corrupt.txt   # write the path list
python utils/rag_purge_corrupt.py --confirm                 # delete; skips files still on disk
python utils/rag_purge_corrupt.py --confirm --force         # delete even if the file exists
```

**Re-check completed rows against the admission rules.** `utils/rag_purge_rescan.py` re-checks
`completed` rows against the admission rules in force and against the file on disk (§5.4).
Dry run by default; `--apply` writes. `--rescan` (default) runs the re-check; `--requeue-transient`
resets `ignored` rows whose reason is a transient 500 or timeout to `pending`. `--save FILE`
writes the candidate list as TSV. Run it after a snapshot pair (§9.5).

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

- `logs/rag-ingest.log` on the NAS receives the wrapper's lines (UTC timestamps), both
  scripts' output and the daemon's log (local-time timestamps).
- `/etc/logrotate.d/ragnart-ingest` on Server 2 (from `scripts/ragnart-ingest-logrotate`):
  daily, 7 rotations, compressed, `copytruncate`, `missingok`, `notifempty`, and
  `su florian florian` — required because the NAS log directory is group-writable and
  logrotate refuses it otherwise.
- `ragnart-search.service` logs to the journal: `journalctl -u ragnart-search.service`.

### 8.7 Moving ingestion to another host

Ingestion (timer, sync, daemon, snapshot) runs as one unit on one host; the scripts are on
NFS, so only the systemd units are host-local. Because `rag_state.db` must have a single
host (§5.6), move all of it together:

```bash
# old host
sudo systemctl disable --now ragnart-ingest.timer
# new host: install the units, then disable the search service there if it should stay on Server 2
sudo bash /mnt/nfs/Florian/Gin-AI/projects/RAGNART/scripts/install-units.sh
```

The new host needs the NFS mount (NFSv4.1, `local_lock=none`), the shared venv under
`$HOME`, `sqlite3`, network access to Qdrant, Ollama, the reranker and OpenCode, and the
OpenCode password file at `opencode.password_env_file`. From then on run the state-DB tools
there only.

---

## 9. Backup, snapshot retention and recovery

### 9.1 Policy

`rag_state.db` and the Qdrant collection must be restored as a **pair** taken at the same
logical point: the set of `completed` rows must match the points in Qdrant.

| Item | Value |
|---|---|
| When | at the end of an ingestion run, if the newest `.snapshot` on the NAS is older than 20 h — normally the 01:00 run; `RAG_FORCE_SNAPSHOT=1` forces one. No cron job. |
| What | one Qdrant snapshot of `documents` (stored as `documents_new-<id>-<ts>.snapshot`, ~21 GB) + `rag_state-<ts>.db` (~250 MB), same `<ts>` |
| Where | `/mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/` |
| Retention | 7 pairs (about one week, ~150 GB) |
| Server 1 NVMe | `/var/lib/qdrant/snapshots` holds one snapshot only while it is being downloaded; normally empty |
| Recovery point | at most ~24 h old; the next ingestion run re-ingests anything newer |

`<ts>` is Qdrant's creation time in **UTC** (`YYYY-MM-DD-HH-MM-SS`).

`scripts/sqlite-qdrant-snapshot.sh [--keep N]` (default 7; the wrapper passes 7):

1. Safety net 1 — delete any snapshot Qdrant still holds for the collection (API).
2. Safety net 2 — delete stale `*.tmp` partial downloads on the NAS.
3. Safety net 3 — abort before creating anything if the NAS has less than 60 GB free.
4. `PRAGMA wal_checkpoint(TRUNCATE)` on `rag_state.db`.
5. `POST /collections/documents/snapshots` — Qdrant writes the archive to its local
   `snapshots_path`.
6. Safety net 4 — an EXIT trap deletes the Qdrant-local snapshot and the partial download on
   any failure from here on.
7. Download to `<name>.tmp` on the NAS (`curl --max-time 7200`), check it is non-empty,
   rename to `<name>`.
8. `DELETE` the snapshot from Qdrant.
9. Copy `rag_state.db` to `rag_state-<ts>.db` next to it.
10. Safety net 5 — keep the newest N `.snapshot` files and delete `rag_state-*.db` copies
    whose snapshot is gone.

The pair is consistent because the snapshot step runs after the daemon has exited, inside the
same oneshot, and the search service never writes: nothing changes Qdrant or the state DB
between steps 5 and 9. Download time depends on Server 1's link (a few minutes on gigabit;
slow downloads point at that link).

### 9.2 Choosing a pair

```bash
ls -lt /mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/
```

Pick the newest `<ts>` (UTC) at or before the point to restore to; use the `.snapshot` and the
`rag_state-<ts>.db` with that same `<ts>`.

### 9.3 Full restore (Qdrant storage lost or corrupt)

Restores both halves from one pair. Needs shell access to both servers.

**1. Stop everything that writes or creates the collection (Server 2).** The search service
calls `ensure_collection()` on every query and would create an empty real `documents`
collection on a fresh Qdrant.

```bash
sudo systemctl stop ragnart-ingest.timer ragnart-ingest.service ragnart-search.service
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

**4. Restore the state DB from the same pair (Server 2).**

```bash
cp /mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/rag_state-<ts>.db \
   /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db
rm -f /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db-{wal,shm}
```

**5. Verify (Server 2).**

```bash
sudo systemctl start ragnart-search.service
python utils/rag_status.py                      # completed count vs Qdrant point count
python utils/rag_sparse_migrate.py status       # alias -> documents_new, sparse space present
python utils/rag_smoke_test.py --query "AWS certifications" --user florian --expect "AWS"
```

**6. Catch up and re-enable (Server 2).** A sync re-queues everything that changed after
`<ts>` (newer mtime, or not in the restored queue) and removes what was deleted after `<ts>`; the
daemon re-ingests it with delete-before-insert.

```bash
bash scripts/ragnart-ingest-run.sh
sudo systemctl start ragnart-ingest.timer
systemctl list-timers ragnart-ingest.timer
```

### 9.4 State-DB-only restore (Qdrant healthy)

Use when `rag_state.db` is corrupt but Qdrant is intact. All steps on Server 2.

```bash
sudo systemctl stop ragnart-ingest.timer ragnart-ingest.service

cp /mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/rag_state-<ts>.db \
   /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db
rm -f /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db-{wal,shm}

bash scripts/ragnart-ingest-run.sh          # sync + re-ingest the gap
sudo systemctl start ragnart-ingest.timer
```

The sync reconciles by mtime and by presence, not by asking Qdrant: files changed or added
after `<ts>` are re-queued and re-ingested (step 0 removes their existing points first);
files deleted after `<ts>` still have rows, are found gone, and their (already absent) points
are deleted again. Qdrant ends up with one set of points per source either way.

### 9.5 On-demand snapshot

```bash
RAG_FORCE_SNAPSHOT=1 bash scripts/ragnart-ingest-run.sh   # preferred: runs after the daemon
bash scripts/sqlite-qdrant-snapshot.sh                    # direct; takes the ingestion lock itself, exits 75 while a run holds it
```

Each extra pair counts toward the 7 kept, so forcing snapshots shortens the history.

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
- **Coverage.** Only `florian` is an active user. `la-familia` content enters the index only
  through a sync run without `--user`, and the scheduled `--user florian` sync removes
  `la-familia` rows and points (§5.3), so in practice the shared root is not indexed.
- **Clients.** Clients call `GET /v1/search` directly and manage their own retrieval loops.
  SoHoAI's gateway on Server 1 answers its own `GET /v1/rag/search` with a 307 redirect to
  this service.
