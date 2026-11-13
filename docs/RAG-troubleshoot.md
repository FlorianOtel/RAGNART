---
title: "RAGNART — RAG troubleshooting guide"
created_at: 20260422-000000
created_by: Claude Code (Claude Sonnet 4.6)
updated_by: OpenCode (Claude Sonnet 5); Claude Code (Claude Opus 5.5, 2026-11-04 -- OpenCode 2.x ingestion loss, old-venv pollution, cross-host WAL "malformed"); Claude Code (Claude Haiku 5.5, 2026-11-13 -- RAGNART v0.1 move: name and path corrections only, content unchanged); Claude Code (Claude Haiku 5.5, /brain pipeline -- v0.1 post-review doc fixes); Claude Code (Claude Opus 5.5, 2026-11-13 -- post-v0.1 ops: Qdrant local config, daily snapshot retention, data root RAGNART--databases); Claude Code (Claude Opus 5.5, 2026-11-13 -- rewritten as a current-state troubleshooting guide); Claude Code (Claude Opus 5.5, 2026-11-13 -- section pointers updated after the as-is rewrite)
updated_at: 2026-11-13--16-45
context: >
  Troubleshooting guide for RAGNART, the RAG retrieval and ingestion stack: Qdrant on
  Server 1 (um690, 192.168.1.93) and the search service, ingestion timer, Ollama bge-m3
  embeddings and the bge-reranker-v2-m3 reranker on Server 2 (NucBoxK11, 192.168.1.95),
  with all persistent data under /mnt/nfs/__Backups/RAGNART--databases/ on the NAS.
  Each entry is laid out as symptom, how to check (exact commands), cause and fix, and
  describes the system as it currently is. Commands, paths, units and thresholds are
  checked against the repository (ragnart/, utils/, scripts/, ragnart.yaml) and the
  live hosts. Design rationale lives in docs/RAG-strategy.md; the history of past
  investigations lives in git.
---

# RAGNART — RAG troubleshooting guide

## 0. Conventions

| Host | Address | Runs |
|---|---|---|
| Server 1 (`um690`) | 192.168.1.93 | Qdrant `:6333` (systemd `qdrant`, storage on local NVMe `/var/lib/qdrant/storage`); SoHoAI gateway (tmux session `SoHoAI`, not a systemd unit) |
| Server 2 (`NucBoxK11`) | 192.168.1.95 | `ragnart-search.service` `:8100`; `ragnart-ingest.timer` → `ragnart-ingest.service`; Ollama `bge-m3` `:11434`; llama-swap `:8000` serving the reranker at `/v1/rerank` (tmux session `llama-swap`) |
| NAS | `/mnt/nfs/__Backups/RAGNART--databases/` | `sqlite/rag_state.db`, `logs/`, `qdrant-snapshots/documents/`, `rag-ingest.lock` |

- Repo: `/mnt/nfs/Florian/Gin-AI/projects/RAGNART`. Every `python utils/...` command below
  runs from the repo root after `source ~/Gin-AI/.Gin-AI-python-3.12/bin/activate`.
- **Anything that opens `rag_state.db` runs on Server 2 only.** That includes `sqlite3`,
  `rag_status.py`, `rag_sync_nfs.py`, `rag_ingest_daemon.py`, `rag_reset.py`,
  `rag_purge_corrupt.py` and `scripts/sqlite-qdrant-snapshot.sh`. The DB is in WAL mode,
  whose `-shm` index only works when every connection is on one host (see §2.9).
- Privileged commands: `sudo …` where a password prompt is acceptable, otherwise
  `ssh root@localhost '…'` on the host in question. `journalctl` needs one of the two.
- `systemctl is-active ragnart-ingest.service` prints `inactive` even while a run is in
  progress (a oneshot is `activating` until it exits). Use
  `systemctl show -p ActiveState --value ragnart-ingest.service` instead.

---

## 1. Quick health check

Run on Server 2. Everything here is read-only.

```bash
cd /mnt/nfs/Florian/Gin-AI/projects/RAGNART
source ~/Gin-AI/.Gin-AI-python-3.12/bin/activate

# Qdrant (Server 1): process, collection, alias
curl -s -m 5 http://192.168.1.93:6333/healthz; echo            # "healthz check passed"
curl -s -m 5 http://192.168.1.93:6333/aliases; echo             # documents -> documents_new
python utils/qdrant_status.py                                   # exit 0 = green
python utils/rag_sparse_migrate.py status                       # alias, point count, sparse vectors ✓

# Search service (Server 2)
systemctl show -p ActiveState --value ragnart-search.service    # active
curl -s -m 10 http://192.168.1.95:8100/health; echo             # {"status":"ok","qdrant":true}
python utils/rag_smoke_test.py --query "AWS certifications" --user florian --expect "AWS"

# Ingestion (Server 2)
systemctl show -p ActiveState --value ragnart-ingest.service    # inactive = idle, activating = running
systemctl show -p Result --value ragnart-ingest.service         # success
systemctl list-timers ragnart-ingest.timer --no-pager           # next slot: 01/07/13/19:00
python utils/rag_status.py --user florian                       # queue counts (see §5.3 for its Qdrant line)
grep -E ' (ERROR|WARNING):' /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log | tail -20

# Embeddings and reranker (Server 2)
curl -s -m 5 http://192.168.1.95:11434/api/tags                 # lists bge-m3:latest
curl -s -m 5 http://192.168.1.95:8000/running                   # bge-reranker-v2-m3 "state":"ready"

# Snapshots and disk
ls -lt /mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/ | head -5
df -h /mnt/nfs/__Backups/RAGNART--databases/
ssh 192.168.1.93 'df -h /var/lib/qdrant; find /var/lib/qdrant/snapshots -type f | wc -l'   # 0 files
```

---

## 2. Ingestion

One run of `ragnart-ingest.service` (`scripts/ragnart-ingest-run.sh`) does, in order:

| Step | What | Typical duration |
|---|---|---|
| 1 | `rag_sync_nfs.py --user florian`: walk `/mnt/nfs/Florian`, `~/.claude/projects` and the OpenCode API; queue new/changed files; delete Qdrant points and queue rows of files that are gone | ~10 min |
| 2 | `rag_ingest_daemon.py --workers 3 --batch 20`: take the NFS lock, parse → chunk → embed → upsert every `pending` file | seconds to hours, depending on the backlog |
| 3 | `PRAGMA wal_checkpoint(TRUNCATE)` on `rag_state.db` | seconds |
| 4 | `sqlite-qdrant-snapshot.sh --keep 7` if the newest NAS snapshot is older than 20 h (§3) | ~4–5 min |

The service is killed after `TimeoutStartSec=7200` (2 h). Env overrides for a manual run:
`RAG_WORKERS`, `RAG_BATCH`, `RAG_LOGFILE`, `RAG_SNAPSHOT_MIN_AGE_H`, `RAG_FORCE_SNAPSHOT=1`.

Queue states in `ingestion_queue`: `pending` → `processing` → `completed`; a failure goes
back to `pending` until `retry_count` reaches `max_retries` (5), then `ignored` with
`skip_reason` set. `failed` rows are reset to `pending` by the next sync.

### 2.1 Did the last run succeed? Is one running now?

```bash
systemctl show -p ActiveState,Result,ExecMainStartTimestamp ragnart-ingest.service
systemctl list-timers ragnart-ingest.timer --no-pager
grep -E '=== RAG ingestion (starting|complete)' /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log | tail -4
ssh root@localhost 'journalctl -u ragnart-ingest.service -n 30 --no-pager'
```

A "starting" line without a matching "complete" line means the run died or is still going.
The wrapper runs under `set -euo pipefail`, so a crash in `rag_sync_nfs.py` aborts the run
before ingestion; the traceback is in the journal.

**A run that ends "complete" can still be wrong.** Warnings do not fail a run. After any
upgrade of a source (OpenCode, a venv rebuild, a directory move), compare the sync summary
counts with the previous run:

```bash
grep -A10 '^Scan complete:' /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log | tail -11
```

A source dropping to 0, or a jump of hundreds of files under one directory, is the signal.

### 2.2 Run stuck or killed

| Symptom | Check | Cause | Fix |
|---|---|---|---|
| `ActiveState=activating` for close to 2 h | `tail -f …/logs/rag-ingest.log`; `python utils/rag_status.py --watch /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log` | Large backlog, or a slow Qdrant / Ollama call | Let it hit the 2 h cap; the next slot continues. For a big backlog run the daemon by hand (below). |
| Rows stuck in `processing` | `python utils/rag_status.py` | Daemon was killed mid-file | Nothing: `crash_recovery()` resets them to `pending` at the next daemon start. |
| Run started late after a reboot | `systemctl list-timers` | `Persistent=true` fires a missed slot at boot | Expected. |

Manual run (Server 2, when `ActiveState` is `inactive`):

```bash
# Whole pipeline exactly as the timer runs it:
sudo systemctl start ragnart-ingest.service
# Or only the daemon, logging to the shared log:
python utils/rag_ingest_daemon.py --workers 3 --batch 20 \
  --log-file /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log
```

`--workers` and `--batch` are both required. GPU embedding (Ollama on Server 2):
`--workers 3 --batch 20`. CPU embedding: `--workers 1 --batch 5`.

### 2.3 "Another rag_ingest_daemon is already running (lock held …)"

Only one daemon may run at a time, on any host. The daemon takes
`fcntl.lockf()` on `/mnt/nfs/__Backups/RAGNART--databases/rag-ingest.lock`; the NAS lock
manager makes the lock cross-host. The lock file is always 0 bytes; its mtime advances on
every daemon start.

```bash
# Is a daemon running anywhere?
systemctl show -p ActiveState --value ragnart-ingest.service
pgrep -af 'utils/rag_ingest_daemon.py'
ssh 192.168.1.93 "pgrep -af 'utils/rag_ingest_daemon.py'"

# Is the lock free? (takes and releases it at once; uses lockf like the daemon —
# `flock --nonblock` is not reliable on this Synology NFSv4.1 mount)
python3 -c "
import fcntl
f = open('/mnt/nfs/__Backups/RAGNART--databases/rag-ingest.lock', 'a')
try:
    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB); print('lock free')
except BlockingIOError:
    print('lock HELD')
"
```

- Lock held and a daemon process exists → wait for it, or stop it
  (`sudo systemctl stop ragnart-ingest.service` for the timer's run).
- Lock held and no process on either server → a client that held it has died without the
  NAS noticing yet; NFSv4 leases expire within a few minutes. Re-check, then retry.
- When the queue is empty the daemon prints "No pending files" to stdout (journal), not to
  `rag-ingest.log`. A run with nothing to do leaves no "Processing" lines in the log.

### 2.4 A file does not show up in search

Work down the layers. Steps 1–2 run on Server 2.

1. **Is it queued?**
   ```bash
   sqlite3 /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db \
     "SELECT file_path, status, retry_count, skip_reason, error_msg
        FROM ingestion_queue WHERE file_path LIKE '%<name-fragment>%';"
   ```
2. **Act on the status:**

   | Status | Meaning | Fix |
   |---|---|---|
   | no row | The scanner did not see it | Go to step 3 |
   | `pending` | Not processed yet | Wait for the next slot, or run the daemon (§2.2) |
   | `ignored`, `skip_reason` = `binary content: NN% printable chars` | Parsed text was under 85 % printable (scanned or encrypted PDF, OLE2 `.pptx`, garbage) | Fix or replace the file; a changed mtime re-queues it automatically |
   | `ignored`, other reason | 5 attempts failed; `skip_reason` is the last error | Fix the cause, then re-queue (§2.10) |
   | `completed` | Points should exist | Go to step 4 |

   `python utils/rag_status.py --ignored --user florian` lists every ignored file with its
   reason. A document whose parsed text is empty is skipped with
   `WARNING: Empty document, skipping` and never produces points.
3. **Why did the scanner not see it?** Check, in this order, against `rag.scanner` in
   `ragnart.yaml`:
   - The extension is in `include_extensions` (`.pdf .pptx .docx .md .sh .txt .csv .yaml
     .yml .ipynb .html .jsonl`).
   - No `exclude_file_patterns` substring is in the file name (`@synoeastream`, `._`, `~$`).
   - No directory on the path, **as walked from the root**, ends with an
     `exclude_dir_names` entry, and none ends with an `exclude_dir_suffixes` entry.
   - The file is under a scanned root. The timer syncs only `--user florian`:
     `/mnt/nfs/Florian`, `/home/florian/.claude/projects` and the OpenCode API (§2.6, §2.7).
   - It is not a second path to a file already seen: the scanner follows symlinks and
     ingests each real file once, under the first path it reaches.
   Then run a sync and look for the file: `python utils/rag_sync_nfs.py --user florian`.
4. **Does Qdrant have points for it?** `source_path` has a keyword index, so these are fast:
   ```bash
   # Points for one exact path
   curl -s -X POST http://192.168.1.93:6333/collections/documents/points/count \
     -H 'Content-Type: application/json' \
     -d '{"filter":{"must":[{"key":"source_path","match":{"value":"/exact/path/to/file"}}]},"exact":true}'

   # Every distinct source_path with its point count (filter the output with grep)
   curl -s -X POST http://192.168.1.93:6333/collections/documents/facet \
     -H 'Content-Type: application/json' -d '{"key":"source_path","limit":200000}' \
     | python3 -c "import json,sys; [print(h['count'], h['value']) for h in json.load(sys.stdin)['result']['hits']]" \
     | grep '<name-fragment>'
   ```
   Do not filter with `match.text` on `source_path`: there is no full-text index and the
   request times out. Claude Code sessions also carry `session_id`, `session_title` and
   `project`; filter by `{"key":"session_id","match":{"value":"<uuid>"}}` when only the UUID
   is known.
5. **Completed with points, still not found** → it is a search problem (§4).

### 2.5 Files that should not be in the index are being ingested

**Check:** the facet query in §2.4 step 4, grepped for the unwanted directory; or
`grep 'Ingested .*' …/logs/rag-ingest.log | grep '<dir>'`.

**Cause:** an `exclude_dir_names` entry matches the path *as `os.walk` sees it*, by suffix
on a `/` boundary. It does not resolve symlinks and does not match variants of a name:

- `~/.claude` is a symlink to `/mnt/nfs/Florian/Gin-AI/dot.claude-Linux`, so the NFS walk
  sees `dot.claude-Linux/`, not `.claude/` — both entries are needed.
- `.Gin-AI-python-3.12/` does not match `.Gin-AI-python-3.12.old/`. Venv contents are
  caught generically by `site-packages/`, `.uv-python/` and `.cache/`.

Audit for new symlinked or renamed dot-directories now and then:

```bash
find /mnt/nfs/Florian -maxdepth 3 -type d -name 'dot.*'
ls -la ~ | grep -- '->'
```

**Fix:** add the real on-disk directory name (with trailing `/`) to
`rag.scanner.exclude_dir_names`. The next sync treats the excluded files as gone: it
queues Qdrant deletes for them and drops their queue rows. Deletes use `wait=false`, so
point counts keep falling for a while; wait for `update_queue.length == 0` before quoting a
count:

```bash
curl -s http://192.168.1.93:6333/collections/documents \
  | python3 -c "import json,sys; r=json.load(sys.stdin)['result']; print(r['points_count'], r['update_queue'])"
```

### 2.6 Sync deletes far more than expected, or a source drops to 0

`rag_sync_nfs.py` treats **every queue row whose path was not seen in this scan** as
deleted, and removes its points. Anything that hides files from the scan therefore removes
them from the index.

| Symptom in the log | Cause | Fix |
|---|---|---|
| `NFS root not accessible, skipping: /mnt/nfs/Florian` followed by a large `Queued Qdrant point deletion for N removed file(s)` | NAS mount missing or stale during the sync | Never run a sync with the mount down (`ls /mnt/nfs/Florian` first). If it happened, remount and run sync + daemon: every file is re-queued and re-ingested. For a large loss, restoring the last snapshot pair is faster (§8). |
| `claude_chats root not accessible` | `~/.claude` symlink target unreachable | Same as above. |
| `Opencode API unreachable — preserving N existing opencode session(s)` | OpenCode down or auth failed; existing `opencode://` rows are kept on purpose | See §2.7. |
| `la-familia` content disappears after a timer run | A sync **without** `--user` scans the shared root `/mnt/nfs/La-Familia` (owner `la-familia`); the timer's `--user florian` sync does not, so it removes those rows again | Shared content is only stable if the timer syncs it; add the shared scope to `RAG_SYNC_USERS` handling in `scripts/ragnart-ingest-run.sh` before relying on it. |

### 2.7 OpenCode sessions missing

**Check:**

```bash
grep -iE 'opencode' /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log | tail -10
ls -l ~/.config/opencode/opencode-server.env        # must exist on Server 2
grep -c OPENCODE_SERVER_PASSWORD ~/.config/opencode/opencode-server.env
```

**Cause / fix:**

- `ERROR: OpenCode scan skipped: …` or HTTP 401/403 → the scanner sends HTTP Basic auth
  (user `opencode`, password `OPENCODE_SERVER_PASSWORD` from
  `opencode.password_env_file` in `ragnart.yaml`, default
  `~/.config/opencode/opencode-server.env`). Restore the file or the key; the next run
  picks it up.
- Connection refused → the OpenCode server on Server 2 (`opencode.api_url`,
  `http://192.168.1.95:4096`) is down; start it.
- Session content is read from `/api/session/{id}` and `/api/session/{id}/message`. A
  non-JSON answer raises an error rather than recording an empty session, so a route change
  in OpenCode shows up as per-session ERROR lines.

While the scan fails, existing opencode points stay in Qdrant; nothing is lost.

### 2.8 Qdrant timeouts during ingestion or sync

**Symptom:** `ERROR: Ingestion failed for …: timed out` /
`qdrant_client.http.exceptions.ResponseHandlingException: timed out`.

**Check:**

```bash
python utils/qdrant_status.py                      # yellow = optimizer merging
curl -s http://192.168.1.93:6333/collections/documents \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['result']['payload_schema'])"
# must contain: 'source_path': {'data_type': 'keyword', ...}
ping -c 5 192.168.1.93
```

**Cause / fix:**

- Every ingest first deletes the file's old points by `source_path`. Without the keyword
  index this is a scan over millions of points and exceeds the client timeout (60 s; 120 s
  in `rag_sync_nfs.py`). `ensure_collection()` creates the index on every process start;
  if it is missing, any daemon or service start recreates it, and Qdrant builds it in the
  background.
- A `yellow` collection (large segment merge after bulk deletes or a big file) can stall
  single requests. Failed files go back to `pending` and are retried on later runs; for bulk
  maintenance wait with `python utils/qdrant_status.py --watch`.
- Any other frequently-filtered field (`owner`, `file_type`) has no payload index. If
  per-user search or count latency grows, an index on `owner` is the first thing to add.

### 2.9 "database disk image is malformed" (state DB)

**Symptom:** `sqlite3.DatabaseError: database disk image is malformed` from
`ragnart/state.py` in the journal or log, usually in `rag_sync_nfs.py`.

**Cause:** `rag_state.db` is in WAL mode. The WAL index (`rag_state.db-shm`) is shared
memory and only coherent between connections on **one host**. A connection from another
host while Server 2 is writing (even a `StateDB()` constructor, which runs PRAGMAs and
schema migrations) corrupts that connection's view, and can corrupt the file. A
per-connection error leaves the file itself intact.

**Check (Server 2, no run active):**

```bash
systemctl show -p ActiveState --value ragnart-ingest.service     # must be inactive
sqlite3 /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db "PRAGMA integrity_check;"
# Who else touches the file? Look for processes on Server 1:
ssh 192.168.1.93 "pgrep -af 'rag_|ragnart'"
```

**Fix:**

- `integrity_check` prints `ok` → the file is fine; the failing process had a bad view.
  Stop whatever opened the DB from the other host, take a copy
  (`cp rag_state.db rag_state-pre-rerun.db` in the same directory), and re-run the
  ingestion.
- `integrity_check` reports errors → restore the DB from the newest snapshot pair:
  §8 / `docs/RAG-strategy.md` §9.4 (state-DB-only restore). Restore only the main `.db`
  file, delete `rag_state.db-wal` and `rag_state.db-shm`, then run
  `python utils/rag_sync_nfs.py --user florian` to reconcile.
- Prevention: every tool that opens the DB runs on Server 2 (§0). `rag_purge_corrupt.py`
  opens it read-only (`mode=ro`), but still belongs on Server 2.

### 2.10 Re-queue an ignored or wrongly skipped file

Server 2, no ingestion run active:

```bash
sqlite3 /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db \
  "UPDATE ingestion_queue SET status='pending', retry_count=0, skip_reason=NULL, error_msg=NULL
    WHERE file_path = '/exact/path/to/file';"
python utils/rag_ingest_daemon.py --workers 1 --batch 5 \
  --log-file /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log
```

Touching the file on disk (`touch /path/to/file`) has the same effect at the next sync,
because a newer mtime re-queues a row whatever its status.

### 2.11 Embedding errors (Ollama HTTP 500)

**Symptom:** every chunk of a file fails with HTTP 500 from
`http://192.168.1.95:11434/api/embeddings`; the Ollama journal shows
`cudaMalloc failed: out of memory` / `graph_reserve: failed to allocate compute buffers`.

**Check (Server 2):**

```bash
systemctl cat ollama | grep -E 'OLLAMA_(CONTEXT_LENGTH|MAX_LOADED_MODELS)'   # CONTEXT_LENGTH=768
curl -s http://192.168.1.95:11434/api/ps | python3 -m json.tool             # context_length when loaded
nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv
curl -s http://192.168.1.95:11434/api/embeddings -H 'Content-Type: application/json' \
  -d '{"model":"bge-m3","prompt":"test"}' \
  | python3 -c "import json,sys; print(len(json.load(sys.stdin)['embedding']), 'dims')"    # 1024 dims
```

**Cause / fix:** Ollama sizes the compute buffer from the context length. With the default
(4096) bge-m3 needs ~1.2 GiB of compute buffer on top of its weights, which does not fit
next to the chat model and the reranker. `Environment="OLLAMA_CONTEXT_LENGTH=768"` in the
Ollama unit keeps it at ~0.6 GiB total; child chunks are ~250 tokens, so 768 is enough.
If the line is missing, add it, `sudo systemctl daemon-reload && sudo systemctl restart
ollama`. Failed files are retried by the next runs.

---

## 3. Snapshots and disk space

**How it works.** `scripts/sqlite-qdrant-snapshot.sh` (called by the ingestion wrapper at
most once per 20 h) checkpoints the WAL, asks Qdrant for a snapshot of `documents`
(written to Server 1's `/var/lib/qdrant/snapshots`), downloads it to the NAS, deletes the
Server 1 copy, copies `rag_state.db` next to it, and keeps the **7** newest pairs:

```
/mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/
  documents_new-<id>-YYYY-MM-DD-HH-MM-SS.snapshot   (~21 GB; timestamp in UTC)
  rag_state-YYYY-MM-DD-HH-MM-SS.db                  (same timestamp = one restore pair)
```

Creation takes ~45 s, the download ~3.5 min on gigabit. Snapshot output goes to
`logs/rag-ingest.log` (lines from `Creating snapshot` to `Snapshot complete`).

**Check:**

```bash
D=/mnt/nfs/__Backups/RAGNART--databases
ls -lt $D/qdrant-snapshots/documents/
grep -E 'snapshot|Snapshot|Download complete|ERROR' $D/logs/rag-ingest.log | tail -15
df -h $D
ssh 192.168.1.93 'df -h /var/lib/qdrant; find /var/lib/qdrant/snapshots -type f'
curl -s http://192.168.1.93:6333/collections/documents/snapshots; echo     # normally []
```

| Symptom | Cause | Fix |
|---|---|---|
| No pair newer than ~24 h | Wrapper skipped it (`snapshot skipped: newest pair is younger than 20 h`), or the snapshot step failed | Read the log lines above. To take one now (Server 2, ingestion `inactive`): snapshot only, `bash scripts/sqlite-qdrant-snapshot.sh --keep 7 2>&1 \| tee -a /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log`; or a full run plus snapshot, `RAG_FORCE_SNAPSHOT=1 bash scripts/ragnart-ingest-run.sh`. |
| `ERROR: only N GB free on the NAS (need 60 GB) -- snapshot skipped` | NAS volume nearly full | Free space on the NAS. 7 pairs take ~150 GB. |
| `*.tmp` file in the snapshot dir | A download was interrupted | Removed automatically at the start of the next snapshot run. |
| Files in `/var/lib/qdrant/snapshots` on Server 1, or `…/snapshots` API not `[]` | A run died between create and delete | Removed automatically at the start of the next snapshot run (through the Qdrant API). Server 1 `/var` must have room for one ~21 GB snapshot. |
| A ~20 GiB download takes 30+ min | Slow link to Server 1 | §6. |
| More than 7 `.snapshot` files, or `logs/sqlite-qdrant-snapshot.log` keeps growing | A second snapshot job (cron) runs besides the wrapper; the wrapper never writes that log file | `crontab -l` and `sudo crontab -l` on both servers, `grep -rl snapshot /etc/cron.d /etc/crontab`; remove the extra job. |
| `rag_state-*.db` without a matching `.snapshot` | Pair rotated out | Removed automatically by the rotation step. |

Run `sqlite-qdrant-snapshot.sh` by hand only on Server 2 (it opens `rag_state.db`) and
only while no ingestion run is active (it does not take the ingest lock).

### Log rotation

`/etc/logrotate.d/ragnart-ingest` (installed from `scripts/ragnart-ingest-logrotate` by
`scripts/install-units.sh`) rotates `logs/rag-ingest.log` daily, keeps 7, compresses,
`copytruncate`. The log directory is group-writable on the NAS, so the config needs
`su florian florian`; without it logrotate refuses with "parent directory has insecure
permissions" and the log grows without bound.

```bash
ls -l /mnt/nfs/__Backups/RAGNART--databases/logs/
ssh root@localhost 'logrotate -d /etc/logrotate.d/ragnart-ingest 2>&1 | grep -iE "error|insecure"'   # no output = fine
```

---

## 4. Search

The service (`ragnart.service:app`, `ragnart-search.service`, Server 2 `:8100`) serves
`GET /v1/search?q=…&user=…&top_k=…&file_types=…&score_threshold=…&rerank=…` and
`GET /health`. Each query: embed with bge-m3 → hybrid dense + sparse (`sparse_text`) query
fused with RRF → cross-encoder rerank → top_k.

Two ways to run a query, useful to tell the service apart from the pipeline:

```bash
# Through the HTTP service
python utils/rag_smoke_test.py --query "your query" --user florian --expect "substring"
curl -s 'http://192.168.1.95:8100/v1/search?q=your+query&user=florian&top_k=5' | python3 -m json.tool
# In-process (same search code, bypasses the service)
python utils/rag_search_cli.py --query "your query" --user florian
python utils/rag_search_cli.py --query "your query" --user florian --no-rerank --file-types claude_chat
```

### 4.1 HTTP 503 "Qdrant client not available"

**Cause:** the service connects to Qdrant once, at start. If Qdrant was unreachable then,
it logs `Qdrant unavailable — search disabled` and answers 503 until it is restarted —
even after Qdrant is back.

```bash
ssh root@localhost 'journalctl -u ragnart-search.service -n 50 --no-pager' | grep -i qdrant
curl -s -m 5 http://192.168.1.93:6333/healthz; echo
```

**Fix:** once Qdrant answers, `sudo systemctl restart ragnart-search.service`. Restart the
search service whenever Qdrant was down while the service was starting.

### 4.2 `/health` hangs, or reports `degraded`

`/health` calls Qdrant's `get_collections()` with the client's 60 s timeout. When Server 1
is down or unreachable the request hangs well beyond 5 s instead of failing fast; use
`curl -m 10` in checks. `{"status":"degraded","qdrant":false}` means Qdrant did not answer:
go to §5 and §6. Searches recover by themselves when Qdrant comes back, unless the service
started while Qdrant was down (§4.1).

### 4.3 Empty or too few results

| Check | Cause | Fix |
|---|---|---|
| Did the request pass `user=`? | With `user`, the filter is `owner ∈ {user, "la-familia"}`. An unknown owner name returns nothing. Without `user`, there is no owner filter at all. | Use the owner from `ragnart.yaml` `users.*.owner` (`florian`). |
| `file_types=` given? | Matches payload `file_type` exactly: `pdf pptx docx md sh txt csv yaml yml ipynb html claude_chat opencode` | Drop it or correct it. |
| `score_threshold=` > 0? | Applies only on the dense-only path (hybrid off or no sparse vectors); there it drops hits below that cosine score | Use `0` (default) to see everything. |
| Is the file indexed at all? | — | §2.4. |
| `python utils/rag_sparse_migrate.py status` shows sparse ✗ | Hybrid silently falls back to dense-only (`hybrid query requested but collection lacks sparse_text`) | Collection must be `documents` → `documents_new` with `sparse_text` (§5.3). |

### 4.4 Results look like noise (binary garbage, scanner artefacts, CSV rows)

**Cause:** points whose `parent_text` is mostly non-printable produce near-random
embeddings that score in the same band as real content. New ingests are protected by the
85 % printable gate (§2.4); points written without that check, and "zombie" points whose
source has no queue row, are not.

```bash
# Server 2. Report only (default): [N pts] IN_DB/NOT_IN_DB ratio path
python utils/rag_purge_corrupt.py --dry-run --user florian
python utils/rag_purge_corrupt.py --dry-run --threshold 0.75 --save /tmp/corrupt_paths.txt
# Delete corrupt points whose source file no longer exists on disk
python utils/qdrant_status.py && python utils/rag_purge_corrupt.py --confirm
# Also delete when the file still exists (re-ingesting it would recreate them -- fix or
# exclude the file first)
python utils/rag_purge_corrupt.py --confirm --force
```

`IN_DB` rows can also be cleaned by removing or renaming the file and running a sync;
`NOT_IN_DB` rows can only be removed with `--confirm`. Run `--confirm` only when
`qdrant_status.py` reports green; it deletes with `wait=true` and retries on Qdrant 500s.

### 4.5 Results are not reranked (no `rerank_score`), or ordering got worse

**Check:**

```bash
ssh root@localhost 'journalctl -u ragnart-search.service --since -1h --no-pager' | grep -i rerank
curl -s -m 5 http://192.168.1.95:8000/running | python3 -m json.tool      # bge-reranker-v2-m3 state
curl -s http://192.168.1.95:8000/v1/rerank -H 'Content-Type: application/json' \
  -d '{"model":"bge-reranker-v2-m3","query":"test","documents":["foo"]}'
# -> {"results":[{"index":0,"relevance_score":...}]}
```

**Cause:** any reranker error (connection refused, timeout > 10 s, HTTP 500) logs
`WARNING: reranker failed: … — falling back to Qdrant order` and returns the fused order.
Users see results, just unreranked.

**Fix:**

- The reranker is a llama-swap model (`bge-reranker-v2-m3`, backend port 8012) in a
  persistent group with `ttl: 0`, preloaded at llama-swap start; nothing evicts it. It is
  reached only through llama-swap at `:8000/v1/rerank`, routed by the `model` field, so
  `rag.rerank.model` in `ragnart.yaml` must equal the llama-swap model key.
- llama-swap is not a systemd unit. After a Server 2 reboot it stays down until restarted:
  ```bash
  tmux send-keys -t llama-swap:0 C-c
  tmux send-keys -t llama-swap:0 'source ./llama-swap--start.sh' Enter
  ```
  (window 0 runs in `/mnt/nfs/Florian/Gin-AI/projects/SoHoAI`; the config is
  `llama-swap--config.yaml` there, reloaded on change by `--watch-config`). The reranker
  then needs ~15 s to load.
- HTTP 500 on long inputs: the reranker runs with `-c 768 -b 768 -ub 768`; the client does
  not truncate, so all three must stay at 768.
- Segfault during CUDA init or OOM: check `nvidia-smi` for an unexpected tenant on the
  12 GB card.

### 4.6 The same query returns a different order

**Cause:** two sources of non-determinism, both expected:

1. RRF fusion breaks exact score ties inconsistently between calls.
2. Reranker scores shift by ~1e-3…1e-2 depending on which candidates share the batch, so
   near-ties swap.

**Check** whether two result lists really differ with the tie-aware comparator (exit 0 =
same documents and scores, ignoring order inside ties). Comparing an endpoint with itself
shows the noise floor:

```bash
python utils/rag_compare_endpoints.py \
  --old-url http://192.168.1.95:8100/v1/search --new-url http://192.168.1.95:8100/v1/search
```

It runs the queries in `utils/rag_bench_queries.txt`. Only a HARD-check failure (different
score multiset, or a different set of documents within a score group) is a real change.

### 4.7 A config change has no effect

`ragnart-search.service` reads `ragnart.yaml` once at start. After editing it:
`sudo systemctl restart ragnart-search.service`. Ingestion reads it at every run.

---

## 5. Qdrant (Server 1)

Qdrant 1.17, `qdrant.service` running `/usr/local/bin/qdrant --config-path
/etc/qdrant/config.yaml`, storage `/var/lib/qdrant/storage` and transient snapshots
`/var/lib/qdrant/snapshots`, both on local NVMe.

### 5.1 Qdrant does not start

```bash
ssh 192.168.1.93 'systemctl show -p ActiveState,ActiveEnterTimestamp qdrant; df -h /var/lib/qdrant'
ssh root@192.168.1.93 'journalctl -u qdrant -n 50 --no-pager'
```

Look for: disk full on `/var`; storage path not on local disk (Qdrant refuses NFS storage);
permission errors (the unit runs as `florian`). Loading ~3 M points takes a while after a
restart; `install-qdrant.sh` waits up to 120 s for `/healthz`. After Qdrant is back,
restart `ragnart-search.service` if it reports 503 (§4.1).

### 5.2 Qdrant started on built-in defaults

**Symptom:** the journal contains `Config file via --config-path is not found`; settings
from the config file (ports, paths, log level) do not apply.

**Cause:** `/etc/qdrant/config.yaml` is missing. Qdrant does not fail; it starts with
defaults.

```bash
ssh root@192.168.1.93 'ls -l /etc/qdrant/config.yaml; journalctl -u qdrant -b --no-pager | grep -c "Config file via --config-path is not found"'
```

**Fix (Server 1):** `sudo bash /mnt/nfs/Florian/Gin-AI/projects/RAGNART/scripts/qdrant/install-qdrant.sh`.
It installs a local copy of `scripts/qdrant/qdrant-config.yaml` and `qdrant.service`,
restarts Qdrant (search returns 503 for a few seconds), and fails if the error persists.
The config must stay a local file: at boot the NFS mount may not be up yet.

### 5.3 Collection and alias

The data lives in collection `documents_new`; everything addresses it through the alias
`documents`.

```bash
curl -s http://192.168.1.93:6333/collections; echo     # {"name":"documents_new"}
curl -s http://192.168.1.93:6333/aliases; echo         # documents -> documents_new
python utils/rag_sparse_migrate.py status              # read-only summary
```

- `utils/rag_status.py` prints `Qdrant: collection 'documents' does not exist yet` even
  when all is well: it checks real collection names and does not resolve aliases. Known
  issue in that utility; ignore the line and use the commands above.
- If the alias is really missing, `ensure_collection()` in any starting process will create
  an **empty** `documents` collection. Do not start services or ingestion in that state;
  recreate the alias first (`POST /collections/aliases` with
  `{"actions":[{"create_alias":{"collection_name":"documents_new","alias_name":"documents"}}]}`)
  or restore from a snapshot (§8).
- `rag_sparse_migrate.py migrate` and `swap` are migration tools, not repair tools; `swap`
  deletes the `documents` collection.

### 5.4 Optimizer state and lag

```bash
python utils/qdrant_status.py              # one shot; exit 0 = green
python utils/qdrant_status.py --verbose    # plus optimizer thresholds
python utils/qdrant_status.py --watch 30   # poll until green and lag 0
```

- **yellow**: a segment merge is running (after bulk deletes or a large ingest). Single
  requests may time out (`timed out after 0ns` on locked segments). Wait for green before
  bulk maintenance (`rag_purge_corrupt.py --confirm`, large re-ingests).
- **green with lag > 0**: idle. Vacuum runs only when a segment has ≥ 20 % deleted
  vectors, so spread-out deletes leave a stable lag; harmless.
- On this hybrid collection the "Indexed" figure counts dense **and** sparse vectors, so
  the lag shown is around 50 % even when idle. Compare "Indexed" with 2 × "Points" for the
  real backlog.
- After `wait=false` deletes (sync cleanup), point counts are only final once
  `update_queue.length` is 0 (§2.5).

---

## 6. Server 1 and the network

| Symptom | Check | Fix |
|---|---|---|
| Snapshot download of ~20 GiB takes 30+ min; ingest or search slow to Qdrant | `ssh 192.168.1.93 'cat /sys/class/net/eno1/speed; ip route show default'` → `1000`, `default via 192.168.1.254 dev eno1` | A speed below 1000 or a default route not on `eno1` (e.g. Wi-Fi) means traffic takes the slow path; fix the link or route. |
| Qdrant unreachable from Server 2 | `ping -c 5 192.168.1.93`; `curl -m 5 http://192.168.1.93:6333/healthz` | Host down, or Qdrant down (§5.1). |
| SoHoAI gateway keeps old behaviour after code or config was edited from Server 2 | `ssh 192.168.1.93 'tmux ls'` (session `SoHoAI`) | The gateway runs `uvicorn --reload` in tmux on Server 1; its file watcher does not see edits made over NFS from another host. Restart it by hand in that tmux session. It is not a systemd unit. |
| Something on Server 1 opens `rag_state.db` | `ssh 192.168.1.93 "pgrep -af 'rag_|ragnart'"` | Stop it; the DB belongs to Server 2 (§2.9). |

---

## 7. Logs and where to look

| What | Where | How |
|---|---|---|
| Ingestion runs (sync, daemon, checkpoint, snapshot) | `/mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log` (+ `.1.gz` … `.7.gz`) | `grep -E ' (ERROR|WARNING):'`; run markers `=== RAG ingestion starting/complete ===`; live: `python utils/rag_status.py --watch <log>` |
| Ingestion stdout/stderr, tracebacks, "No pending files" | journal of `ragnart-ingest.service` (Server 2) | `ssh root@localhost 'journalctl -u ragnart-ingest.service --since today --no-pager'` |
| Search requests, reranker fallbacks, Qdrant connect at start | journal of `ragnart-search.service` (Server 2) | `ssh root@localhost 'journalctl -u ragnart-search.service --since -1h --no-pager'` |
| Queue state | `sqlite/rag_state.db` (Server 2 only) | `python utils/rag_status.py [--user florian] [--ignored] [--list-pending 50]` |
| Qdrant | journal of `qdrant` (Server 1) | `ssh root@192.168.1.93 'journalctl -u qdrant --since today --no-pager'` |
| Ollama embeddings | journal of `ollama` (Server 2) | `ssh root@localhost 'journalctl -u ollama --since -1h --no-pager'` |
| Reranker / llama-swap | tmux `llama-swap:0` (Server 2) | `tmux attach -t llama-swap`; `curl -s :8000/running` |
| Timer schedule | — | `systemctl list-timers ragnart-ingest.timer` |
| Installed units vs repo | `/etc/systemd/system/ragnart-*.{service,timer}` vs `scripts/` | `systemctl cat ragnart-ingest.service ragnart-search.service ragnart-ingest.timer`; reinstall with `sudo bash scripts/install-units.sh` (refuses while a run is active) |

---

## 8. Recovery

Restoring Qdrant and `rag_state.db` from a snapshot pair (full rollback, or SQLite only
when Qdrant is healthy) is described in the restore procedure in `docs/RAG-strategy.md`.
Points that apply here:

- Always restore a **pair** (same timestamp) from
  `/mnt/nfs/__Backups/RAGNART--databases/qdrant-snapshots/documents/`; the DB copy was
  taken right after the WAL checkpoint, so only the main `.db` file is needed. Delete any
  `rag_state.db-wal` / `-shm` before the first open.
- Stop `ragnart-ingest.timer` first, and keep `ragnart-search.service` from recreating an
  empty `documents` collection (§5.3) while Qdrant is being restored.
- Afterwards run `python utils/rag_sync_nfs.py --user florian` and the daemon (§2.2) to
  pick up everything changed since the snapshot; ingestion is idempotent.
- `utils/rag_reset.py [--user OWNER] [--yes]` resets the queue to `pending` (all rows, or
  one owner) for a full re-ingest. Like `rag_status.py`, it checks real collection names
  and does not resolve the `documents` alias, so it currently leaves the Qdrant points in
  place (re-ingest replaces them file by file; points of files that no longer exist stay).
  It is a last resort, not a repair step.
