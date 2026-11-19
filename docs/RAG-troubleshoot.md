---
title: "RAGNART — RAG troubleshooting guide"
created_at: 20260422-000000
created_by: Claude Code (Claude Sonnet 4.6)
updated_by: OpenCode (Claude Sonnet 5); Claude Code (Claude Opus 5.5, 2026-11-04 -- OpenCode 2.x ingestion loss, old-venv pollution, cross-host WAL "malformed"); Claude Code (Claude Haiku 5.5, 2026-11-13 -- RAGNART v0.1 move: name and path corrections only, content unchanged); Claude Code (Claude Haiku 5.5, /brain pipeline -- v0.1 post-review doc fixes); Claude Code (Claude Opus 5.5, 2026-11-13 -- post-v0.1 ops: Qdrant local config, daily snapshot retention, data root RAGNART--databases); Claude Code (Claude Opus 5.5, 2026-11-13 -- rewritten as a current-state troubleshooting guide); Claude Code (Claude Opus 5.5, 2026-11-13 -- section pointers updated after the as-is rewrite); Claude Code (Claude Haiku 5.5, 2026-11-14 -- .ragignore checks, sync guards, ignored-reason entries, Qdrant 503 and /health behaviour); Claude Code (Claude Haiku 5.5, 2026-11-14 -- Ollama context length 2048, context-length 500 entry in §2.13); Claude Code (Claude Haiku 5.5); Claude Code (Claude Haiku 5.5, 2026-11-15 -- v0.2 session A: OpenCode database backup entry in §3); Claude Code (Claude Haiku 5.5, 2026-11-15 -- v0.2 session B: metadata backfill entry §2.14; automatic collision resolution §2.14; skipped-root message §2.6 and §2.12); Claude Code (Claude Haiku 5.5, 2026-11-16 -- v0.2 session C: sync plan, moves, tombstones and grace period §2.15; reconciliation anomalies and refused zombie deletes §2.16; GET /v1/search 422 §4.8 (smoke and compare tools send scope=all); sync message and La-Familia entries §1, §2.6, §2.12); Claude Code (Claude Opus 5.5, 2026-11-19 -- v0.3 session B2: NAS ingestion on the ingestion API, two locks, reconciliation on the ingestion side, intake worker; operator-approved edits)
updated_at: 2026-11-19--20-43
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
| Server 2 (`NucBoxK11`) | 192.168.1.95 | `ragnart-search.service` `:8100`; `ragnart-ingest.timer` → `ragnart-ingest.service`; `ragnart-ingest-api.service` `:8101`; `ragnart-intake.timer` → `ragnart-intake.service`; Ollama `bge-m3` `:11434`; llama-swap `:8000` serving the reranker at `/v1/rerank` (tmux session `llama-swap`) |
| NAS | `/mnt/nfs/__Backups/RAGNART--databases/` | `sqlite/rag_state.db`, `sqlite/hub_registry.db`, `logs/`, `qdrant-snapshots/documents/`, `rag-ingest.lock`, `rag-nas-ingest.lock`, `spool/` |

- Repo: `/mnt/nfs/Florian/Gin-AI/projects/RAGNART`. Every `python utils/...` command below
  runs from the repo root after `source ~/Gin-AI/.Gin-AI-python-3.12/bin/activate`.
- **Anything that opens `rag_state.db` runs on Server 2 only.** That includes `sqlite3`,
  `rag_status.py`, `rag_sync_nfs.py`, `rag_reset.py`,
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
| 1 | `rag_sync_nfs.py`: poll the outcome of the previous run's requests, walk every configured root (`/mnt/nfs/Florian`, `/mnt/nfs/La-Familia`), `~/.claude/projects` and the OpenCode API, plan, reconcile against the hub's listing of each source, and submit to the ingestion API: new/changed files as uploads, renamed files as key changes, gone files as tombstones, tombstones older than 24 h as delete requests | about 10 to 15 min (the three listings take about 15 s) |
| 2 | `PRAGMA wal_checkpoint(TRUNCATE)` on `rag_state.db`, then on `hub_registry.db` | seconds |
| 3 | `sqlite-qdrant-snapshot.sh --keep 7` if the newest NAS snapshot is older than 20 h (§3) | ~4–5 min |

The run submits requests; the hub's intake worker (`ragnart-intake.timer`, every 10 minutes,
`ragnart-intake.service`) parses, chunks, embeds and writes the points. The service is killed after
`TimeoutStartSec=7200` (2 h). Env overrides for a manual run: `RAG_LOGFILE`, `RAG_SNAPSHOT_MIN_AGE_H`,
`RAG_FORCE_SNAPSHOT=1`.

Queue states in `ingestion_queue`: `pending` → `processing` → `completed`; a failure goes
back to `pending` until `retry_count` reaches `max_retries` (5), then `ignored` with
`skip_reason` set. `failed` rows are reset to `pending` by the next sync. `processing` means submitted to
the hub and waiting for its outcome, which the next run's poll reads. The retries are counted over runs,
one per run.

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
| `ActiveState=activating` for close to 2 h | `tail -f …/logs/rag-ingest.log` | A large scan, or a slow hub (the listing or an upload) | Let it hit the 2 h cap; the next slot continues. The intake worker drains the requests on its own timer. |
| Rows stuck in `processing` | `python utils/rag_status.py`; `curl -s -H "Authorization: Bearer <secret>" http://127.0.0.1:8101/v1/status/<request_id>` | The request is still waiting in the hub's intake table, or the intake worker is not running | Nothing, when the intake queue is draining: the next run's poll closes the row (`completed`, `failed`, `ignored`). A request the hub no longer knows (404) is marked `failed` and sent again. Check `systemctl status ragnart-intake.timer ragnart-intake.service`. |
| Run started late after a reboot | `systemctl list-timers` | `Persistent=true` fires a missed slot at boot | Expected. |

Manual run (Server 2, when `ActiveState` is `inactive`):

```bash
# The NAS side exactly as the timer runs it:
sudo systemctl start ragnart-ingest.service
# Or only the sync, logging to the shared log:
python utils/rag_sync_nfs.py 2>&1 | tee -a /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log
# Apply the waiting requests now (instead of waiting for the intake timer):
python -m ragnart.intake_worker            # --workers 1 for a gentler load on Ollama
```

The intake worker's `--workers` (default 3) is the number of requests applied at once; with CPU embedding
use `--workers 1`. The Ollama concurrency per item is fixed at 5.

### 2.3 "another writer holds the ingestion lock" (exit 75)

**Symptom.** A NAS run, a sync, the intake worker, a reset or the snapshot script exits with
status 75 and prints one line to stderr, naming the lock path (`rag-ingest.lock` for the index lock,
`rag-nas-ingest.lock` for the NAS-ingestion lock):

```
ragnart: another writer holds the ingestion lock (<path>): pid N on HOST, <command>, since <time>
```

A timer run of `ragnart-ingest.service` or `ragnart-intake.service` shows the same line in its journal;
systemd counts exit 75 as success for both units (`SuccessExitStatus=75`), so a skipped slot is not a
failure. Nothing is written: the writer exits before its first write. The next scheduled slot runs normally.
The snapshot takes both locks, index first; it is skipped (exit 75) while either is held.

**Cause.** Only one index writer may hold the lock at a time, on any host. The writers and the
lock are described in `docs/RAG-strategy.md` §5.6. The holder keeps the lock until it exits; a
second writer does not wait.

**How to see the holder.**

```bash
# The lock file holds one line: the holder's pid, host, command and start time.
cat /mnt/nfs/__Backups/RAGNART--databases/rag-ingest.lock
cat /mnt/nfs/__Backups/RAGNART--databases/rag-nas-ingest.lock

# Is that pid still running? Run this on the host named in the line.
ps -o pid,lstart,args -p <pid>

# Is the lock held right now? Takes and releases it at once, using lockf like the writers
# (`flock --nonblock` is not reliable on this Synology NFSv4.1 mount).
~/Gin-AI/.Gin-AI-python-3.12/bin/python -c "
import fcntl
for name in ('rag-ingest.lock', 'rag-nas-ingest.lock'):
    f = open('/mnt/nfs/__Backups/RAGNART--databases/' + name, 'a')
    try:
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB); print(name, 'lock free')
    except BlockingIOError:
        print(name, 'lock HELD')
"
```

The file is not cleared when its holder exits. So when the probe says `lock free`, the line
in the file is the last holder's, and its pid is normally no longer running.

`python -m ragnart.lock held` (run from the repository root) does not report a holder. It
exits 0 only when the calling process is a step started by a run script under its lock, and 1
otherwise. Use `cat` and the probe above to find out who holds the lock.

**What to do.**

- The named pid is running on its host: wait for it. A timer run ends by itself, or at the
  2 h cap (`TimeoutStartSec=7200`). To stop a timer run now, use
  `sudo systemctl stop ragnart-ingest.service`. For a manual writer, let it finish, or stop it
  with Ctrl-C or `kill <pid>`; the lock is released when it exits.
- The named pid is gone, or the probe says `lock free`: the lock is free. Run the command again.
- **Never delete or rename the lock file.** The lock is a kernel record lock on the open file,
  not on its name. Deleting the file does not release a holder that still has it open. A writer
  that then creates a new file takes a lock on a different file, and two writers could hold
  "the" lock at once.

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
   | `pending` | Not submitted yet | Wait for the next slot, or run the sync (§2.2) |
   | `processing` | Submitted, waiting for the hub | Wait for the intake timer, or run `python -m ragnart.intake_worker`; the next sync's poll closes the row |
   | `ignored`, `skip_reason` = `binary content: NN% printable chars` | Parsed text was under 85 % printable (scanned or encrypted PDF, OLE2 `.pptx`, garbage) | Fix or replace the file; a changed mtime re-queues it automatically |
   | `ignored`, other reason | 5 attempts failed; `skip_reason` is the last error | Fix the cause, then re-queue (§2.10) |
   | `completed` | Points should exist | Go to step 4 |

   `python utils/rag_status.py --ignored --user florian` lists every ignored file with its
   reason. A document whose parsed text is empty is skipped with
   `WARNING: Empty document, skipping` and never produces points.
3. **Why did the scanner not see it?** Check, in this order:
   - The extension is in `include_extensions` in `ragnart.yaml` (`rag.scanner`).
   - No `.ragignore` pattern matches. `ragnart check-ignore -v <path>` prints the file and
     line of the rule that excludes the path, and prints nothing when no rule matches.
     `config/ragignore` applies under every root (`docs/RAGNART-design.md` §1.2).
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

**Cause:** no rule in `config/ragignore` or in a `.ragignore` matches the path, so the file is
included. Rules match the path *as the walk sees it*. The walk does not resolve symlinks, and a
rule matches the exact name, not variants of it:

- `~/.claude` is a symlink to `/mnt/nfs/Florian/Gin-AI/dot.claude-Linux`, so the NFS walk
  sees `dot.claude-Linux/`, not `.claude/`. Both names have a rule.
- `.Gin-AI-python-3.12/` does not match `.Gin-AI-python-3.12.old/`. Venv contents are
  caught generically by `site-packages/`, `.uv-python/` and `.cache/`.

Audit for new symlinked or renamed dot-directories now and then:

```bash
find /mnt/nfs/Florian -maxdepth 3 -type d -name 'dot.*'
ls -la ~ | grep -- '->'
```

**Fix:** add a gitignore pattern for the real on-disk directory (for example
`dot.claude-Linux/`) to `config/ragignore` (every root), or to the `.ragignore` in the
directory above it. Syntax and precedence: `docs/RAGNART-design.md` §1.2. Check the result with
`ragnart check-ignore -v <path>`. The next sync treats the excluded files as gone: it deletes
their Qdrant points (`wait=True`, so the deletes are applied when the sync returns) and drops
their queue rows. The sync is subject to the guards in §2.12. Point counts are final when the
sync exits.

### 2.6 Sync deletes far more than expected, or a source drops to 0

`rag_sync_nfs.py` tombstones **every completed queue row whose path was not seen in this scan**.
Its points stay searchable for 24 hours, then the first sync after that purges them (§2.15).
Anything that hides files from the scan therefore starts that clock for them, unless a guard stops
the tombstoning (§2.12).

| Symptom in the log | Cause | Fix |
|---|---|---|
| `Deletion phase SKIPPED: root(s) not scannable this run` (ERROR, exit 1) | NAS mount missing or stale during the sync. Nothing was deleted. | Check the mount (`ls /mnt/nfs/Florian` first), then run the sync again (§2.12). Only if an earlier sync already deleted points, restoring the last snapshot pair is faster (§8). |
| `claude_chats root not accessible` | `~/.claude` symlink target unreachable | Same as above. |
| `Opencode API unreachable — preserving N existing opencode session(s)` | OpenCode down or auth failed; existing `opencode://` rows are kept on purpose | See §2.7. |
| `la-familia` content not updated after a timer run | Every sync scans the shared root `/mnt/nfs/La-Familia` (owner `la-familia`), so a miss means the root was not readable during that run. The run log then shows `Deletion phase SKIPPED` (§2.12), and no `la-familia` row is tombstoned in that run. | Check the mount (`ls /mnt/nfs/La-Familia`). The next scheduled run picks up the change. Run `python utils/rag_sync_nfs.py` by hand to do it sooner. |

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

- Every ingest first deletes the item's old points by `source_id` and `source_path`. Without the keyword
  index this is a scan over millions of points and exceeds the client timeout (60 s).
  `ensure_collection()` creates the index on every process start; if it is missing, any intake-worker or
  service start recreates it, and Qdrant builds it in the background.
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
  `python utils/rag_sync_nfs.py --user florian`: the sync and its reconciliation compare the restored rows
  with the hub's listing and submit the gap. With no usable copy, rebuild the file from the hub:
  `python utils/rag_sync_nfs.py --rebuild-from-hub` (about 10 minutes and 8 GB of memory; it refuses unless
  the state database has no rows).
- Prevention: every tool that opens the DB runs on Server 2 (§0). `rag_purge_corrupt.py`
  opens it read-only (`mode=ro`), but still belongs on Server 2.

### 2.10 Re-queue an ignored or wrongly skipped file

Server 2, no ingestion run active:

```bash
sqlite3 /mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db \
  "UPDATE ingestion_queue SET status='pending', retry_count=0, skip_reason=NULL, error_msg=NULL
    WHERE file_path = '/exact/path/to/file';"
python utils/rag_sync_nfs.py              # submits the pending row
python -m ragnart.intake_worker --workers 1
```

Touching the file on disk (`touch /path/to/file`) has the same effect at the next sync,
because a newer mtime re-queues a row whatever its status.

### 2.11 Embedding errors (Ollama HTTP 500)

**Symptom:** every chunk of a file fails with HTTP 500 from
`http://192.168.1.95:11434/api/embeddings`; the Ollama journal shows
`cudaMalloc failed: out of memory` / `graph_reserve: failed to allocate compute buffers`.
A single chunk failing with `the input length exceeds the context length` in the Ollama journal
has another cause: see §2.13.

**Check (Server 2):**

```bash
systemctl cat ollama | grep -E 'OLLAMA_(CONTEXT_LENGTH|MAX_LOADED_MODELS)'   # CONTEXT_LENGTH=2048
curl -s http://192.168.1.95:11434/api/ps | python3 -m json.tool             # context_length when loaded
nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv
curl -s http://192.168.1.95:11434/api/embeddings -H 'Content-Type: application/json' \
  -d '{"model":"bge-m3","prompt":"test"}' \
  | python3 -c "import json,sys; print(len(json.load(sys.stdin)['embedding']), 'dims')"    # 1024 dims
```

**Cause / fix:** Ollama sizes the compute buffer from the context length. With the default
(4096) bge-m3 needs ~1.2 GiB of compute buffer on top of its weights, which does not fit
next to the chat model and the reranker. The systemd drop-in
`/etc/systemd/system/ollama.service.d/ragnart-context-length.conf` sets
`Environment="OLLAMA_CONTEXT_LENGTH=2048"` and overrides the 768 set in `ollama.service`. At 2048,
Ollama uses ~1.1 GiB of VRAM. The limit is 2048 because chunks are sized in cl100k tokens (flat
chunks up to 512), and bge-m3 can produce more tokens than cl100k for code, YAML and non-English
text. A chunk that still exceeds 2048 is rejected (§2.13).
If the value is not 2048, check that the drop-in exists, then `sudo systemctl daemon-reload && sudo
systemctl restart ollama`. Failed files are retried by the next runs.

### 2.12 Sync exits 1 with "skipped roots" or "mass delete"

**Symptom:** the sync logs an ERROR and exits 1, and the ingestion log shows
`WARN: sync exited 1 — deletions not applied`. Either:

- `Deletion phase SKIPPED: root(s) not scannable this run: …`, or
- `Deletion phase SKIPPED: N completed file(s) would be tombstoned, more than 10% of the M completed rows …`.

Nothing was deleted. Queueing of new and modified files and their submission still ran, and the WAL
checkpoints and the snapshot still ran (`docs/RAG-strategy.md` §8.2). The ingestion unit ends failed.

**Check:**

```bash
grep -E ' (ERROR|WARNING):' /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log | tail -20
mount | grep /mnt/nfs
ls /mnt/nfs/Florian
```

**Skipped roots — cause and fix:** a root is missing, is not a directory, or (an NFS root) is empty
while the queue holds rows under it. A `claude_chats` root that yields no transcripts while the queue holds rows under it counts the same way. The guard refuses moves and tombstones for the whole run, and
`--allow-mass-delete` does not override it. Fix the mount or the root path, then run the sync by hand:
`python utils/rag_sync_nfs.py --user florian`.

**Mass delete — cause and fix:** more than `rag.sync.max_delete_fraction` (`0.10`) of the
completed rows would be tombstoned. Usually a source vanished (a directory moved or renamed, a
share unmounted in part), or a new `.ragignore` or `config/ragignore` rule covers a large part of
the corpus. Find the cause before overriding. `ragnart check-ignore -v <path>` shows which rule
excludes a sample path. If the deletion is intended, run the sync again with the override:

```bash
python utils/rag_sync_nfs.py --user florian --allow-mass-delete
```

The run logs a WARNING with the counts before it tombstones. A tombstone deletes nothing: the points stay
until the grace period ends (§2.15). The purge that follows deletes Qdrant points before it removes the
row, so an interrupted run is repeated by the next sync.

If the guard trips, the same plan is printed by `python utils/rag_sync_nfs.py --dry-run`, which changes
nothing (§2.15).

### 2.13 Items marked ignored with a reason

`ignored` rows carry a `skip_reason`. The reasons written by admission, by the parsers and by the embedder:

| `skip_reason` starts with | Meaning | Changes when |
|---|---|---|
| `too large: N bytes > 5000000 byte cap` | A text-like file (not docling, ipynb or jsonl) is bigger than `rag.admission.max_text_file_size_bytes`. It is not read. | The file changes, or the cap is changed and the row is re-checked (below). |
| `too large: N chunks > 5000 chunk cap` | The item would produce more chunks than `rag.admission.max_chunks_per_item`. Nothing is embedded. | As above. |
| `binary: NUL byte in first 8KB` | A text-like file has a NUL byte near its start. | As above. |
| `binary: NN.N% replacement chars` | A text-like file's decoded text has more U+FFFD characters than `rag.admission.fffd_ratio_threshold` (0.5%). | As above. |
| `binary content: NN% printable chars` | The parsed text is below 0.85 printable (the quality gate). | As above. |
| `docling failed: <error>` | PDF or DOCX parsing failed. There is no raw-read fallback. The item was retried up to `max_retries` (5), then marked `ignored`. | The file changes, or it is re-queued (§2.10). |
| `Server error '500 Internal Server Error' … /api/embeddings` | A chunk is longer than Ollama's context length (`OLLAMA_CONTEXT_LENGTH=2048`, bge-m3 tokens). Ollama rejects the input; it does not truncate it. Retried up to `max_retries` (5), then marked `ignored`. **Check:** `ssh root@localhost 'journalctl -u ollama --since -1d --no-pager' \| grep 'input length exceeds the context length'`. | The file changes, or it is re-queued (§2.10). A chunk that is still too long is rejected again. |

The admission rules are in `docs/RAGNART-design.md` §5. The sync re-queues an `ignored`
row only when its file's mtime changes. A change to a cap in `ragnart.yaml` applies to new and
changed files. Existing `ignored` and `completed` rows are listed with
`utils/rag_purge_rescan.py`, a report (`docs/RAG-strategy.md` §8.5).

To see the reason for each ignored file:

```bash
ragnart status --ignored                              # retry count and skip_reason per ignored row
python utils/rag_status.py --ignored --user florian   # the same listing from the operator tool
```

### 2.14 Metadata backfill (v2) stalls or reports errors

The backfill is `utils/rag_metadata_backfill.py`; its runbook is `docs/RAG-strategy.md` §5.7.

**Symptom:** `--apply` prints `errors:` greater than 0 and exits 1, or the run stops making progress.

**Check what remains.** Run on Server 2:

```bash
DB=/mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db
sqlite3 "$DB" "SELECT COUNT(*) FROM ingestion_queue WHERE status='completed' AND pipeline_version IS NULL;"
python utils/rag_metadata_backfill.py --dry-run       # per kind; read-only, takes no lock
```

Workflow journals (`claude_journal_skip`) are never migrated. They stay NULL until a sync removes
their rows, so a non-zero count after a full run is expected when journals remain.

**Re-running is safe.** Migrated rows have `pipeline_version = 'p1'` and are skipped. Failed rows still
have it NULL and are retried. Run `--dry-run` first to check the plan, then `--apply`. For a long run,
use the `systemd-run --user` form in `docs/RAG-strategy.md` §5.7. If the lock is held, `--apply` exits
75 and names the holder (§2.3).

**Per-row errors.** Each failed row is logged as `Backfill failed for <key> — <reason>`, in the run's
output (or `journalctl --user -u ragnart-metadata-backfill.service`). A failed row keeps `pipeline_version`
NULL, and the run continues with the next row.

| Reason | Meaning | Fix |
|---|---|---|
| `FileNotFoundError` on a NAS path or a `.jsonl` | The file vanished after it was ingested. The file is read before any Qdrant write, so nothing changed for that row. | Nothing to migrate. Once the backfill has finished, a sync removes the row and its points (`docs/RAG-strategy.md` §5.3): `python utils/rag_sync_nfs.py --user florian`. |
| `HTTPError` with 401 or 403, on `/api/session/…` | The OpenCode password was rejected. | Check `opencode.password_env_file` (§2.7). Re-run. |
| `ValueError` "answered text/html (not JSON)" | The OpenCode API route is wrong or the server is not 2.x. | Check `opencode.api_url` in `ragnart.yaml`. Re-run. |
| `URLError` | The OpenCode server is unreachable from Server 2. | Start the OpenCode server (§2.7), then re-run. |
| Timeout or `ResponseHandlingException` from Qdrant | Qdrant is slow or down (§2.8). | Re-run once Qdrant answers `/healthz`. |

**Collisions are resolved automatically.** One session uuid can exist as two transcript files, for example copies in two Claude Code project folders. The backfill keeps the newer copy (by modification time; an exact tie goes to the greatest path), deletes the Qdrant points under both keys of that session, and re-queues the session as `pending` under its new key. The daemon re-ingests the newer copy on the next run after the timer is restarted (`docs/RAG-strategy.md` §5.7, step 6). The run summary counts these as `collisions resolved: N`. A collision is not an error: `errors:` does not count it, and the run exits 0. Nothing needs to be done by hand.

**Checkpoint file.** `/mnt/nfs/__Backups/RAGNART--databases/sqlite/metadata_backfill.checkpoint` has one
line every 100 rows: UTC time, `done/total` (counts restart at 0 on each run), and the last `file_path`.

```bash
tail -3 /mnt/nfs/__Backups/RAGNART--databases/sqlite/metadata_backfill.checkpoint
```

It is for watching progress only. Nothing reads it back, so a stale file does not affect correctness. A
file that stops growing means the run has stopped: check `systemctl --user show -p ActiveState --value ragnart-metadata-backfill.service`.

### 2.15 Sync plan: moves, tombstones and the grace period

A sync logs its plan before it applies it. `docs/RAG-strategy.md` §5.3 describes the plan and its
guards. `--dry-run` prints the same plan and changes nothing. It takes no lock, so it is safe while
the timer runs. Run it on Server 2:

```bash
cd /mnt/nfs/Florian/Gin-AI/projects/RAGNART && source ~/Gin-AI/.Gin-AI-python-3.12/bin/activate
python utils/rag_sync_nfs.py --dry-run
```

The output starts with two header lines: `roots skipped` and `mass-delete guard tripped`. Then one
block per kind, each with a count: `project upserts`, `project removals`, `moves` (`old -> new`),
`newly missing (tombstoned, points kept)`, `reappeared`, `purge`, `stale queue rows removed`,
`attachment changes`.

**Move, or delete plus new.** A move is listed under `moves` as `old -> new`. Its points stay, and nothing
is re-embedded. A move needs both paths in the same sync: one vanished completed row and one new file
with the same non-empty `content_hash`, and each hash matches exactly once. The empty-file hash never
matches. A session key never moves.

If the old path appears under `newly missing` and the new path is ingested as a new file, the sync saw no
move. Typical causes: the file was moved in one sync and seen again in a later one, the file was also
edited while it moved, or two copies share the hash. The new copy is embedded again. To confirm, grep the
sync log for the move line:

```bash
grep -E 'Moved |Tombstoned \(' /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log | tail -20
```

**Tombstones and the grace period.** A completed file that vanished is not deleted at once. Its row gets
`missing_since`, and its points stay searchable for 24 hours. A later sync that still does not see it
keeps the first `missing_since`. A tombstoned file that reappears has its tombstone cleared, and nothing is
re-ingested. A vanished file that still shows in search within 24 hours is therefore expected. To list the
tombstones (Server 2):

```bash
DB=/mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db
sqlite3 "$DB" "SELECT file_path, missing_since, chunk_count FROM ingestion_queue
  WHERE missing_since IS NOT NULL ORDER BY missing_since;"
```

**Purge.** The first sync that runs after the 24 hours removes the points and then the row. Each purge
writes one log line: `purged <path> chunks=<n> reason=tombstone grace period expired missing_since=<time>`.
Find them with `grep ' purged ' /mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log`. A purge is not
subject to the mass-delete guard, and it still runs when a root was skipped.

**Mass tombstone refused.** More than `rag.sync.max_delete_fraction` (`0.10`) of the completed rows that
are not moves would be tombstoned. The sync logs
`Deletion phase SKIPPED: N completed file(s) would be tombstoned, more than 10% of the M completed rows …`,
tombstones nothing, removes no stale rows and exits 1. Moves still apply. The `--dry-run` header then reads
`mass-delete guard tripped (tombstones refused): yes`. Before overriding, find out why so many files vanished
(§2.6, §2.12). If the change is intended, run the sync with `--allow-mass-delete`.

### 2.16 Reconciliation sends an item again, corrects it, or refuses an orphan delete

Reconciliation (`docs/RAG-strategy.md` §5.8) runs inside every `rag_sync_nfs.py` run, after the scan and before the
submit, for each NAS-side source. It compares `rag_state.db` with the hub's listing (`GET /v1/items`) and acts only
through API requests; it never writes Qdrant. Each source ends with one summary line, `<source>: reconcile {counts}`,
and the run summary prints the same counts under `Reconcile:`.

| Log line | Meaning | What to check |
|---|---|---|
| `<source>: N orphan(s) on the hub, but the local state holds no completed row for this source; no delete sent` (WARNING) | Empty-state guard: the state database has no completed row for the source, as after a lost or reset `rag_state.db`. | Do not delete by hand. Rebuild the rows (`python utils/rag_sync_nfs.py --rebuild-from-hub`), or restore `rag_state.db` from the snapshot triple. |
| `<source>: N orphan(s) on the hub, more than the mass-delete limit L (P% of C completed rows of this source); no delete sent. …` (WARNING) | Mass-delete guard, per source: more orphans than `rag.sync.max_delete_fraction` of this source's completed rows. Nothing was deleted. | Find why the rows vanished (a reset, a lost or restored state DB, a key change). `--dry-run` shows `orphan_would_delete` and `orphan_guard_tripped`. |
| `<source>: deleted orphan <key> (no local row)` | A hub item with no row in the state DB was deleted. | Normally nothing. |
| `<source>: orphan <key> not deleted (retried next run): <error>` (WARNING) | The delete request failed. | `systemctl status ragnart-ingest-api.service`. |
| `<source>: <key> is not listed by the hub; submitting it again` | A completed row whose file is on disk has no points on the hub, for example a hub write that failed after the request was accepted. The row goes back to pending and is sent in the same run. | Normally nothing. If the same key returns every run, check the intake request's error (`/v1/status/<id>`). |
| `<source>: N item(s) on an older pipeline version; M resubmitted this run, K deferred` | Items ingested by an older pipeline version are sent again, at most `rag.nas_ingest.resubmit_batch_size` (200) per run; the rest wait for the next runs. | Expected after a pipeline-version change; the count falls run by run. |
| `<source>: corrected <key> on the hub: ['owner', 'project_id']` | The hub's `owner` or `project_id` differed from the state DB; a `change_metadata` request was sent. | Normally nothing. Many at once point to a wrong owner mapping or project markers. |
| `<source>: metadata of <key> not corrected (retried next run): <error>` (WARNING) | That request failed. | The ingestion API. |
| `<source>: <key> has points that disagree on [...]` (WARNING) | The item's points carry different values of a listed field. | Re-send the item (touch the file, or reset its row to pending), which rewrites all its points. |
| `<source>: the hub listing could not be read; reconcile skipped this run: <error>` (ERROR) | `GET /v1/items` failed. The other sources still reconcile. | The API, and the source's secret (`/v1/info` returns 401 for a wrong one). |
| `<source>: the hub did not answer the reconcile info call; reconcile skipped: <error>` (ERROR) | `GET /v1/info` failed. | As above. |

`missing_zero_chunks_skipped` in the counts is informational: completed rows with zero chunks (empty transcripts,
empty notebooks) have no points, so the hub cannot list them, and they are left alone.

**Count the points under one key** (Server 2, read-only; the collection is `documents` on Server 1):

```bash
curl -s -X POST http://192.168.1.93:6333/collections/documents/points/count \
  -H 'Content-Type: application/json' \
  -d '{"exact": true, "filter": {"must": [{"key": "source_path", "match": {"value": "/exact/path/to/file"}}]}}'
DB=/mnt/nfs/__Backups/RAGNART--databases/sqlite/rag_state.db
sqlite3 "$DB" "SELECT status, chunk_count, pipeline_version, missing_since FROM ingestion_queue
  WHERE file_path = '/exact/path/to/file';"
```

**Re-run.** `python utils/rag_sync_nfs.py --dry-run` reads the live hub, prints the counts for each rule per source,
sends nothing and writes nothing. It takes no lock, so it is safe while the timers run. On a settled corpus every rule
shows zero except `missing_zero_chunks_skipped`.

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

Run `sqlite-qdrant-snapshot.sh` by hand only on Server 2 (it opens `rag_state.db`). It takes the
ingest lock itself, and while a run or another writer holds the lock it exits 75 and names the
holder (§2.3).

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

### OpenCode database backup

`ragnart-opencode-backup.timer` fires at 03:30 local time and runs
`ragnart-opencode-backup.service`, which calls `scripts/opencode-db-backup.sh` as `florian`.
The script copies the live OpenCode database (`/home/florian/.local/share/opencode/opencode.db`)
with `sqlite3 .backup`, which is safe while OpenCode writes to it. It does not take the
ingestion lock (§2.3). Each copy is written to a temp file and renamed into place only when
non-empty, then the newest 7 are kept:

```
/mnt/nfs/__Backups/RAGNART--databases/opencode-backups/<hostname>-<ip>/
  opencode-YYYY-MM-DD.db     (UTC date; ~2.3 GB; mode 0600, directory 0700)
  .opencode-YYYY-MM-DD.db.tmp (only while a run is active, or left by a crashed run)
```

**Check:**

```bash
B=/mnt/nfs/__Backups/RAGNART--databases/opencode-backups
ls -lt $B/*/                                                     # newest copy dated today or yesterday
systemctl list-timers ragnart-opencode-backup.timer              # NEXT must be set
ssh root@localhost 'journalctl -u ragnart-opencode-backup.service -n 20'   # last run's log (journal is root-only)
```

To check a copy's integrity, open it with the immutable URI form, and do not open it with a
plain `sqlite3 -readonly`:

```bash
sqlite3 "file:<path-to-copy>?immutable=1" 'PRAGMA integrity_check;'   # expect: ok
```

The copies are WAL-mode databases. A plain open, even read-only, creates `-wal` and `-shm`
side files next to the copy. `immutable=1` opens it without creating them and without
writing to it.

| Symptom | Cause | Fix |
|---|---|---|
| Newest copy is older than yesterday | Timer missed its slot, or the last run failed | Read the journal lines above. Run the backup now as `florian`: `bash scripts/opencode-db-backup.sh`, or `ssh root@localhost 'systemctl start ragnart-opencode-backup.service'`. A missed slot runs at the next boot (`Persistent=true`). |
| `systemctl list-timers` shows no `NEXT` for the timer, or the unit is missing | Units not installed or not enabled | `sudo bash scripts/install-units.sh` (installs and enables the timer). |
| Journal shows an error from the backup step, or `ERROR: backup is empty` | The source database cannot be read. The unit does not set `OPENCODE_DB_PATH`, so the default path `/home/florian/.local/share/opencode/opencode.db` is used | Check that the file exists and is readable by `florian`. |
| `integrity_check` prints anything other than `ok` | Corrupt copy | Do not restore from it. Use the next-newest copy. The next nightly run writes a new one. |
| `Permission denied` reading a copy as another user | Copies are owner-only (0600) in a 0700 directory, by design (private session data) | Read as `florian`, or with `sudo`. Do not relax the mode. |
| A `.opencode-*.db.tmp` file in the directory, older than the last run | A run crashed mid-copy | Removed automatically at the start of the next run. No action. |
| More than 7 `opencode-*.db` files | Rotation step did not run (the run failed after the copy) | Rotation runs on the next successful backup. Check the journal for the failed step. |

---

## 4. Search

The service (`ragnart.service:app`, `ragnart-search.service`, Server 2 `:8100`) serves
`GET /v1/search?q=…&scope=…&user=…&top_k=…&file_types=…&score_threshold=…&rerank=…`, `GET /v1/projects`
and `GET /health`. `scope` is required (§4.8). Each query: embed with bge-m3 → hybrid dense + sparse
(`sparse_text`) query fused with RRF → cross-encoder rerank → collapse identical copies → per-source cap
→ top_k.

Two ways to run a query, useful to tell the service apart from the pipeline:

```bash
# Through the HTTP service
python utils/rag_smoke_test.py --query "your query" --user florian --expect "substring"
curl -s 'http://192.168.1.95:8100/v1/search?q=your+query&user=florian&top_k=5&scope=all' | python3 -m json.tool
# In-process (same search code, bypasses the service)
python utils/rag_search_cli.py --query "your query" --user florian
python utils/rag_search_cli.py --query "your query" --user florian --no-rerank --file-types claude_chat
```

### 4.1 HTTP 503 "Qdrant unavailable"

**Cause:** Qdrant is unreachable, or answers with a server error, while the search runs. The
search answers 503 `Qdrant unavailable` for that request only.

```bash
curl -s -m 5 http://192.168.1.93:6333/healthz; echo
curl -s -m 10 http://192.168.1.95:8100/health; echo    # {"status":"degraded","qdrant":false} while Qdrant is down
```

**Fix:** restore Qdrant (§5). No service restart is needed: the next search after Qdrant
answers succeeds.

### 4.2 `/health` reports `degraded`

`/health` probes Qdrant's `/healthz` with a 2 s timeout, so it answers within about 2 s.
`{"status":"degraded","qdrant":false}` means Qdrant did not answer within that time: go to §5
and §6. Searches recover by themselves when Qdrant comes back (§4.1).

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
# Server 2. Report only: [N pts] IN_DB/NOT_IN_DB ratio path
python utils/rag_purge_corrupt.py --user florian
python utils/rag_purge_corrupt.py --threshold 0.75 --save /tmp/corrupt_paths.txt
python utils/rag_purge_corrupt.py --source nas        # one source
```

The tool reports and deletes nothing (it has no `--confirm`, `--force` or `--dry-run`). `IN_DB` rows are
cleaned by removing or renaming the file and running a sync, which sends the delete after the grace period. A
`NOT_IN_DB` item (points with no queue row) is an orphan: the next sync's reconciliation deletes it on the
hub, under the guards of `docs/RAG-strategy.md` §5.8; check `python utils/qdrant_status.py` is green before a
large delete drains.

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

It runs the queries in `utils/rag_bench_queries.txt`. Every request to a `/v1/search` URL sends `scope=all`, so the example needs no extra flag. A request to the legacy `/v1/rag/search` URL sends no scope. Only a HARD-check failure (different
score multiset, or a different set of documents within a score group) is a real change.

### 4.7 A config change has no effect

`ragnart-search.service` reads `ragnart.yaml` once at start. After editing it:
`sudo systemctl restart ragnart-search.service`. Ingestion reads it at every run.

### 4.8 `GET /v1/search` answers 422

**Cause:** the `scope` parameter is required. There is no default. The service checks `scope` before any
embedding or Qdrant call, so a 422 costs nothing and changes nothing. A 422 means one of the causes below.

| Response `detail` | Cause | Fix |
|---|---|---|
| A FastAPI validation list with `"loc": ["query", "scope"]` and `"msg": "Field required"` | `scope` is missing | Add `scope=all`, or a project term from `GET /v1/projects`. |
| `<term>: no ragnart project at this path` | An exact path names no ragnart project | Use a path from `GET /v1/projects`. For a whole subtree, end the term with `/`. |
| `<term>: no ragnart project at or below this path` | A `<path>/` term matches no project | As above. |
| `<term>: tag scope must be tag:<key>=<value> with a non-empty key and value` | Malformed tag term | Write `tag:<key>=<value>`. |
| `<term>: no ragnart project carries this tag` | No project has that tag | `GET /v1/projects` lists each project's tags. |
| `empty scope term: …` | A `scope=` with no value | Remove it, or use `all`. |

Every term is validated, even when `all` is present. `scope=all&scope=/typo` is therefore a 422.

**Clients must send `scope`.** Both copies of the `/rag` command must send `scope=all`. A copy that sends
no `scope` gets this 422 on every search.

**Check what was searched.** A 200 response echoes the resolved selection under `scope`: `all`, `global`, and
the projects searched. A search that returns nothing is worth checking against that echo.

**503 `Queue database unavailable`** is a different failure. The service could not open the queue database
when it started, and the scope endpoints stay down until it restarts. Restart it once the database is
reachable, on Server 2: `sudo systemctl restart ragnart-search.service`.

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
  bulk maintenance (a large delete drain by the intake worker, large re-ingests, `utils/rag_source_remove.py --apply`).
- **green with lag > 0**: idle. Vacuum runs only when a segment has ≥ 20 % deleted
  vectors, so spread-out deletes leave a stable lag; harmless.
- On this hybrid collection the "Indexed" figure counts dense **and** sparse vectors, so
  the lag shown is around 50 % even when idle. Compare "Indexed" with 2 × "Points" for the
  real backlog.

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
| Ingestion runs (sync, checkpoints, snapshot) | `/mnt/nfs/__Backups/RAGNART--databases/logs/rag-ingest.log` (+ `.1.gz` … `.7.gz`) | `grep -E ' (ERROR|WARNING):'`; run markers `=== RAG ingestion starting/complete ===`; live: `tail -f <log>` |
| Hub intake (the points are written here) | journal of `ragnart-intake.service` (Server 2) | `ssh root@localhost 'journalctl -u ragnart-intake.service --since today --no-pager'` |
| Ingestion API | journal of `ragnart-ingest-api.service` (Server 2) | `ssh root@localhost 'journalctl -u ragnart-ingest-api.service --since -1h --no-pager'` |
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
- Stop `ragnart-ingest.timer`, `ragnart-intake.timer` and `ragnart-ingest-api.service` first, and keep `ragnart-search.service` from recreating an
  empty `documents` collection (§5.3) while Qdrant is being restored.
- Afterwards run `python utils/rag_sync_nfs.py --user florian` and the intake worker (`python -m ragnart.intake_worker`, §2.2) to
  pick up everything changed since the snapshot; ingestion is idempotent.
- `utils/rag_reset.py [--user OWNER] [--yes]` resets the queue to `pending` (all rows, or one
  owner) for a full re-ingest. A full reset refuses when `documents` is an alias: it exits
  non-zero and deletes nothing. Restore from a snapshot pair instead (`docs/RAG-strategy.md`
  §9.3). `--dry-run` prints the points and rows it would delete and writes nothing. It is a last
  resort, not a repair step.
