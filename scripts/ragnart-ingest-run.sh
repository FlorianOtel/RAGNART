#!/usr/bin/env bash
# RAGNART ingestion wrapper.
# Run by systemd ragnart-ingest.service or directly for manual/debug use.
# This script holds the ingestion lock (rag.ingest_lock in ragnart.yaml) for the WHOLE
# run: sync -> daemon -> WAL checkpoint -> snapshot. It takes the lock via a
# `python -m ragnart.lock run` wrapper, re-invoking itself under that lock (see the
# guard below the `cd`). The child steps (sync, daemon, snapshot) detect that their
# ancestor holds the lock, so they do not try to take it again. If another writer
# holds the lock, this run exits 75 (EXIT_LOCK_HELD) at once and names the holder.
#
# Env overrides:
#   RAG_WORKERS      file-level parallelism (default 3, GPU embed; use 1 for CPU embed)
#   RAG_BATCH        chunk-level Ollama concurrency (default 20, GPU; use 5 for CPU)
#   RAG_LOGFILE      log file path (default: NAS logs dir, see below)
#   RAGNART_DATA_ROOT        data root (default /mnt/nfs/__Backups/RAGNART--databases)
#   RAG_SNAPSHOT_MIN_AGE_H   take a snapshot only if the newest NAS snapshot is older
#                            than this many hours (default 20 -> one snapshot per day,
#                            normally on the 01:00 run)
#   RAG_FORCE_SNAPSHOT=1     take a snapshot on this run regardless of age
#
# GPU embed (Ollama on Server 2, RTX 5070): RAG_WORKERS=3 RAG_BATCH=20
# CPU embed (Ollama on Server 1, local):    RAG_WORKERS=1 RAG_BATCH=5
set -euo pipefail

VENV="$HOME/Gin-AI/.Gin-AI-python-3.12/bin/activate"
PROJECT="/mnt/nfs/Florian/Gin-AI/projects/RAGNART"
DATA_ROOT="${RAGNART_DATA_ROOT:-/mnt/nfs/__Backups/RAGNART--databases}"
LOGFILE="${RAG_LOGFILE:-${DATA_ROOT}/logs/rag-ingest.log}"
WORKERS="${RAG_WORKERS:-3}"
BATCH="${RAG_BATCH:-20}"
SNAPSHOT_MIN_AGE_H="${RAG_SNAPSHOT_MIN_AGE_H:-20}"
SNAPSHOT_KEEP=7   # NAS retention: 7 daily restore pairs (~150 GB)

mkdir -p "$(dirname "$LOGFILE")"
log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)  $*" | tee -a "$LOGFILE"; }

# shellcheck disable=SC1090
source "$VENV"
cd "$PROJECT"

# --- Single-writer guard: hold the ingestion lock for the whole run ------------------
# Why: sync, daemon, WAL checkpoint and snapshot all write the index. They must not
# interleave with each other or with any other writer, so the whole run is one
# lock-holding unit.
#
# How: when this process is not already under a lock holder, `held` fails and `exec`
# replaces this process with the `run` wrapper. The wrapper takes the lock and
# re-invokes this script with RAGNART_LOCK_HOLDER set, so the second invocation's
# `held` check succeeds and the body runs. Child steps see the same holder through
# their ancestor chain. The venv python is named explicitly.
SELF="$(readlink -f "$0")"
VENV_PY="$HOME/Gin-AI/.Gin-AI-python-3.12/bin/python"
if ! "$VENV_PY" -m ragnart.lock held; then
    exec "$VENV_PY" -m ragnart.lock run -- bash "$SELF" "$@"
fi

# Logged here, after the guard, so the start line is written once per run (by the
# process that holds the lock), not once by the outer process and again by the wrapper.
log "=== RAG ingestion starting  workers=$WORKERS  batch=$BATCH ==="

# A failing sync (refused deletion, or a crash) must not stop the ingest daemon from
# running. Its exit code is recorded here and returned at the very end, so systemd
# still marks the unit failed. SYNC_RC stays 0 while every sync succeeds, and keeps
# the first non-zero code otherwise.
SYNC_RC=0

# One sync call scans every configured root (La-Familia included), so no per-user loop.
log "--- rag_sync_nfs.py ---"
# `|| rc=$?` stops set -e from aborting on a failing sync. pipefail makes the
# pipeline's status the sync's own exit code, not the exit code of tee.
rc=0
python utils/rag_sync_nfs.py 2>&1 | tee -a "$LOGFILE" || rc=$?
if (( rc != 0 )); then
    log "WARN: sync exited $rc — deletions not applied; see log above"
    if (( SYNC_RC == 0 )); then
        SYNC_RC=$rc
    fi
fi

log "--- rag_ingest_daemon.py ---"
# The run script already holds the ingestion lock (see the header), so the daemon
# runs under its ancestor's lock and does not take it again.
# Do NOT tee here: --log-file writes directly to $LOGFILE; tee would double log lines.
# stderr goes to journald via StandardError=journal in the service unit.
python utils/rag_ingest_daemon.py \
    --workers "$WORKERS" --batch "$BATCH" --log-file "$LOGFILE"

log "--- SQLite WAL checkpoint ---"
DB_PATH="${DATA_ROOT}/sqlite/rag_state.db"
sqlite3 "$DB_PATH" "PRAGMA wal_checkpoint(TRUNCATE);" 2>&1 | tee -a "$LOGFILE" || \
  log "WARN: WAL checkpoint failed (non-fatal)"

# Snapshot policy: at most one restore pair per day. Ingestion itself runs 4x/day and
# is idempotent, so a restore to the last daily pair loses nothing that the next run
# will not re-ingest. Snapshot when the newest pair on the NAS is older than
# SNAPSHOT_MIN_AGE_H hours (or none exists), or when RAG_FORCE_SNAPSHOT=1.
SNAP_DIR="${DATA_ROOT}/qdrant-snapshots/documents"
NEWEST_SNAP=$(find "$SNAP_DIR" -maxdepth 1 -type f -name "*.snapshot" -printf '%T@\n' 2>/dev/null | sort -rn | head -1)
NOW=$(date +%s)
if [[ "${RAG_FORCE_SNAPSHOT:-0}" == "1" ]] || [[ -z "$NEWEST_SNAP" ]] \
   || (( NOW - ${NEWEST_SNAP%.*} > SNAPSHOT_MIN_AGE_H * 3600 )); then
    log "--- sqlite-qdrant-snapshot.sh (keep ${SNAPSHOT_KEEP}) ---"
    bash scripts/sqlite-qdrant-snapshot.sh --keep "$SNAPSHOT_KEEP" 2>&1 | tee -a "$LOGFILE"
else
    log "--- snapshot skipped: newest pair is younger than ${SNAPSHOT_MIN_AGE_H} h ---"
fi

log "=== RAG ingestion complete ==="

# Return the sync's failure code, if any, so systemd marks the unit failed. The daemon
# and the checkpoint and snapshot steps above have already run by this point.
exit "$SYNC_RC"
