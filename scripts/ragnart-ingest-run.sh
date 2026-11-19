#!/usr/bin/env bash
# RAGNART ingestion wrapper (the NAS ingestion run, session B2).
# Run by systemd ragnart-ingest.service or directly for manual/debug use.
#
# This script holds the NAS-ingestion lock (rag.nas_ingest.lock in ragnart.yaml) for the WHOLE run:
# sync -> rag_state.db WAL checkpoint -> hub_registry WAL checkpoint -> snapshot. It does not hold the
# index lock (rag.ingest_lock): the sync submits to the ingestion API, and only the intake worker and hub
# maintenance write the index. The snapshot takes both locks itself (index first, then NAS, see
# scripts/sqlite-qdrant-snapshot.sh), so the restore triple is taken while neither writer runs.
#
# It takes the NAS lock via a `python -m ragnart.lock run --which nas` wrapper, re-invoking itself under
# that lock (see the guard below the `cd`). The child steps (the sync, the snapshot) see the NAS lock as held
# by an ancestor, so they do not take it again. If another NAS run holds the lock, this run exits 75
# (EXIT_LOCK_HELD) at once and names the holder. The service unit treats 75 as a skipped slot.
#
# Env overrides:
#   RAGNART_DATA_ROOT        data root (default /mnt/nfs/__Backups/RAGNART--databases)
#   RAG_LOGFILE              log file path (default: NAS logs dir, see below)
#   RAG_SNAPSHOT_MIN_AGE_H   take a snapshot only if the newest NAS snapshot is older
#                            than this many hours (default 20 -> one snapshot per day,
#                            normally on the 01:00 run)
#   RAG_FORCE_SNAPSHOT=1     take a snapshot on this run regardless of age
set -euo pipefail

VENV="$HOME/Gin-AI/.Gin-AI-python-3.12/bin/activate"
PROJECT="/mnt/nfs/Florian/Gin-AI/projects/RAGNART"
DATA_ROOT="${RAGNART_DATA_ROOT:-/mnt/nfs/__Backups/RAGNART--databases}"
LOGFILE="${RAG_LOGFILE:-${DATA_ROOT}/logs/rag-ingest.log}"
SNAPSHOT_MIN_AGE_H="${RAG_SNAPSHOT_MIN_AGE_H:-20}"
SNAPSHOT_KEEP=7   # NAS retention: 7 daily restore pairs (~150 GB)

mkdir -p "$(dirname "$LOGFILE")"
log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)  $*" | tee -a "$LOGFILE"; }

# shellcheck disable=SC1090
source "$VENV"
cd "$PROJECT"

# --- Single-writer guard: hold the NAS-ingestion lock for the whole run -------------
# Why: the sync and the rag_state.db checkpoint write the NAS side's state database and submit to the hub.
# Two NAS runs must not overlap, so the whole run is one lock-holding unit.
#
# How: when this process is not already under the NAS lock holder, `held --which nas` fails and `exec`
# replaces this process with the `run --which nas` wrapper. The wrapper takes the lock and re-invokes this
# script, so the second invocation's `held` check succeeds and the body runs. The venv python is named
# explicitly.
SELF="$(readlink -f "$0")"
VENV_PY="$HOME/Gin-AI/.Gin-AI-python-3.12/bin/python"
if ! "$VENV_PY" -m ragnart.lock held --which nas; then
    exec "$VENV_PY" -m ragnart.lock run --which nas -- bash "$SELF" "$@"
fi

# Logged here, after the guard, so the start line is written once per run (by the
# process that holds the lock), not once by the outer process and again by the wrapper.
log "=== RAG ingestion starting ==="

# A failing sync (refused deletion, or a crash) must not stop the checkpoints and the snapshot from running.
# Its exit code is recorded here and returned at the very end, so systemd still marks the unit failed.
# SYNC_RC stays 0 while every step succeeds, and keeps the first non-zero code otherwise.
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

log "--- SQLite WAL checkpoint ---"
DB_PATH="${DATA_ROOT}/sqlite/rag_state.db"
sqlite3 "$DB_PATH" "PRAGMA wal_checkpoint(TRUNCATE);" 2>&1 | tee -a "$LOGFILE" || \
  log "WARN: WAL checkpoint failed (non-fatal)"

# The hub registry gets the same checkpoint when it exists. Before the registry migration the file is
# absent, and that is not an error. The intake worker may write the registry at the same time, under the
# index lock this run does not hold. A checkpoint that cannot finish then is not an error either: the
# snapshot checkpoints the registry again under both locks.
REGISTRY_PATH="${DATA_ROOT}/sqlite/hub_registry.db"
if [[ -e "$REGISTRY_PATH" ]]; then
    log "--- hub_registry WAL checkpoint ---"
    sqlite3 "$REGISTRY_PATH" "PRAGMA wal_checkpoint(TRUNCATE);" 2>&1 | tee -a "$LOGFILE" || \
      log "WARN: hub_registry WAL checkpoint failed (non-fatal)"
else
    log "--- hub_registry WAL checkpoint skipped: no hub_registry.db ---"
fi

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
    # The snapshot takes the index lock itself. The `|| snap_rc=$?` keeps set -e from ending the run here,
    # so the exit code can be classified below.
    snap_rc=0
    bash scripts/sqlite-qdrant-snapshot.sh --keep "$SNAPSHOT_KEEP" 2>&1 | tee -a "$LOGFILE" || snap_rc=$?
    if (( snap_rc == 75 )); then
        # Exit 75 is EXIT_LOCK_HELD: the intake worker or hub maintenance holds the index lock. This is a skipped
        # snapshot, not a failure. The next run takes the next daily pair.
        log "--- snapshot skipped: the index lock is held by another writer (exit 75) ---"
    elif (( snap_rc != 0 )); then
        log "ERROR: snapshot exited $snap_rc — see log above"
        if (( SYNC_RC == 0 )); then
            SYNC_RC=$snap_rc
        fi
    fi
else
    log "--- snapshot skipped: newest pair is younger than ${SNAPSHOT_MIN_AGE_H} h ---"
fi

log "=== RAG ingestion complete ==="

# Return the sync's failure code, if any, so systemd marks the unit failed. The checkpoint and snapshot steps
# above have already run by this point.
exit "$SYNC_RC"
