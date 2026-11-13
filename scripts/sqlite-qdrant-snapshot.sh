#!/usr/bin/env bash
# Take a Qdrant snapshot of the `documents` collection plus a matching copy of
# rag_state.db, and keep them on the NAS as a restore pair.
#
# Where things live (retention policy: docs/RAG-strategy.md §9.1):
#   - Qdrant writes the snapshot to its LOCAL snapshots_path on Server 1
#     (/var/lib/qdrant/snapshots, see scripts/qdrant/qdrant-config.yaml). That copy is
#     transient: it is downloaded to the NAS and then deleted, so Server 1's NVMe holds
#     at most one snapshot (~21 GB), and only while this script runs.
#   - The NAS keeps the newest KEEP restore pairs (default 7 = one week of daily pairs):
#       ${DATA_ROOT}/qdrant-snapshots/documents/<name>.snapshot
#       ${DATA_ROOT}/qdrant-snapshots/documents/rag_state-<timestamp>.db
#
# Safety nets against filling a disk:
#   1. Before creating a snapshot, any snapshot Qdrant still holds for the collection
#      (left by an earlier crashed run) is deleted through the Qdrant API.
#   2. Before creating a snapshot, stale partial downloads (*.tmp) on the NAS are removed.
#   3. The NAS must have at least MIN_FREE_GB free, or the run aborts before creating anything.
#   4. On any failure after creation, an EXIT trap deletes the Qdrant-local snapshot and
#      the partial download.
#   5. Rotation keeps exactly KEEP pairs and removes orphan rag_state-*.db copies.
#
# Usage: bash scripts/sqlite-qdrant-snapshot.sh [--keep N]   (default: keep 7)
# Env:   RAGNART_DATA_ROOT  (default /mnt/nfs/__Backups/RAGNART--databases)
set -euo pipefail

QDRANT_URL="http://192.168.1.93:6333"
COLLECTION="documents"   # Qdrant alias -> documents_new
DATA_ROOT="${RAGNART_DATA_ROOT:-/mnt/nfs/__Backups/RAGNART--databases}"
SNAPSHOTS_NFS_DIR="${DATA_ROOT}/qdrant-snapshots/${COLLECTION}"
SQLITE_DB="${DATA_ROOT}/sqlite/rag_state.db"
KEEP=7
MIN_FREE_GB=60           # ~3x one snapshot (~21 GB); abort below this

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)  $*"; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep) KEEP="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

mkdir -p "${SNAPSHOTS_NFS_DIR}"

# --- Safety net 1: delete any snapshot Qdrant still holds for this collection -------
# Normally there are none; a crashed earlier run could have left one on Server 1's NVMe.
STALE_NAMES=$(curl -sf "${QDRANT_URL}/collections/${COLLECTION}/snapshots" \
    | python3 -c "import sys,json; print('\n'.join(s['name'] for s in json.load(sys.stdin)['result']))")
for stale in ${STALE_NAMES}; do
    log "Deleting stale Qdrant-local snapshot: ${stale}"
    curl -sf -X DELETE "${QDRANT_URL}/collections/${COLLECTION}/snapshots/${stale}" > /dev/null
done

# --- Safety net 2: remove partial downloads left by an earlier crashed run -----------
# Only one snapshot run can be active at a time (it runs inside the single
# ragnart-ingest.service oneshot), so any *.tmp here is stale.
find "${SNAPSHOTS_NFS_DIR}" -maxdepth 1 -type f -name "*.tmp" -print -delete \
    | sed "s|^|$(date -u +%Y-%m-%dT%H:%M:%SZ)  Deleted stale partial download: |"

# --- Safety net 3: enough free space on the NAS? ------------------------------------
FREE_GB=$(df --output=avail -BG "${SNAPSHOTS_NFS_DIR}" | tail -1 | tr -dc '0-9')
if [[ "${FREE_GB}" -lt "${MIN_FREE_GB}" ]]; then
    log "ERROR: only ${FREE_GB} GB free on the NAS (need ${MIN_FREE_GB} GB) -- snapshot skipped" >&2
    exit 1
fi

# Checkpoint SQLite WAL first so rag_state.db is fully self-contained before
# the Qdrant snapshot is created -- both reflect the same logical point in time.
log "Checkpointing SQLite WAL..."
sqlite3 "${SQLITE_DB}" "PRAGMA wal_checkpoint(TRUNCATE);"

# Create snapshot (written by Qdrant to its local snapshots_path on Server 1)
log "Creating snapshot for '${COLLECTION}'..."
RESPONSE=$(curl -sf -X POST "${QDRANT_URL}/collections/${COLLECTION}/snapshots")
NAME=$(python3 -c "import sys,json; print(json.load(sys.stdin)['result']['name'])" <<< "$RESPONSE")
log "Created: ${NAME}"

# --- Safety net 4: on any failure from here on, delete the Qdrant-local snapshot and
# the partial download, so nothing is orphaned on Server 1 or on the NAS. On success
# the EXIT trap receives rc=0 and does nothing.
cleanup_on_failure() {
    local rc="$1"
    if [[ "${rc}" -ne 0 ]]; then
        log "ERROR: snapshot run failed (exit ${rc}) -- cleaning up Qdrant-local snapshot and .tmp" >&2
        if [[ -n "${NAME:-}" ]]; then
            # The snapshot may already be deleted on the success path; a 404 is fine.
            curl -sf -X DELETE "${QDRANT_URL}/collections/${COLLECTION}/snapshots/${NAME}" >/dev/null 2>&1 || true
        fi
        if [[ -n "${TEMP_FILE:-}" ]]; then
            rm -f "${TEMP_FILE}"
        fi
    fi
}
trap 'cleanup_on_failure $?' EXIT

# Download snapshot to the NAS
log "Downloading to NFS..."
TEMP_FILE="${SNAPSHOTS_NFS_DIR}/${NAME}.tmp"
FINAL_FILE="${SNAPSHOTS_NFS_DIR}/${NAME}"

curl -sf --max-time 7200 \
    "${QDRANT_URL}/collections/${COLLECTION}/snapshots/${NAME}" \
    -o "${TEMP_FILE}"

# Verify download is non-empty
FILE_SIZE=$(stat -c%s "${TEMP_FILE}" 2>/dev/null || echo "0")
if [[ "${FILE_SIZE}" -le 0 ]]; then
    log "ERROR: Downloaded snapshot is empty (${FILE_SIZE} bytes)" >&2
    exit 1
fi

mv "${TEMP_FILE}" "${FINAL_FILE}"
log "Download complete ($(numfmt --to=iec-i --suffix=B "${FILE_SIZE}" 2>/dev/null || echo "${FILE_SIZE} bytes"))"

# Delete the transient snapshot from Qdrant (Server 1 local NVMe)
log "Deleting snapshot from Qdrant local storage..."
curl -sf -X DELETE "${QDRANT_URL}/collections/${COLLECTION}/snapshots/${NAME}" > /dev/null
log "Qdrant local copy deleted"

# Copy rag_state.db alongside the Qdrant snapshot with a matching timestamp.
# NAME is e.g. documents_new-8393594205839792-2026-11-13-12-10-48.snapshot
SNAP_TS="${NAME#*-*-}"          # -> 2026-11-13-12-10-48.snapshot
SNAP_TS="${SNAP_TS%.snapshot}"  # -> 2026-11-13-12-10-48
SQLITE_DST="${SNAPSHOTS_NFS_DIR}/rag_state-${SNAP_TS}.db"
log "Copying rag_state.db -> $(basename "${SQLITE_DST}")..."
cp "${SQLITE_DB}" "${SQLITE_DST}"
log "SQLite copy complete ($(numfmt --to=iec-i --suffix=B "$(stat -c%s "${SQLITE_DST}")" 2>/dev/null || echo "?"))"

# --- Safety net 5: rotation. Keep the KEEP newest pairs, delete the rest. ------------
log "Rotating snapshots (keeping ${KEEP} most recent)..."
find "${SNAPSHOTS_NFS_DIR}" -maxdepth 1 -type f -name "*.snapshot" -printf '%T@ %p\n' \
    | sort -rn \
    | tail -n +"$((KEEP + 1))" \
    | cut -d' ' -f2- \
    | while read -r old_file; do
        rm -f "${old_file}"
        log "Deleted old snapshot: $(basename "${old_file}")"
    done

# Remove any rag_state-*.db whose matching snapshot no longer exists (its pair was
# rotated out above, or it was left behind by an older script version).
for db_copy in "${SNAPSHOTS_NFS_DIR}"/rag_state-*.db; do
    [[ -e "${db_copy}" ]] || continue
    ts="$(basename "${db_copy}" .db)"; ts="${ts#rag_state-}"
    if ! compgen -G "${SNAPSHOTS_NFS_DIR}/*-${ts}.snapshot" > /dev/null; then
        rm -f "${db_copy}"
        log "Deleted unpaired SQLite copy: $(basename "${db_copy}")"
    fi
done

log "Snapshot pairs in NFS: $(find "${SNAPSHOTS_NFS_DIR}" -maxdepth 1 -type f -name "*.snapshot" | wc -l)/${KEEP}"
log "Snapshot complete"
