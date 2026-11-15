#!/usr/bin/env bash
# Take a nightly online backup of OpenCode's database (opencode.db) and keep the
# newest KEEP copies (7) on the NAS.
#
# Where things live:
#   - Source: the live OpenCode database on this host (default below; override with
#     OPENCODE_DB_PATH). OpenCode keeps writing to it while the backup runs. `sqlite3
#     .backup` uses SQLite's online backup API, which is safe on a live database and
#     produces a consistent copy.
#   - Destination: ${DEST_DIR}/opencode-<YYYY-MM-DD>.db (UTC date). One directory per
#     host and IP, so backups from several machines never collide.
#   - Retention: the newest KEEP copies in DEST_DIR; older ones are deleted.
#
# NOT an index writer: this script does NOT take the ingestion lock. It must not take
# rag.ingest_lock (ragnart.yaml; ragnart/lock.py). It only reads the OpenCode database,
# which RAGNART does not own, and only writes to its own backup directory. Taking the
# ingestion lock would make a nightly backup wait for, or block, an ingestion run.
#
# Safety nets:
#   1. Stale partial backups (.opencode-*.db.tmp) from an earlier crashed run are
#      removed before a new backup starts.
#   2. The backup is written to a temp file and renamed into place only once it is
#      non-empty, so a dated opencode-*.db is always complete.
#   3. Rotation matches only opencode-*.db in DEST_DIR. Temp files and other files are
#      never touched.
#
# Usage: bash scripts/opencode-db-backup.sh
# Env:   OPENCODE_DB_PATH  (default /home/florian/.local/share/opencode/opencode.db)
set -euo pipefail

# Create backup files owner-only (0600), matching the source database: the backups hold private session data.
umask 077

SRC="${OPENCODE_DB_PATH:-/home/florian/.local/share/opencode/opencode.db}"
KEEP=7

# HOST_NAME, not HOSTNAME: bash sets HOSTNAME itself, so reusing it would be confusing.
HOST_NAME="$(hostname)"
IP="$(hostname -I | awk '{print $1}')"
DEST_DIR="/mnt/nfs/__Backups/RAGNART--databases/opencode-backups/${HOST_NAME}-${IP}"
TODAY="$(date -u +%Y-%m-%d)"
TMP="${DEST_DIR}/.opencode-${TODAY}.db.tmp"
FINAL="${DEST_DIR}/opencode-${TODAY}.db"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)  $*"; }

mkdir -p "${DEST_DIR}"

# --- Safety net 1: remove partial backups left by an earlier crashed run -------------
# Only one backup run can be active at a time (it runs inside the single
# ragnart-opencode-backup.service oneshot), so any such temp file is stale.
find "${DEST_DIR}" -maxdepth 1 -type f -name ".opencode-*.db.tmp" -print -delete \
    | sed "s|^|$(date -u +%Y-%m-%dT%H:%M:%SZ)  Deleted stale partial backup: |"

# --- Back up the live database through SQLite's online backup API -------------------
log "Backing up ${SRC} -> $(basename "${FINAL}")..."
sqlite3 "${SRC}" ".backup '${TMP}'"

# --- Safety net 2: refuse an empty backup, and rename into place only when complete ---
FILE_SIZE="$(stat -c%s "${TMP}" 2>/dev/null || echo 0)"
if [[ "${FILE_SIZE}" -le 0 ]]; then
    log "ERROR: backup is empty (${FILE_SIZE} bytes) -- not kept" >&2
    rm -f "${TMP}"
    exit 1
fi
# Same directory as TMP, so this rename is atomic.
mv "${TMP}" "${FINAL}"
log "Backup complete ($(numfmt --to=iec-i --suffix=B "${FILE_SIZE}" 2>/dev/null || echo "${FILE_SIZE} bytes"))"

# --- Safety net 3: rotation. Keep the KEEP newest copies, delete the rest. ----------
log "Rotating backups (keeping ${KEEP} most recent)..."
find "${DEST_DIR}" -maxdepth 1 -type f -name "opencode-*.db" -printf '%T@ %p\n' \
    | sort -rn \
    | tail -n +"$((KEEP + 1))" \
    | cut -d' ' -f2- \
    | while read -r old_file; do
        rm -f "${old_file}"
        log "Deleted old backup: $(basename "${old_file}")"
    done

log "Backups in NFS: $(find "${DEST_DIR}" -maxdepth 1 -type f -name "opencode-*.db" | wc -l)/${KEEP}"
log "OpenCode backup complete"
