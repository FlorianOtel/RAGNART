#!/usr/bin/env bash
# Install RAGNART's systemd units and logrotate config on Server 2 (NucBoxK11, .95),
# and remove what SoHoAI left behind (rag-ingest.service/.timer, its logrotate file).
#
# Run ON SERVER 2 as root:
#   sudo bash /mnt/nfs/Florian/Gin-AI/projects/RAGNART/scripts/install-units.sh
#
# Idempotent. Refuses while an ingestion run is active. Never leaves two ingestion
# timers enabled. (Qdrant on Server 1 has its own installer: scripts/qdrant/install-qdrant.sh.)
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Run with sudo." >&2
    exit 1
fi

REPO="/mnt/nfs/Florian/Gin-AI/projects/RAGNART"
UNIT_DIR="/etc/systemd/system"

for unit in rag-ingest.service ragnart-ingest.service; do
    if systemctl is-active --quiet "$unit"; then
        echo "REFUSING: $unit is currently active -- wait for it to finish." >&2
        exit 1
    fi
done

echo "Installing RAGNART systemd units..."
cp "${REPO}/scripts/ragnart-search.service" "${UNIT_DIR}/ragnart-search.service"
cp "${REPO}/scripts/ragnart-ingest.service" "${UNIT_DIR}/ragnart-ingest.service"
cp "${REPO}/scripts/ragnart-ingest.timer"   "${UNIT_DIR}/ragnart-ingest.timer"
cp "${REPO}/scripts/ragnart-opencode-backup.service" "${UNIT_DIR}/ragnart-opencode-backup.service"
cp "${REPO}/scripts/ragnart-opencode-backup.timer"   "${UNIT_DIR}/ragnart-opencode-backup.timer"

echo "Installing logrotate config (replaces SoHoAI's /etc/logrotate.d/rag-ingest)..."
install -m 0644 "${REPO}/scripts/ragnart-ingest-logrotate" /etc/logrotate.d/ragnart-ingest
rm -f /etc/logrotate.d/rag-ingest

echo "Removing SoHoAI's retired rag-ingest units (never run both timers)..."
systemctl disable --now rag-ingest.timer 2>/dev/null || true
rm -f "${UNIT_DIR}/rag-ingest.timer" "${UNIT_DIR}/rag-ingest.service"
systemctl daemon-reload
systemctl reset-failed rag-ingest.service 2>/dev/null || true

echo "Enabling RAGNART's units..."
systemctl enable --now ragnart-ingest.timer
systemctl enable --now ragnart-search.service
systemctl enable --now ragnart-opencode-backup.timer

echo
echo "=== Status ==="
systemctl status ragnart-search.service --no-pager -l | head -5 || true
systemctl list-timers ragnart-ingest.timer --no-pager || true
logrotate -d /etc/logrotate.d/ragnart-ingest 2>&1 | grep -iE "error|considering|rotating pattern" || true
