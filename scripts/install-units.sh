#!/usr/bin/env bash
# Install RAGNART's systemd units; retire SoHoAI's rag-ingest.timer.
# Idempotent. Must run as root. Refuses if an ingestion run is active
# under either name. Never enables both ingestion timers.
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Run with sudo." >&2
    exit 1
fi

REPO="/mnt/nfs/Florian/Gin-AI/projects/RAGNART"
UNIT_DIR="/etc/systemd/system"

if systemctl is-active --quiet rag-ingest.service; then
    echo "REFUSING: rag-ingest.service (SoHoAI) is currently active — wait for it to finish." >&2
    exit 1
fi
if systemctl is-active --quiet ragnart-ingest.service; then
    echo "REFUSING: ragnart-ingest.service is currently active — wait for it to finish." >&2
    exit 1
fi

echo "Installing RAGNART systemd units..."
cp "${REPO}/scripts/ragnart-search.service" "${UNIT_DIR}/ragnart-search.service"
cp "${REPO}/scripts/ragnart-ingest.service" "${UNIT_DIR}/ragnart-ingest.service"
cp "${REPO}/scripts/ragnart-ingest.timer"   "${UNIT_DIR}/ragnart-ingest.timer"

systemctl daemon-reload

echo "Disabling SoHoAI's rag-ingest.timer (never run both timers)..."
systemctl disable --now rag-ingest.timer || echo "  (already disabled or not installed — OK)"

echo "Enabling RAGNART's units..."
systemctl enable --now ragnart-ingest.timer
systemctl enable --now ragnart-search.service

echo
echo "=== Status ==="
systemctl status ragnart-search.service --no-pager -l || true
systemctl list-timers ragnart-ingest.timer rag-ingest.timer --no-pager || true
