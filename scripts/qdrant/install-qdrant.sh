#!/usr/bin/env bash
# Install RAGNART's Qdrant config and systemd unit on Server 1 (um690, 192.168.1.93).
#
# Run ON SERVER 1 as root:
#   sudo bash /mnt/nfs/Florian/Gin-AI/projects/RAGNART/scripts/qdrant/install-qdrant.sh
#
# What it does (idempotent):
#   1. Copies qdrant-config.yaml to /etc/qdrant/config.yaml (a LOCAL copy, so a slow
#      or missing NFS mount at boot cannot make Qdrant fall back to built-in defaults).
#   2. Copies qdrant.service to /etc/systemd/system/qdrant.service.
#   3. daemon-reload, then restarts Qdrant and waits until it answers on :6333.
#
# Qdrant is unavailable for a few seconds during the restart (search returns 503).
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Run as root (sudo)." >&2
    exit 1
fi

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

install -D -m 0644 "${SRC_DIR}/qdrant-config.yaml" /etc/qdrant/config.yaml
install -m 0644 "${SRC_DIR}/qdrant.service" /etc/systemd/system/qdrant.service
install -d -o florian -g florian -m 0755 /var/lib/qdrant/snapshots

systemctl daemon-reload
systemctl restart qdrant

# Wait (up to 120 s) for Qdrant to load its collections and answer.
for _ in $(seq 1 60); do
    if curl -sf --max-time 2 http://127.0.0.1:6333/healthz >/dev/null; then
        echo "Qdrant is up."
        break
    fi
    sleep 2
done

systemctl --no-pager status qdrant | head -5
# Fail loudly if Qdrant still reports a config problem.
if journalctl -u qdrant --since "-3min" --no-pager | grep -q "Config file via --config-path is not found"; then
    echo "ERROR: Qdrant did not find its config file." >&2
    exit 1
fi
