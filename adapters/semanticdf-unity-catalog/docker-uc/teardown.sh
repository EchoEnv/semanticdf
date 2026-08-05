#!/usr/bin/env bash
# Safe teardown — stop UC, remove container + bind mounts,
# kill any leftover monitor. Idempotent; safe to run when the
# cluster is already down.
#
# Per user constraint: 'monitor memory, disk while running, to not
# explode server.' Teardown matters as much as startup: orphaned
# bind mounts are how disk fills up.

set -uo pipefail

cd "$(dirname "$0")"

echo "[$(date -Iseconds)] stopping docker compose stack..."
docker compose down --remove-orphans 2>&1 || true

# Belt + suspenders: kill any lingering monitor process.
if [ -f monitor.pid ]; then
  echo "[$(date -Iseconds)] killing monitor pid $(cat monitor.pid)..."
  kill -INT "$(cat monitor.pid)" 2>/dev/null || true
  rm -f monitor.pid
fi

# Wipe bind-mounted data + db so the next run starts clean.
# (catalog data persists in ./data; if we don't wipe, the next
# run sees stale tables from prior tests.)
if [ -d data ]; then
  echo "[$(date -Iseconds)] wiping bind-mounted data dir..."
  rm -rf data/* data/.* 2>/dev/null || true
  # Re-apply world-writable so the next start can write
  # (the container's `unitycatalog` user is UID 100, not the
  # host's emilio user).
  chmod -R 777 data 2>/dev/null || true
fi

# Report remaining artifacts so the user knows the server is clean.
echo ""
echo "=== docker container(s) remaining for this project ==="
docker ps -a --filter "name=semanticdf-uc" --format "{{.ID}}\t{{.Status}}\t{{.Image}}" || echo "(none)"
echo "=== docker volume(s) remaining for this project ==="
docker volume ls --filter "name=semanticdf-uc" --format "{{.Name}}" || echo "(none)"
echo ""
echo "[$(date -Iseconds)] teardown complete."