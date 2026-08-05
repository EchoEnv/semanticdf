#!/usr/bin/env bash
# Safe teardown — stop the cluster, remove container + volumes,
# kill any leftover monitor. Idempotent; safe to run when the
# cluster is already down.
#
# Per user constraint: 'monitor memory, disk while running, to
# not explode server.' Teardown matters as much as startup:
# orphaned containers / volumes are how disk fills up.

set -uo pipefail

cd "$(dirname "$0")"

echo "[$(date -Iseconds)] stopping docker compose stack..."
docker compose down --remove-orphans -v 2>&1 || true

# Belt + suspenders: kill any lingering monitor process for this
# compose project. PIDs are recorded in monitor.pid if it was
# launched by monitor.sh wrapper (not required by this script).
if [ -f monitor.pid ]; then
  echo "[$(date -Iseconds)] killing monitor pid $(cat monitor.pid)..."
  kill -INT "$(cat monitor.pid)" 2>/dev/null || true
  rm -f monitor.pid
fi

# Wipe bind-mounted data (Trino spill, etc.). Bounded safety net.
# The container may have written 0-bytes-after-tests, but we wipe
# to be sure.
if [ -d data ]; then
  echo "[$(date -Iseconds)] wiping bind-mounted data dir..."
  rm -rf data/* data/.* 2>/dev/null || true
fi

# Report remaining docker artifacts so the user knows the server
# is actually clean.
echo ""
echo "=== docker container(s) remaining for this project ==="
docker ps -a --filter "name=semanticdf-trino" --format "{{.ID}}\t{{.Status}}\t{{.Image}}" || echo "(none)"
echo "=== docker volume(s) remaining for this project ==="
docker volume ls --filter "name=semanticdf-trino" --format "{{.Name}}" || echo "(none)"
echo ""
echo "[$(date -Iseconds)] teardown complete."
