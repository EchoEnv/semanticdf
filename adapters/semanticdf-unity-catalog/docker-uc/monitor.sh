#!/usr/bin/env bash
# Resource monitor for the Unity Catalog docker-compose stack.
# Per user constraint: 'monitor memory, disk while running, to not
# explode server.' Polls every N seconds and writes to stderr.
#
# Usage:
#   ./monitor.sh                  # 10s polling
#   ./monitor.sh 5                # 5s polling
#   ./monitor.sh 2 100            # 2s polling, exit after 100 ticks
#
# Output (stderr only — stdout stays clean for the test runner):
#   [ts] container <name> mem=<rss> limit=<cap> disk=<used> cap=<quota>
#
# The script writes its PID to monitor.pid so teardown.sh can SIGINT
# it cleanly. teardown.sh is idempotent: killing an already-dead
# monitor is a no-op.

set -uo pipefail

cd "$(dirname "$0")"

INTERVAL="${1:-10}"
MAX_TICKS="${2:-0}"   # 0 = run forever
LOG_FILE="${MONITOR_LOG:-monitor.log}"

CONTAINER_NAME="semanticdf-uc-test"

# Discover the container's hard memory cap from Docker itself
# (so this script tracks the compose-declared cap, not a guess).
MEM_LIMIT_BYTES=$(docker inspect "$CONTAINER_NAME" \
  --format '{{.HostConfig.Memory}}' 2>/dev/null || echo 0)
MEM_LIMIT_MIB=$((MEM_LIMIT_BYTES / 1048576))

echo "[$(date -Iseconds)] starting monitor (interval=${INTERVAL}s, container=${CONTAINER_NAME}, mem_cap=${MEM_LIMIT_MIB} MiB)" | tee -a "$LOG_FILE" >&2

# Background-friendly: write PID for teardown.
echo $$ > monitor.pid
trap 'rm -f monitor.pid; exit 0' INT TERM

ticks=0
while true; do
  ticks=$((ticks + 1))

  # Container stats — RSS in bytes (sum across cgroups).
  rss_bytes=$(docker stats "$CONTAINER_NAME" --no-stream --format '{{.MemUsage}}' 2>/dev/null \
    | awk -F' / ' '{print $1}' | numfmt --from=iec 2>/dev/null || echo 0)
  rss_mib=$((rss_bytes / 1048576))

  # Disk usage on the bind-mounted data dir. This is the actual
  # disk the catalog writes to.
  disk_bytes=$(du -sb ./data 2>/dev/null | awk '{print $1}')
  disk_mib=$((disk_bytes / 1048576))

  echo "[$(date -Iseconds)] tick=${ticks} container=${CONTAINER_NAME} mem=${rss_mib}MiB cap=${MEM_LIMIT_MIB}MiB disk=${disk_mib}MiB" \
    | tee -a "$LOG_FILE" >&2

  if [ "$MAX_TICKS" -gt 0 ] && [ "$ticks" -ge "$MAX_TICKS" ]; then
    echo "[$(date -Iseconds)] reached max ticks (${MAX_TICKS}), exiting" >&2
    break
  fi
  sleep "$INTERVAL"
done