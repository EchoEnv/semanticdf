#!/usr/bin/env bash
# memory + disk monitor for the Trino decision-gate cluster.
#
# Per user constraint: 'monitor memory, disk while running, to
# not explode server.' This script tails memory + disk usage
# every 5s and computes peak. Paired with docker-compose's
# memory caps (1.5GB hard).
#
# Usage:
#   ./monitor.sh             # default: 5s interval, log to monitor.log
#   MONITOR_INTERVAL_SEC=10 ./monitor.sh
#   MONITOR_LOG=/tmp/mon.log ./monitor.sh
#
# Output:
#   monitor.log — comma-separated samples
#   On Ctrl-C / SIGTERM: prints peak memory + max disk %

set -uo pipefail

CONTAINER="${TRINO_CONTAINER:-semanticdf-trino-test}"
LOGFILE="${MONITOR_LOG:-monitor.log}"
INTERVAL="${MONITOR_INTERVAL_SEC:-5}"
DATA_DIR="${DATA_DIR:-$(dirname "$0")/data}"

# CSV header
{
  echo "timestamp,mem_used_bytes,mem_used_human,mem_pct_of_host,disk_pct_root,data_dir_bytes,data_dir_human"
} > "$LOGFILE"

PEAK_MEM=0
PEAK_MEM_HUMAN=""
PEAK_MEM_PCT=0
PEAK_DISK_PCT=0
PEAK_DATA_BYTES=0

now() { date -u +%FT%TZ; }

cleanup() {
  echo "" | tee -a "$LOGFILE"
  echo "[$(now)] monitor stopped. PEAKS:" | tee -a "$LOGFILE"
  echo "  memory (container RSS):  ${PEAK_MEM} bytes (${PEAK_MEM_HUMAN})" | tee -a "$LOGFILE"
  echo "  memory % of host:        ${PEAK_MEM_PCT}%" | tee -a "$LOGFILE"
  echo "  disk % (root fs):        ${PEAK_DISK_PCT}%" | tee -a "$LOGFILE"
  echo "  data dir:                ${PEAK_DATA_BYTES} bytes" | tee -a "$LOGFILE"
  echo "" | tee -a "$LOGFILE"
  exit 0
}
trap cleanup INT TERM

echo "[$(now)] starting monitor (interval=${INTERVAL}s, logfile=${LOGFILE})"

while true; do
  # Bail out cleanly if the container vanished
  if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$CONTAINER"; then
    echo "[$(now)] container '$CONTAINER' is no longer running — stopping monitor" | tee -a "$LOGFILE"
    cleanup
  fi

  # Memory: use docker stats with no-stream (no JSON dance; works
  # in minimal Docker installs). Format: "USED / LIMIT  (PCT%)"
  STATS_LINE="$(docker stats --no-stream --format '{{.MemUsage}}|{{.MemPerc}}' "$CONTAINER" 2>/dev/null || true)"
  MEM_USED_HUMAN="$(printf '%s' "$STATS_LINE" | awk -F'|' '{print $1}' | awk '{print $1}')"
  MEM_PCT_RAW="$(printf '%s' "$STATS_LINE" | awk -F'|' '{print $2}' | tr -d '%')"
  MEM_USED_BYTES="$(printf '%s' "$MEM_USED_HUMAN" | awk '
    /MiB$/ {printf "%d\n", $1 * 1024 * 1024; next}
    /GiB$/ {printf "%d\n", $1 * 1024 * 1024 * 1024; next}
    /KiB$/ {printf "%d\n", $1 * 1024; next}
    /^[[:digit:]]+$/ {printf "%d\n", $1; next}
    {printf "0\n"}
  ')"

  # Disk: root filesystem
  DISK_PCT_RAW="$(df -P / 2>/dev/null | awk 'NR==2 {gsub("%","",$5); print $5}')"
  DISK_PCT="${DISK_PCT_RAW:-0}"

  # Data dir (Trino's bind-mounted volume)
  DATA_BYTES="$(du -sb "$DATA_DIR" 2>/dev/null | awk '{print $1}')"
  DATA_BYTES="${DATA_BYTES:-0}"
  DATA_HUMAN="$(du -sh "$DATA_DIR" 2>/dev/null | awk '{print $1}')"
  DATA_HUMAN="${DATA_HUMAN:-0K}"

  printf '%s,%d,%s,%s,%s,%d,%s\n' \
    "$(now)" "$MEM_USED_BYTES" "$MEM_USED_HUMAN" \
    "${MEM_PCT_RAW:-0}" "$DISK_PCT" "$DATA_BYTES" "$DATA_HUMAN" \
    >> "$LOGFILE"

  # Peak tracking (only update if sane values)
  if [ "${MEM_USED_BYTES:-0}" -gt "$PEAK_MEM" ] 2>/dev/null; then
    PEAK_MEM="$MEM_USED_BYTES"
    PEAK_MEM_HUMAN="$MEM_USED_HUMAN"
  fi
  PCT_INT="${MEM_PCT_RAW:-0}"
  if [ "$PCT_INT" -gt "$PEAK_MEM_PCT" ] 2>/dev/null; then
    PEAK_MEM_PCT="$PCT_INT"
  fi
  if [ "$DISK_PCT" -gt "$PEAK_DISK_PCT" ] 2>/dev/null; then
    PEAK_DISK_PCT="$DISK_PCT"
  fi
  if [ "$DATA_BYTES" -gt "$PEAK_DATA_BYTES" ] 2>/dev/null; then
    PEAK_DATA_BYTES="$DATA_BYTES"
  fi

  sleep "$INTERVAL"
done
