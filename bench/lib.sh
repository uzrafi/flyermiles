#!/usr/bin/env bash
# Shared helpers for the benchmark scripts (Docker Compose stack on localhost).
set -euo pipefail
# Git Bash on Windows would otherwise rewrite container paths like /opt/kafka into C:/Program Files/Git/...
export MSYS_NO_PATHCONV=1
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mkdir -p bench/results

MEMBER=${MEMBER_URL:-http://localhost:8081}
LEDGER=${LEDGER_URL:-http://localhost:8082}
QUERY=${QUERY_URL:-http://localhost:8084}
SIM=${SIM_URL:-http://localhost:8085}

log() { printf '\033[1;34m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*" >&2; }

# psql against one of the service databases inside the compose Postgres container
psql_db() { docker compose exec -T postgres psql -U flyermiles -d "$1" -At -c "$2"; }

# Sum of lag for a consumer group, optionally on one topic only, from Kafka's own CLI.
group_lag() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
    --describe --group "$1" 2>/dev/null \
    | awk -v t="${2:-}" 'NR>1 && $6 ~ /^[0-9]+$/ && (t == "" || $2 == t) {s+=$6} END {print s+0}'
}

# Wait until the pipeline is quiet: ledger and read model agree and neither changed since the last poll.
# (Consumer lag on points.earned never reads 0: each Kafka Streams commit marker occupies an offset.)
wait_projection() {
  local timeout=${1:-300} t=0 prev=""
  while :; do
    local l q
    l=$(psql_db ledger "SELECT count(*) FROM ledger_entries")
    q=$(psql_db query "SELECT count(*) FROM statement_lines")
    [[ "$l" == "$q" && "$l" == "$prev" ]] && { log "settled: $l ledger entries projected"; return 0; }
    (( t >= timeout )) && { log "not settled after ${timeout}s (ledger=$l read=$q)"; return 1; }
    prev=$l
    sleep 3; t=$((t + 3))
  done
}

# Wait until the projection has applied at least N entries (for timing with ~1 s resolution).
wait_entries() {
  local n=$1
  until (( $(psql_db query "SELECT count(*) FROM statement_lines") >= n )); do sleep 1; done
}
