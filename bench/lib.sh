#!/usr/bin/env bash
# Shared helpers for the benchmark scripts (Docker Compose stack on localhost).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mkdir -p bench/results

MEMBER=${MEMBER_URL:-http://localhost:8081}
LEDGER=${LEDGER_URL:-http://localhost:8082}
QUERY=${QUERY_URL:-http://localhost:8084}
SIM=${SIM_URL:-http://localhost:8085}

log() { printf '\033[1;34m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*" >&2; }

# psql against one of the service databases inside the compose Postgres container
psql_db() { docker compose exec -T postgres psql -U flyermiles -d "$1" -At -c "$2"; }

# Sum of lag for a consumer group, from Kafka's own CLI.
group_lag() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
    --describe --group "$1" 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}'
}

# Wait until the read model has caught up with the ledger (same number of statement lines as entries).
wait_projection() {
  local timeout=${1:-300} t=0
  while :; do
    local l q
    l=$(psql_db ledger "SELECT count(*) FROM ledger_entries")
    q=$(psql_db query "SELECT count(*) FROM statement_lines")
    local lag; lag=$(group_lag ledger-service)
    [[ "$l" == "$q" && "$lag" == "0" ]] && { log "settled: $l ledger entries projected"; return 0; }
    (( t >= timeout )) && { log "not settled after ${timeout}s (ledger=$l read=$q ledger-lag=$lag)"; return 1; }
    sleep 2; t=$((t + 2))
  done
}
