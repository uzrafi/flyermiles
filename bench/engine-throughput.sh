#!/usr/bin/env bash
# Earning-engine throughput with P partitions and P instances (1 stream thread each).
# Recreates the stack, preloads N flights with the engine stopped, then starts P engines and times
# how long the earning-engine consumer group takes to drain flight.completed. flight.completed is
# written by a non-transactional producer, so the engine's lag on it does reach exactly 0.
# ledger/query services are stopped so the run measures the streams app alone.
# Usage: bench/engine-throughput.sh "1 3 6" [N]
source "$(dirname "$0")/lib.sh"
PARTS=${1:-"1 3 6"}
N=${2:-1000000}

for P in $PARTS; do
  log "=== $P partition(s), $P engine instance(s), $N flights ==="
  docker compose --profile apps --profile obs down -v >/dev/null 2>&1
  PARTITIONS=$P docker compose --profile apps up -d --scale earning-engine=0 >/dev/null 2>&1
  docker compose stop ledger-service query-service >/dev/null 2>&1
  until curl -sf "$MEMBER/actuator/health/readiness" >/dev/null && curl -sf "$SIM/actuator/health/readiness" >/dev/null; do sleep 2; done

  curl -sf -X POST "$MEMBER/members/bulk" -H 'Content-Type: application/json' -d '{"count":10000}' >/dev/null
  curl -sf -X POST "$SIM/simulator/members/reload" >/dev/null
  curl -sf -X POST "$SIM/simulator/burst?count=$N" >/dev/null

  PARTITIONS=$P docker compose --profile apps up -d --no-recreate --scale earning-engine="$P" earning-engine >/dev/null 2>&1
  start=""; startLag=""
  while :; do
    lag=$(group_lag earning-engine flight.completed)
    now=$(date +%s.%N)
    # the group shows no offsets until the first commit; treat that as "not started"
    if [[ -z "$start" && "$lag" -gt 0 && "$lag" -lt "$N" ]]; then start=$now; startLag=$lag; fi
    if [[ -n "$start" && "$lag" -eq 0 ]]; then break; fi
    sleep 0.5
  done
  secs=$(awk "BEGIN {print $now - $start}")
  rate=$(awk "BEGIN {printf \"%d\", $startLag / ($now - $start)}")
  echo "{\"partitions\":$P,\"instances\":$P,\"events\":$startLag,\"seconds\":$secs,\"eventsPerSecond\":$rate}" \
    | tee "bench/results/engine-throughput-p$P.json"
done

log "restoring the default 6-partition stack"
docker compose --profile apps --profile obs down -v >/dev/null 2>&1
docker compose --profile apps --profile obs up -d >/dev/null 2>&1
