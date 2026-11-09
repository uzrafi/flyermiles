#!/usr/bin/env bash
# End-to-end earn latency: flight completed (simulator clock) -> balance visible in the read model,
# at a steady rate. Usage: bench/earn-latency.sh [events_per_s] [seconds]
source "$(dirname "$0")/lib.sh"
RATE=${1:-1000}
SECS=${2:-120}

curl -sf -X POST "$QUERY/admin/earn-latency/reset" >/dev/null
log "simulating $RATE flights/s for ${SECS}s"
curl -sf -X POST "$SIM/simulator/start?rate=$RATE" >/dev/null
sleep "$SECS"
curl -sf -X POST "$SIM/simulator/stop" >/dev/null
wait_projection 300

result=$(curl -sf "$QUERY/admin/earn-latency")
echo "{\"rate\":$RATE,\"seconds\":$SECS,\"latency\":$result}" | tee "bench/results/earn-latency-${RATE}.json"
