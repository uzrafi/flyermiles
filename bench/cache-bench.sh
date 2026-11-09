#!/usr/bin/env bash
# Balance reads with and without the Redis cache, while flights keep arriving (so invalidations are
# real), and the hit rate from the query-service's own counters.
# Usage: bench/cache-bench.sh [reads_per_s] [flights_per_s]
source "$(dirname "$0")/lib.sh"
RATE=${1:-1000}
WRITES=${2:-100}

hits()   { curl -sf "$QUERY/actuator/metrics/flyermiles_balance_cache_requests_total?tag=result:hit" | sed -E 's/.*"value":([0-9.E]+).*/\1/'; }
misses() { curl -sf "$QUERY/actuator/metrics/flyermiles_balance_cache_requests_total?tag=result:miss" | sed -E 's/.*"value":([0-9.E]+).*/\1/'; }

curl -sf -X POST "$SIM/simulator/start?rate=$WRITES" >/dev/null

curl -sf -X POST "$QUERY/admin/cache?enabled=false" >/dev/null
log "cache OFF: $RATE reads/s"
k6 run -q -e RATE="$RATE" -e DURATION=60s -e LABEL=cache-off k6/balance-read.js | tail -9 || true

curl -sf -X POST "$QUERY/admin/cache?enabled=true" >/dev/null
h0=$(hits); m0=$(misses)
log "cache ON: $RATE reads/s"
k6 run -q -e RATE="$RATE" -e DURATION=60s -e LABEL=cache-on k6/balance-read.js | tail -9 || true
h1=$(hits); m1=$(misses)

curl -sf -X POST "$SIM/simulator/stop" >/dev/null
awk -v h="$(awk "BEGIN{print $h1-$h0}")" -v m="$(awk "BEGIN{print $m1-$m0}")" \
  'BEGIN { printf "{\"hits\":%d,\"misses\":%d,\"hitRate\":%.4f}\n", h, m, h/(h+m) }' | tee bench/results/cache-hit-rate.json
