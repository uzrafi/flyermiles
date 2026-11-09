#!/usr/bin/env bash
# KEDA lag-driven scale-out, as backlog recovery: the query-service is paused at 0 replicas (as in an
# outage) while flights keep flowing, so ledger.events builds a backlog. Then the pause is lifted and
# KEDA scales the deployment on consumer lag; the script records replicas and lag every 5 s until the
# backlog is gone. (Under steady load the ledger is the slowest stage, so one query-service replica
# never falls behind and KEDA rightly does nothing.)
# Needs scripts/port-forward.sh running (simulator on :8085, Prometheus on :9090).
set -euo pipefail
cd "$(dirname "$0")/.."
NS=flyermiles
SIM=http://localhost:8085
N=${1:-200000}
OUT=bench/results/keda-timeline.csv
mkdir -p bench/results
log() { printf '\033[1;34m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*" >&2; }

lag() {
  # Lag from Strimzi's Kafka Exporter via Prometheus: the same signal KEDA scales on.
  curl -sf --get http://localhost:9090/api/v1/query \
    --data-urlencode 'query=sum(kafka_consumergroup_lag{consumergroup="query-service",topic="ledger.events"})' \
    | sed -E 's/.*"value":\[[0-9.]+,"(-?[0-9.]+)".*/\1/'
}
replicas() { kubectl -n $NS get deploy query-service -o jsonpath='{.status.readyReplicas}'; }
ledger_events() {
  curl -sf --get http://localhost:9090/api/v1/query \
    --data-urlencode 'query=sum(kafka_topic_partition_current_offset{topic="ledger.events"})' \
    | sed -E 's/.*"value":\[[0-9.]+,"([0-9.]+)".*/\1/'
}

log "pausing query-service at 0 replicas"
kubectl -n $NS annotate scaledobject query-service autoscaling.keda.sh/paused-replicas=0 --overwrite >/dev/null
kubectl -n $NS wait --for=delete pod -l app.kubernetes.io/name=query-service --timeout=3m >/dev/null 2>&1 || true

log "producing $N flights; waiting for the ledger to publish them"
before=$(ledger_events)
curl -sf -X POST "$SIM/simulator/burst?count=$N" >/dev/null
until (( $(printf '%.0f' "$(ledger_events)") >= $(printf '%.0f' "$before") + N )); do sleep 5; done
sleep 15
backlog=$(printf '%.0f' "$(lag)")
log "backlog on ledger.events: $backlog; lifting the pause"

echo "t_seconds,lag,ready_replicas" > "$OUT"
kubectl -n $NS annotate scaledobject query-service autoscaling.keda.sh/paused-replicas- >/dev/null
t0=$(date +%s); max=0; first_max_t=""
while :; do
  t=$(( $(date +%s) - t0 )); l=$(printf '%.0f' "$(lag || echo 0)"); r=$(replicas); r=${r:-0}
  echo "$t,$l,$r" | tee -a "$OUT"
  if (( r > max )); then max=$r; first_max_t=$t; fi
  (( t > 30 && l <= 6 )) && break   # <= 1 per partition: Kafka Exporter counts the last commit marker
  (( t > 1200 )) && break
  sleep 5
done
echo "{\"backlog\":$backlog,\"minReplicas\":1,\"maxReplicas\":$max,\"secondsToMaxReplicas\":${first_max_t:-null},\"secondsToDrain\":$t}" \
  | tee bench/results/keda.json
