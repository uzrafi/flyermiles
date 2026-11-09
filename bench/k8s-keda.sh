#!/usr/bin/env bash
# KEDA lag-driven scale-out: burst flights so ledger.events floods, record query-service replicas and
# consumer lag every 5 s until lag is back to 0, then report scale-out and drain time.
# Needs scripts/port-forward.sh running.
set -euo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
NS=flyermiles
SIM=http://localhost:8085
N=${1:-200000}
OUT=bench/results/keda-timeline.csv
mkdir -p bench/results

lag() {
  # Lag from Strimzi's Kafka Exporter, via Prometheus (the same number KEDA acts on).
  curl -sf --get http://localhost:9090/api/v1/query \
    --data-urlencode 'query=sum(kafka_consumergroup_lag{consumergroup="query-service"})' \
    | sed -E 's/.*"value":\[[0-9.]+,"([0-9.]+)".*/\1/'
}
replicas() { kubectl -n $NS get deploy query-service -o jsonpath='{.status.readyReplicas}'; }

echo "t_seconds,lag,ready_replicas" > "$OUT"
start_replicas=$(replicas)
curl -sf -X POST "$SIM/simulator/burst?count=$N" >/dev/null &
t0=$(date +%s); max=$start_replicas; peak_lag=0; saw_lag=0
while :; do
  t=$(( $(date +%s) - t0 )); l=$(lag || echo 0); r=$(replicas); r=${r:-0}
  echo "$t,$l,$r" | tee -a "$OUT"
  (( r > max )) && max=$r
  l_int=${l%.*}
  (( l_int > peak_lag )) && peak_lag=$l_int
  (( l_int > 1000 )) && saw_lag=1
  (( saw_lag == 1 && l_int == 0 )) && break
  (( t > 900 )) && break
  sleep 5
done
echo "{\"events\":$N,\"startReplicas\":$start_replicas,\"maxReplicas\":$max,\"peakLag\":$peak_lag,\"secondsToZeroLag\":$t}" \
  | tee bench/results/keda.json
