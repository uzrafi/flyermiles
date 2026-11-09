#!/usr/bin/env bash
# Failure tests on the kind cluster, each under load and each followed by reconciliation:
#   1. delete an earning-engine pod mid-burst
#   2. rolling restart of a Kafka broker (Strimzi manual rolling update)
#   3. kill the query-service during a read-model rebuild
#   4. zero-downtime rolling update of the earning-engine during load
# Needs scripts/port-forward.sh running (simulator on :8085, query-service on :8084).
set -euo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
NS=flyermiles
SIM=http://localhost:8085
QUERY=http://localhost:8084
RATE=${RATE:-500}
mkdir -p bench/results
log() { printf '\033[1;34m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*" >&2; }

reconcile() {
  local name=$1 job="recon-$1-$(date +%s)"
  kubectl -n $NS create job --from=cronjob/reconciliation "$job" >/dev/null
  kubectl -n $NS wait --for=condition=complete --timeout=10m "job/$job" >/dev/null 2>&1 \
    || kubectl -n $NS wait --for=condition=failed --timeout=10s "job/$job" >/dev/null 2>&1 || true
  kubectl -n $NS logs "job/$job" | sed -n '/RECONCILIATION/,$p' > "bench/results/k8s-recon-$name.txt"
  local ok; ok=$(grep -m1 '"mismatches"' "bench/results/k8s-recon-$name.txt" | tr -dc '0-9')
  echo "{\"test\":\"$name\",\"mismatches\":${ok:-null}}" | tee "bench/results/k8s-failure-$name.json"
}

load_on()  { curl -sf -X POST "$SIM/simulator/start?rate=$RATE" >/dev/null; }
load_off() { curl -sf -X POST "$SIM/simulator/stop" >/dev/null; }

log "1/4 delete an earning-engine pod mid-load"
load_on; sleep 30
pod=$(kubectl -n $NS get pod -l app.kubernetes.io/name=earning-engine -o name | head -1)
kubectl -n $NS delete "$pod" --wait=false; sleep 60; load_off
reconcile engine-pod-delete

log "2/4 rolling restart of Kafka broker flyermiles-dual-role-1 under load"
load_on; sleep 20
kubectl -n $NS annotate pod flyermiles-dual-role-1 strimzi.io/manual-rolling-update=true --overwrite
# Strimzi rolls it on its next reconciliation; wait for the pod to be replaced and Ready again.
uid=$(kubectl -n $NS get pod flyermiles-dual-role-1 -o jsonpath='{.metadata.uid}')
until [[ "$(kubectl -n $NS get pod flyermiles-dual-role-1 -o jsonpath='{.metadata.uid}' 2>/dev/null)" != "$uid" ]]; do sleep 5; done
kubectl -n $NS wait pod/flyermiles-dual-role-1 --for=condition=Ready --timeout=10m
sleep 30; load_off
reconcile broker-restart

log "3/4 kill the query-service during a read-model rebuild"
kubectl -n $NS scale deploy/query-service --replicas=1   # rebuild needs a single consumer
kubectl -n $NS patch scaledobject query-service --type merge -p '{"metadata":{"annotations":{"autoscaling.keda.sh/paused-replicas":"1"}}}'
kubectl -n $NS rollout status deploy/query-service
curl -sf -X POST "$QUERY/admin/rebuild" >/dev/null; sleep 5
kubectl -n $NS delete pod -l app.kubernetes.io/name=query-service --wait=false
kubectl -n $NS rollout status deploy/query-service
# The replacement pod resumes from the committed offsets; the idempotent projection absorbs replays.
sleep 60
kubectl -n $NS annotate scaledobject query-service autoscaling.keda.sh/paused-replicas-
reconcile query-kill-during-rebuild

log "4/4 rolling update of the earning-engine during load"
load_on; sleep 20
kubectl -n $NS rollout restart deploy/earning-engine
kubectl -n $NS rollout status deploy/earning-engine --timeout=10m
sleep 30; load_off
reconcile engine-rolling-update
