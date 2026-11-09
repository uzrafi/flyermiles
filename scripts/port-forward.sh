#!/usr/bin/env bash
# Forwards the APIs and Grafana from the kind cluster to the same localhost ports Docker Compose uses.
# Ctrl+C stops them all.
set -uo pipefail
trap 'kill $(jobs -p) 2>/dev/null' EXIT

kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80 >/dev/null &
kubectl -n monitoring port-forward svc/kube-prometheus-stack-prometheus 9090:9090 >/dev/null &
for pair in member-service:8081 ledger-service:8082 redemption-service:8083 query-service:8084 flight-activity-simulator:8085; do
  kubectl -n flyermiles port-forward "svc/${pair%%:*}" "${pair##*:}" >/dev/null &
done
echo "Grafana http://localhost:3000 (admin / flyermiles), APIs on 8081-8085. Ctrl+C to stop."
wait
