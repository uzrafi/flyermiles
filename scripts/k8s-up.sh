#!/usr/bin/env bash
# Stands up the whole system on a local 3-node kind cluster:
#   Strimzi (Kafka), KEDA, kube-prometheus-stack + Loki + Tempo + OTel Collector, infra, all services.
# Re-runnable: every step is an install-or-upgrade.
set -euo pipefail
cd "$(dirname "$0")/.."

STRIMZI_VERSION=0.44.0
KEDA_VERSION=2.15.1
NS=flyermiles

step() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

step "kind cluster"
kind get clusters | grep -qx flyermiles || kind create cluster --config deploy/k8s/kind-config.yaml
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

step "helm repositories"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
helm repo add grafana https://grafana.github.io/helm-charts >/dev/null
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts >/dev/null
helm repo add kedacore https://kedacore.github.io/charts >/dev/null
helm repo update >/dev/null

step "monitoring: kube-prometheus-stack, Loki, Tempo, OpenTelemetry Collector, Pushgateway"
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  -n monitoring -f deploy/k8s/monitoring/kube-prometheus-stack.yaml --wait --timeout 10m
helm upgrade --install loki grafana/loki -n monitoring -f deploy/k8s/monitoring/loki.yaml --wait --timeout 10m
helm upgrade --install tempo grafana/tempo -n monitoring -f deploy/k8s/monitoring/tempo.yaml --wait
helm upgrade --install otel-collector open-telemetry/opentelemetry-collector \
  -n monitoring -f deploy/k8s/monitoring/otel-collector.yaml --wait
helm upgrade --install pushgateway prometheus-community/prometheus-pushgateway \
  -n monitoring --set serviceMonitor.enabled=true --wait
kubectl apply -f deploy/k8s/monitoring/prometheus-rules.yaml
kubectl -n monitoring create configmap flyermiles-dashboard \
  --from-file=flyermiles.json=deploy/observability/grafana/dashboards/flyermiles.json \
  --dry-run=client -o yaml | kubectl label --local -f - grafana_dashboard=1 -o yaml | kubectl apply -f -

step "Strimzi operator $STRIMZI_VERSION (watching namespace $NS)"
helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version "$STRIMZI_VERSION" \
  -n kafka --create-namespace --set "watchNamespaces={$NS}" --set fullReconciliationIntervalMs=30000 --wait

step "Kafka cluster, topics, users"
kubectl -n "$NS" apply -f deploy/k8s/strimzi/kafka.yaml
kubectl -n "$NS" wait kafka/flyermiles --for=condition=Ready --timeout=15m
kubectl -n "$NS" apply -f deploy/k8s/strimzi/topics.yaml -f deploy/k8s/strimzi/users.yaml
kubectl -n "$NS" wait kafkatopic --all --for=condition=Ready --timeout=5m
kubectl -n "$NS" wait kafkauser --all --for=condition=Ready --timeout=5m
kubectl apply -f deploy/k8s/monitoring/monitors.yaml

step "KEDA $KEDA_VERSION"
helm upgrade --install keda kedacore/keda --version "$KEDA_VERSION" -n keda --create-namespace --wait

step "images"
scripts/build-images.sh --kind

step "infra: Postgres, Redis, Apicurio"
helm upgrade --install infra deploy/helm/flyermiles-infra -n "$NS" --wait

step "services"
for chart in member-service ledger-service redemption-service query-service earning-engine flight-activity-simulator; do
  helm dependency build "deploy/helm/$chart" >/dev/null
  helm upgrade --install "$chart" "deploy/helm/$chart" -n "$NS" --wait --timeout 5m
done
helm upgrade --install reconciliation-job deploy/helm/reconciliation-job -n "$NS"

step "done"
kubectl -n "$NS" get pods -o wide
cat <<'EOF'

Port-forwards (each in its own terminal):
  kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80
  kubectl -n flyermiles port-forward svc/member-service 8081
  kubectl -n flyermiles port-forward svc/redemption-service 8083
  kubectl -n flyermiles port-forward svc/query-service 8084
  kubectl -n flyermiles port-forward svc/flight-activity-simulator 8085
Or run scripts/port-forward.sh to start them all.
EOF
