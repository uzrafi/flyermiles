#!/usr/bin/env bash
# Builds every service jar, then one image per service. With --kind, loads them into the kind cluster.
set -euo pipefail
cd "$(dirname "$0")/.."

MODULES=(member-service ledger-service redemption-service query-service earning-engine flight-activity-simulator reconciliation-job)
TAG="${TAG:-dev}"

./mvnw -q -B -DskipTests package

for m in "${MODULES[@]}"; do
  echo "==> flyermiles/$m:$TAG"
  docker build -q --build-arg MODULE="$m" -t "flyermiles/$m:$TAG" .
done

if [[ "${1:-}" == "--kind" ]]; then
  for m in "${MODULES[@]}"; do
    kind load docker-image "flyermiles/$m:$TAG" --name flyermiles
  done
fi
