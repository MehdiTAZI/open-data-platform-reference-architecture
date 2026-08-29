#!/usr/bin/env bash
set -euo pipefail

cluster_name="odp-local"
config="infrastructure/environments/local/kind-config.yaml"

if kind get clusters | grep -qx "$cluster_name"; then
  echo "Kind cluster $cluster_name already exists"
else
  kind create cluster --config "$config"
fi

kubectl apply -f deployment/kubernetes/base/namespaces.yaml
./scripts/local-secrets.sh

# Re-running bootstrap against a new local database is safe. A stale completed Job
# would otherwise prevent re-execution after individual service recreation.
kubectl -n odp-system delete job polaris-bootstrap --ignore-not-found >/dev/null 2>&1 || true

kubectl apply -k deployment/kubernetes/standalone

echo "Waiting for standalone services..."
kubectl -n odp-data rollout status deployment/postgres --timeout=180s
kubectl -n odp-data rollout status deployment/garage --timeout=180s
kubectl -n odp-data rollout status deployment/kafka --timeout=240s
kubectl -n odp-system wait --for=condition=complete job/polaris-bootstrap --timeout=240s
kubectl -n odp-system rollout status deployment/polaris --timeout=300s
kubectl -n odp-data rollout status deployment/trino --timeout=300s
kubectl -n odp-data rollout status deployment/spark-client --timeout=180s
kubectl -n odp-system rollout status deployment/airflow --timeout=420s

echo
echo "Standalone platform is ready."
echo "Run: make smoke-test"
echo "Credentials: source .local/credentials.env"
