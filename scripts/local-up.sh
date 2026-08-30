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

# Mirror the root Polaris credential into the data namespace for standalone clients.
# Cross-namespace Secret references are intentionally impossible in Kubernetes.
polaris_secret="$(kubectl -n odp-system get secret polaris-root-credentials -o jsonpath='{.data.client-secret}' | python3 -c 'import base64,sys; print(base64.b64decode(sys.stdin.buffer.read()).decode())')"
kubectl -n odp-data create secret generic polaris-client-credentials \
  --from-literal=client-secret="$polaris_secret" --dry-run=client -o yaml | kubectl apply -f -

./scripts/build-local-images.sh

kubectl -n odp-system delete job polaris-bootstrap polaris-catalog-setup --ignore-not-found >/dev/null 2>&1 || true
kubectl apply -k deployment/kubernetes/standalone

echo "Waiting for standalone services..."
kubectl -n odp-data rollout status deployment/postgres --timeout=180s
kubectl -n odp-data rollout status deployment/garage --timeout=180s
kubectl -n odp-data rollout status deployment/kafka --timeout=240s
kubectl -n odp-data rollout status deployment/debezium-connect --timeout=300s
kubectl -n odp-system wait --for=condition=complete job/polaris-bootstrap --timeout=240s
kubectl -n odp-system rollout status deployment/polaris --timeout=300s
kubectl -n odp-system wait --for=condition=complete job/polaris-catalog-setup --timeout=240s
kubectl -n odp-data rollout status deployment/trino --timeout=300s
kubectl -n odp-data rollout status deployment/spark-client --timeout=180s
kubectl -n odp-system rollout status deployment/airflow --timeout=420s
kubectl -n odp-observability rollout status deployment/otel-collector --timeout=180s
kubectl -n odp-observability rollout status deployment/prometheus --timeout=180s
kubectl -n odp-observability rollout status deployment/grafana --timeout=240s

echo
echo "Standalone platform is ready."
echo "Run: make smoke-test"
echo "Credentials: source .local/credentials.env"
