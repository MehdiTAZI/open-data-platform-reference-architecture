#!/usr/bin/env bash
set -euo pipefail

cluster_name="odp-local"
config="infrastructure/environments/local/kind-config.yaml"

diagnose_deployment() {
  local namespace="$1"
  local name="$2"
  echo "::error::Deployment ${namespace}/${name} failed readiness" >&2
  kubectl -n "$namespace" get "deployment/$name" -o wide || true
  kubectl -n "$namespace" describe "deployment/$name" || true
  kubectl -n "$namespace" get pods -o wide || true
  kubectl -n "$namespace" logs "deployment/$name" --all-containers=true --tail=300 || true
  kubectl -n "$namespace" get events --sort-by=.lastTimestamp | tail -80 || true
}

diagnose_job() {
  local namespace="$1"
  local name="$2"
  echo "::error::Job ${namespace}/${name} did not complete" >&2
  kubectl -n "$namespace" get "job/$name" -o wide || true
  kubectl -n "$namespace" describe "job/$name" || true
  kubectl -n "$namespace" get pods -o wide || true
  kubectl -n "$namespace" logs "job/$name" --all-containers=true --tail=300 || true
  kubectl -n "$namespace" get events --sort-by=.lastTimestamp | tail -80 || true
}

wait_deployment() {
  local namespace="$1"
  local name="$2"
  local timeout="$3"
  echo "Waiting for deployment ${namespace}/${name}..."
  if ! kubectl -n "$namespace" rollout status "deployment/$name" --timeout="$timeout"; then
    diagnose_deployment "$namespace" "$name"
    return 1
  fi
}

wait_job() {
  local namespace="$1"
  local name="$2"
  local timeout="$3"
  echo "Waiting for job ${namespace}/${name}..."
  if ! kubectl -n "$namespace" wait --for=condition=complete "job/$name" --timeout="$timeout"; then
    diagnose_job "$namespace" "$name"
    return 1
  fi
}

if kind get clusters | grep -qx "$cluster_name"; then
  echo "Kind cluster $cluster_name already exists"
else
  kind create cluster --config "$config"
fi

kubectl apply -f deployment/kubernetes/base/namespaces.yaml
./scripts/local-secrets.sh

polaris_secret="$(kubectl -n odp-system get secret polaris-root-credentials -o jsonpath='{.data.client-secret}' | python3 -c 'import base64,sys; print(base64.b64decode(sys.stdin.buffer.read()).decode())')"
kubectl -n odp-data create secret generic polaris-client-credentials \
  --from-literal=client-secret="$polaris_secret" --dry-run=client -o yaml | kubectl apply -f -

./scripts/build-local-images.sh

kubectl -n odp-system delete job polaris-bootstrap polaris-catalog-setup --ignore-not-found >/dev/null 2>&1 || true
kubectl apply -k deployment/kubernetes/standalone

echo "Waiting for standalone services..."
wait_deployment odp-data postgres 180s
wait_deployment odp-data garage 180s
wait_deployment odp-data kafka 240s
wait_deployment odp-data debezium-connect 300s
wait_job odp-system polaris-bootstrap 240s
wait_deployment odp-system polaris 300s
wait_job odp-system polaris-catalog-setup 240s
wait_deployment odp-data trino 300s
wait_deployment odp-data spark-client 180s
wait_deployment odp-system airflow 420s
wait_deployment odp-observability otel-collector 180s
wait_deployment odp-observability prometheus 180s
wait_deployment odp-observability grafana 240s

echo
echo "Standalone platform is ready."
echo "Run: make smoke-test"
echo "Credentials: source .local/credentials.env"
