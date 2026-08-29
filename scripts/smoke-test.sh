#!/usr/bin/env bash
set -euo pipefail

kubectl cluster-info >/dev/null
kubectl wait --for=condition=Ready nodes --all --timeout=120s
for ns in odp-system odp-data odp-observability; do
  kubectl get namespace "$ns" >/dev/null
done
kubectl -n odp-data get networkpolicy default-deny-ingress-egress >/dev/null

echo "Standalone Kubernetes smoke test passed"
