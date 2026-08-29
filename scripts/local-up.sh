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
# Default deny is intentionally applied only to the data plane. Explicit allow
# policies will be introduced with each platform service before it is deployed.
kubectl apply -f deployment/kubernetes/base/default-deny.yaml

echo "Standalone control plane is ready. Platform services are introduced in V0.2."
