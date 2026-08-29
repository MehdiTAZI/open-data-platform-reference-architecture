#!/usr/bin/env bash
set -euo pipefail

cluster_name="odp-local"

if ! kind get clusters | grep -qx "$cluster_name"; then
  echo "Kind cluster '$cluster_name' does not exist. Run make local-up." >&2
  exit 1
fi

rand_hex() { openssl rand -hex "$1"; }
decode_b64() { python3 -c 'import base64,sys; sys.stdout.write(base64.b64decode(sys.stdin.buffer.read()).decode())'; }
secret_value() {
  kubectl -n "$1" get secret "$2" -o "jsonpath={.data.$3}" | decode_b64
}

write_local_credentials() {
  mkdir -p .local
  chmod 700 .local
  cat > .local/credentials.env <<EOF
export ODP_GARAGE_ACCESS_KEY='$1'
export ODP_GARAGE_SECRET_KEY='$2'
export ODP_GARAGE_ENDPOINT='http://localhost:3900'
export ODP_POLARIS_CLIENT_ID='root'
export ODP_POLARIS_CLIENT_SECRET='$3'
export ODP_POLARIS_REALM='odp'
export ODP_POSTGRES_USER='$4'
export ODP_POSTGRES_PASSWORD='$5'
export ODP_GRAFANA_ADMIN_USER='admin'
export ODP_GRAFANA_ADMIN_PASSWORD='$6'
EOF
  chmod 600 .local/credentials.env
}

if kubectl -n odp-data get secret postgres-credentials >/dev/null 2>&1 \
  && kubectl -n odp-data get secret garage-credentials >/dev/null 2>&1 \
  && kubectl -n odp-system get secret polaris-db-credentials >/dev/null 2>&1 \
  && kubectl -n odp-system get secret polaris-root-credentials >/dev/null 2>&1 \
  && kubectl -n odp-system get secret polaris-storage-credentials >/dev/null 2>&1 \
  && kubectl -n odp-observability get secret grafana-admin >/dev/null 2>&1; then
  write_local_credentials \
    "$(secret_value odp-data garage-credentials access-key)" \
    "$(secret_value odp-data garage-credentials secret-key)" \
    "$(secret_value odp-system polaris-root-credentials client-secret)" \
    "$(secret_value odp-data postgres-credentials username)" \
    "$(secret_value odp-data postgres-credentials password)" \
    "$(secret_value odp-observability grafana-admin admin-password)"
  echo "Reused existing standalone credentials and refreshed .local/credentials.env"
  exit 0
fi

postgres_user="odp"
postgres_password="$(rand_hex 24)"
garage_access="GK$(rand_hex 16)"
garage_secret="$(rand_hex 32)"
garage_rpc="$(rand_hex 32)"
garage_admin="$(rand_hex 32)"
garage_metrics="$(rand_hex 32)"
polaris_root_secret="$(rand_hex 24)"
grafana_password="$(rand_hex 20)"

kubectl -n odp-data create secret generic postgres-credentials \
  --from-literal=username="$postgres_user" --from-literal=password="$postgres_password" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n odp-data create secret generic garage-credentials \
  --from-literal=access-key="$garage_access" --from-literal=secret-key="$garage_secret" \
  --from-literal=rpc-secret="$garage_rpc" --from-literal=admin-token="$garage_admin" \
  --from-literal=metrics-token="$garage_metrics" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n odp-system create secret generic polaris-storage-credentials \
  --from-literal=access-key="$garage_access" --from-literal=secret-key="$garage_secret" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n odp-system create secret generic polaris-db-credentials \
  --from-literal=username="$postgres_user" --from-literal=password="$postgres_password" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n odp-system create secret generic polaris-root-credentials \
  --from-literal=client-id=root --from-literal=client-secret="$polaris_root_secret" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n odp-observability create secret generic grafana-admin \
  --from-literal=admin-user=admin --from-literal=admin-password="$grafana_password" \
  --dry-run=client -o yaml | kubectl apply -f -

write_local_credentials "$garage_access" "$garage_secret" "$polaris_root_secret" "$postgres_user" "$postgres_password" "$grafana_password"
echo "Generated new ephemeral standalone credentials in .local/credentials.env"
