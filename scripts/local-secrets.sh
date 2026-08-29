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

ensure_postgres() {
  if kubectl -n odp-data get secret postgres-credentials >/dev/null 2>&1; then
    postgres_user="$(secret_value odp-data postgres-credentials username)"
    postgres_password="$(secret_value odp-data postgres-credentials password)"
  else
    postgres_user="odp"
    postgres_password="$(rand_hex 24)"
    kubectl -n odp-data create secret generic postgres-credentials \
      --from-literal=username="$postgres_user" --from-literal=password="$postgres_password"
  fi
}

ensure_garage() {
  if kubectl -n odp-data get secret garage-credentials >/dev/null 2>&1; then
    garage_access="$(secret_value odp-data garage-credentials access-key)"
    garage_secret="$(secret_value odp-data garage-credentials secret-key)"
  else
    garage_access="GK$(rand_hex 16)"
    garage_secret="$(rand_hex 32)"
    kubectl -n odp-data create secret generic garage-credentials \
      --from-literal=access-key="$garage_access" \
      --from-literal=secret-key="$garage_secret" \
      --from-literal=rpc-secret="$(rand_hex 32)" \
      --from-literal=admin-token="$(rand_hex 32)" \
      --from-literal=metrics-token="$(rand_hex 32)"
  fi
}

ensure_polaris() {
  if kubectl -n odp-system get secret polaris-root-credentials >/dev/null 2>&1; then
    polaris_root_secret="$(secret_value odp-system polaris-root-credentials client-secret)"
  else
    polaris_root_secret="$(rand_hex 24)"
    kubectl -n odp-system create secret generic polaris-root-credentials \
      --from-literal=client-id=root --from-literal=client-secret="$polaris_root_secret"
  fi

  kubectl -n odp-system create secret generic polaris-storage-credentials \
    --from-literal=access-key="$garage_access" --from-literal=secret-key="$garage_secret" \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n odp-system create secret generic polaris-db-credentials \
    --from-literal=username="$postgres_user" --from-literal=password="$postgres_password" \
    --dry-run=client -o yaml | kubectl apply -f -
}

ensure_grafana() {
  if kubectl -n odp-observability get secret grafana-admin >/dev/null 2>&1; then
    grafana_password="$(secret_value odp-observability grafana-admin admin-password)"
  else
    grafana_password="$(rand_hex 20)"
    kubectl -n odp-observability create secret generic grafana-admin \
      --from-literal=admin-user=admin --from-literal=admin-password="$grafana_password"
  fi
}

ensure_debezium() {
  debezium_user="debezium"
  if kubectl -n odp-data get secret debezium-credentials >/dev/null 2>&1; then
    debezium_password="$(secret_value odp-data debezium-credentials password)"
  else
    debezium_password="$(rand_hex 24)"
    kubectl -n odp-data create secret generic debezium-credentials \
      --from-literal=username="$debezium_user" --from-literal=password="$debezium_password"
  fi
}

write_local_credentials() {
  mkdir -p .local
  chmod 700 .local
  cat > .local/credentials.env <<EOF
export ODP_GARAGE_ACCESS_KEY='$garage_access'
export ODP_GARAGE_SECRET_KEY='$garage_secret'
export ODP_GARAGE_ENDPOINT='http://localhost:3900'
export ODP_POLARIS_CLIENT_ID='root'
export ODP_POLARIS_CLIENT_SECRET='$polaris_root_secret'
export ODP_POLARIS_REALM='odp'
export ODP_POSTGRES_USER='$postgres_user'
export ODP_POSTGRES_PASSWORD='$postgres_password'
export ODP_DEBEZIUM_USER='$debezium_user'
export ODP_DEBEZIUM_PASSWORD='$debezium_password'
export ODP_GRAFANA_ADMIN_USER='admin'
export ODP_GRAFANA_ADMIN_PASSWORD='$grafana_password'
EOF
  chmod 600 .local/credentials.env
}

ensure_postgres
ensure_garage
ensure_polaris
ensure_grafana
ensure_debezium
write_local_credentials

echo "Standalone credentials are ready in .local/credentials.env"
