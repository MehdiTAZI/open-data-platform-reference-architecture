#!/usr/bin/env bash
set -euo pipefail

cluster_name="odp-local"

if ! kind get clusters | grep -qx "$cluster_name"; then
  echo "Kind cluster '$cluster_name' does not exist. Run make local-up." >&2
  exit 1
fi

rand_hex() {
  local bytes="$1"
  openssl rand -hex "$bytes"
}

decode_b64() {
  python3 -c 'import base64,sys; sys.stdout.write(base64.b64decode(sys.stdin.buffer.read()).decode())'
}

secret_value() {
  local namespace="$1"
  local secret="$2"
  local key="$3"
  kubectl -n "$namespace" get secret "$secret" -o "jsonpath={.data.${key}}" | decode_b64
}

ensure_grafana_secret() {
  if kubectl -n odp-observability get secret grafana-credentials >/dev/null 2>&1; then
    secret_value odp-observability grafana-credentials admin-password
    return
  fi

  local password
  password="$(rand_hex 24)"
  kubectl -n odp-observability create secret generic grafana-credentials \
    --from-literal=admin-password="$password" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  printf '%s' "$password"
}

write_local_credentials() {
  local garage_access="$1"
  local garage_secret="$2"
  local polaris_root_secret="$3"
  local postgres_user="$4"
  local postgres_password="$5"
  local grafana_password="$6"

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
export ODP_GRAFANA_USER='admin'
export ODP_GRAFANA_PASSWORD='$grafana_password'
EOF
  chmod 600 .local/credentials.env
}

grafana_password="$(ensure_grafana_secret)"

if kubectl -n odp-data get secret postgres-credentials >/dev/null 2>&1 \
  && kubectl -n odp-data get secret garage-credentials >/dev/null 2>&1 \
  && kubectl -n odp-system get secret polaris-db-credentials >/dev/null 2>&1 \
  && kubectl -n odp-system get secret polaris-root-credentials >/dev/null 2>&1; then

  postgres_user="$(secret_value odp-data postgres-credentials username)"
  postgres_password="$(secret_value odp-data postgres-credentials password)"
  garage_access="$(secret_value odp-data garage-credentials access-key)"
  garage_secret="$(secret_value odp-data garage-credentials secret-key)"
  polaris_root_secret="$(secret_value odp-system polaris-root-credentials client-secret)"

  write_local_credentials \
    "$garage_access" "$garage_secret" "$polaris_root_secret" \
    "$postgres_user" "$postgres_password" "$grafana_password"

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

kubectl -n odp-data create secret generic postgres-credentials \
  --from-literal=username="$postgres_user" \
  --from-literal=password="$postgres_password" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n odp-data create secret generic garage-credentials \
  --from-literal=access-key="$garage_access" \
  --from-literal=secret-key="$garage_secret" \
  --from-literal=rpc-secret="$garage_rpc" \
  --from-literal=admin-token="$garage_admin" \
  --from-literal=metrics-token="$garage_metrics" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n odp-system create secret generic polaris-db-credentials \
  --from-literal=username="$postgres_user" \
  --from-literal=password="$postgres_password" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n odp-system create secret generic polaris-root-credentials \
  --from-literal=client-id=root \
  --from-literal=client-secret="$polaris_root_secret" \
  --dry-run=client -o yaml | kubectl apply -f -

write_local_credentials \
  "$garage_access" "$garage_secret" "$polaris_root_secret" \
  "$postgres_user" "$postgres_password" "$grafana_password"

echo "Generated new ephemeral standalone credentials in .local/credentials.env"
