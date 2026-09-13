#!/usr/bin/env bash
set -euo pipefail

job_manifest="examples/golden-paths/cdc-orders/kubernetes/job.yaml"
topic="odp-commerce.source.orders"
port_forward_pid=""

cleanup() {
  if [[ -n "$port_forward_pid" ]]; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

decode_secret() {
  local key="$1"
  kubectl -n odp-data get secret postgres-credentials -o "jsonpath={.data.${key}}" \
    | python3 -c 'import base64,sys; print(base64.b64decode(sys.stdin.buffer.read()).decode())'
}

postgres_user="$(decode_secret username)"
postgres_password="$(decode_secret password)"

register_connector() {
  mkdir -p .local
  kubectl -n odp-data port-forward svc/debezium-connect 18083:8083 >.local/debezium-port-forward.log 2>&1 &
  port_forward_pid="$!"
  POSTGRES_USER="$postgres_user" POSTGRES_PASSWORD="$postgres_password" \
    python3 scripts/register-cdc-connector.py --url http://127.0.0.1:18083
  kill "$port_forward_pid" >/dev/null 2>&1 || true
  wait "$port_forward_pid" 2>/dev/null || true
  port_forward_pid=""
}

wait_for_topic() {
  for _ in $(seq 1 60); do
    if kubectl -n odp-data exec deployment/kafka -- \
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka.odp-data.svc.cluster.local:9092 --list \
      | grep -qx "$topic"; then
      return
    fi
    sleep 1
  done
  echo "CDC topic did not appear: $topic" >&2
  return 1
}

run_cdc_job() {
  local logs
  kubectl -n odp-data delete job cdc-orders --ignore-not-found --wait=true >/dev/null
  kubectl apply -f "$job_manifest" >/dev/null
  if ! kubectl -n odp-data wait --for=condition=complete job/cdc-orders --timeout=420s; then
    kubectl -n odp-data describe job cdc-orders || true
    kubectl -n odp-data logs job/cdc-orders --all-containers=true || true
    return 1
  fi
  logs="$(kubectl -n odp-data logs job/cdc-orders --all-containers=true)"
  printf '%s\n' "$logs"
  grep -q 'OPENLINEAGE_EVENT .*"eventType":"START"' <<<"$logs"
  grep -q 'OPENLINEAGE_EVENT .*"eventType":"COMPLETE"' <<<"$logs"
}

query_scalar() {
  local sql="$1"
  kubectl -n odp-data exec deployment/trino -- trino --output-format TSV --execute "$sql" | tr -d '\r'
}

wait_for_initial_snapshot() {
  for _ in $(seq 1 5); do
    run_cdc_job
    if [[ "$(query_scalar 'SELECT count(*) FROM polaris.silver.orders_cdc')" == "6" ]]; then
      return
    fi
    sleep 2
  done
  echo "CDC initial snapshot did not converge to six rows" >&2
  return 1
}

apply_source_changes() {
  kubectl -n odp-data exec deployment/postgres -- env PGPASSWORD="$postgres_password" \
    psql -v ON_ERROR_STOP=1 -U "$postgres_user" -d platform -c "
      UPDATE source.orders SET amount = 95.00 WHERE order_id = 1002;
      INSERT INTO source.orders(order_id, customer_id, order_ts, status, amount, country)
        VALUES (1007, 'C007', TIMESTAMP '2026-08-30 10:00:00', 'COMPLETED', 75.00, 'MA');
      DELETE FROM source.orders WHERE order_id = 1003;
    " >/dev/null
}

validate_initial() {
  [[ "$(query_scalar 'SELECT count(*) FROM polaris.bronze.orders_cdc_events')" == "6" ]]
  [[ "$(query_scalar 'SELECT count(*) FROM polaris.silver.orders_cdc')" == "6" ]]
  [[ "$(query_scalar 'SELECT count(*) FROM polaris.platform.cdc_processed_events')" == "6" ]]
  [[ "$(query_scalar "SELECT count(*) FROM polaris.platform.data_quality_results WHERE pipeline = 'cdc-orders'")" -gt 0 ]]
}

validate_changed_state() {
  [[ "$(query_scalar 'SELECT count(*) FROM polaris.bronze.orders_cdc_events')" == "9" ]]
  [[ "$(query_scalar 'SELECT count(*) FROM polaris.platform.cdc_processed_events')" == "9" ]]
  [[ "$(query_scalar 'SELECT count(*) FROM polaris.silver.orders_cdc')" == "6" ]]
  [[ "$(query_scalar 'SELECT count(*) FROM polaris.silver.orders_cdc WHERE order_id = 1003')" == "0" ]]
  [[ "$(query_scalar 'SELECT count(*) FROM polaris.silver.orders_cdc WHERE order_id = 1007')" == "1" ]]
  [[ "$(query_scalar "SELECT CAST(amount AS VARCHAR) FROM polaris.silver.orders_cdc WHERE order_id = 1002")" == "95.00" ]]

  local summary
  summary="$(kubectl -n odp-data exec deployment/trino -- trino --output-format TSV_HEADER --execute \
    "SELECT CAST(sum(order_count) AS BIGINT) AS orders, CAST(sum(gross_amount) AS VARCHAR) AS gross, CAST(sum(completed_amount) AS VARCHAR) AS completed FROM polaris.gold.daily_order_summary_cdc")"
  printf '%s\n' "$summary"
  grep -q $'6\t730.50\t640.50' <<<"$summary"
}

echo "[1/7] Register Debezium PostgreSQL connector"
register_connector

echo "[2/7] Wait for CDC topic"
wait_for_topic

echo "[3/7] Consume and validate initial snapshot"
wait_for_initial_snapshot
validate_initial

echo "[4/7] Apply update + insert + delete at source"
apply_source_changes
sleep 3

echo "[5/7] Consume incremental changes"
run_cdc_job
validate_changed_state

echo "[6/7] Replay Kafka history without new source changes"
run_cdc_job

echo "[7/7] Validate replay-safe Bronze, processing commit state and current Silver/Gold"
validate_changed_state
[[ "$(query_scalar "SELECT count(*) FROM polaris.platform.pipeline_runs WHERE pipeline = 'cdc-orders' AND status = 'FAILED'")" == "0" ]]

echo "CDC golden path passed: PostgreSQL -> Debezium -> Kafka -> Spark Structured Streaming -> Iceberg, including update/insert/delete, durable processing commits and replay."
