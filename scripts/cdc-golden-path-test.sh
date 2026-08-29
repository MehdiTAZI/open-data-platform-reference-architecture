#!/usr/bin/env bash
set -euo pipefail

connector_manifest="examples/golden-paths/cdc-orders/kubernetes/connector-setup.yaml"
stream_manifest="examples/golden-paths/cdc-orders/kubernetes/streaming.yaml"

postgres_sql() {
  kubectl -n odp-data exec -i deployment/postgres -- /bin/bash -ec 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d platform' <<<"$1"
}

trino_query() {
  kubectl -n odp-data exec deployment/trino -- trino --output-format TSV --execute "$1" 2>/dev/null | tr -d '\r'
}

wait_for_query() {
  local sql="$1"
  local expected="$2"
  local label="$3"
  local result=""
  for _ in $(seq 1 120); do
    result="$(trino_query "$sql" || true)"
    if [[ "$result" == "$expected" ]]; then
      echo "$label: $result"
      return 0
    fi
    sleep 3
  done
  echo "Timed out waiting for $label. Last result: $result" >&2
  return 1
}

summary_sql="SELECT CAST(sum(order_count) AS VARCHAR), CAST(sum(gross_amount) AS VARCHAR), CAST(sum(completed_amount) AS VARCHAR) FROM polaris.gold.daily_order_summary_cdc"
ids_sql="SELECT array_join(array_agg(CAST(order_id AS VARCHAR) ORDER BY order_id), ',') FROM polaris.silver.orders_cdc"

reset_source() {
  postgres_sql "
    DELETE FROM source.orders WHERE order_id NOT IN (1001,1002,1003,1004,1005,1006);
    INSERT INTO source.orders (order_id, customer_id, order_ts, status, amount, country) VALUES
      (1001, 'C001', TIMESTAMP '2026-08-25 10:15:00', 'COMPLETED', 120.50, 'MA'),
      (1002, 'C002', TIMESTAMP '2026-08-25 11:00:00', 'COMPLETED', 80.00, 'FR'),
      (1003, 'C001', TIMESTAMP '2026-08-25 12:30:00', 'CANCELLED', 45.25, 'MA'),
      (1004, 'C003', TIMESTAMP '2026-08-26 09:00:00', 'COMPLETED', 200.00, 'ES'),
      (1005, 'C004', TIMESTAMP '2026-08-26 15:45:00', 'COMPLETED', 150.00, 'MA'),
      (1006, 'C002', TIMESTAMP '2026-08-26 17:20:00', 'PENDING', 90.00, 'FR')
    ON CONFLICT (order_id) DO UPDATE SET
      customer_id=EXCLUDED.customer_id, order_ts=EXCLUDED.order_ts, status=EXCLUDED.status,
      amount=EXCLUDED.amount, country=EXCLUDED.country;
  " >/dev/null
}

echo "[1/8] Configure dedicated PostgreSQL replication role/publication and Debezium connector"
kubectl -n odp-data delete job cdc-orders-connector-setup --ignore-not-found --wait=true >/dev/null
kubectl apply -f "$connector_manifest" >/dev/null
kubectl -n odp-data wait --for=condition=complete job/cdc-orders-connector-setup --timeout=300s
kubectl -n odp-data exec deployment/debezium-connect -- curl -fsS http://localhost:8083/connectors/odp-orders/status | grep -q 'RUNNING'

echo "[2/8] Start Spark Structured Streaming materialization"
kubectl apply -f "$stream_manifest" >/dev/null
kubectl -n odp-data rollout status deployment/cdc-orders --timeout=420s

echo "[3/8] Establish canonical baseline"
reset_source
wait_for_query "$summary_sql" $'6\t685.75\t550.50' "baseline summary"
wait_for_query "$ids_sql" "1001,1002,1003,1004,1005,1006" "baseline IDs"

echo "[4/8] Apply update/delete/insert source transaction"
postgres_sql "
  UPDATE source.orders SET amount = 95.00 WHERE order_id = 1002;
  DELETE FROM source.orders WHERE order_id = 1003;
  INSERT INTO source.orders (order_id, customer_id, order_ts, status, amount, country)
    VALUES (1007, 'C005', TIMESTAMP '2026-08-27 08:30:00', 'COMPLETED', 70.00, 'MA')
    ON CONFLICT (order_id) DO UPDATE SET amount=EXCLUDED.amount, status=EXCLUDED.status;
" >/dev/null
wait_for_query "$summary_sql" $'6\t725.50\t635.50' "mutated summary"
wait_for_query "$ids_sql" "1001,1002,1004,1005,1006,1007" "mutated IDs"

echo "[5/8] Restart streaming driver with the same checkpoint"
kubectl -n odp-data rollout restart deployment/cdc-orders >/dev/null
kubectl -n odp-data rollout status deployment/cdc-orders --timeout=420s
wait_for_query "$summary_sql" $'6\t725.50\t635.50' "post-restart summary"

echo "[6/8] Apply a post-restart change"
postgres_sql "UPDATE source.orders SET amount = 130.00 WHERE order_id = 1001;" >/dev/null
wait_for_query "$summary_sql" $'6\t735.00\t645.00' "post-restart mutation"

echo "[7/8] Verify transport metadata is retained"
bronze_count="$(trino_query 'SELECT CAST(count(*) AS VARCHAR) FROM polaris.bronze.orders_cdc_events')"
[[ "$bronze_count" =~ ^[0-9]+$ ]] && (( bronze_count >= 10 ))


echo "[8/8] Restore canonical source state for repeatability"
reset_source
wait_for_query "$summary_sql" $'6\t685.75\t550.50' "restored summary"
wait_for_query "$ids_sql" "1001,1002,1003,1004,1005,1006" "restored IDs"

echo "CDC golden path passed: PostgreSQL WAL -> Debezium -> Kafka -> Spark -> Iceberg/Polaris -> Trino."
