#!/usr/bin/env bash
set -euo pipefail

connector_manifest="examples/golden-paths/cdc-orders/kubernetes/connector-setup.yaml"
stream_manifest="examples/golden-paths/cdc-orders/kubernetes/streaming.yaml"

postgres_sql() {
  kubectl -n odp-data exec -i deployment/postgres -- /bin/bash -ec "psql -v ON_ERROR_STOP=1 -U \"\$POSTGRES_USER\" -d platform" <<<"$1"
}

postgres_query() {
  kubectl -n odp-data exec -i deployment/postgres -- /bin/bash -ec "psql -v ON_ERROR_STOP=1 -At -F '|' -U \"\$POSTGRES_USER\" -d platform" <<<"$1" | tr -d '\r'
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

assert_query_equals() {
  local sql="$1"
  local expected="$2"
  local label="$3"
  local result
  result="$(trino_query "$sql")"
  if [[ "$result" != "$expected" ]]; then
    echo "Assertion failed for $label. Expected '$expected', got '$result'." >&2
    return 1
  fi
  echo "$label: passed"
}

assert_zero() {
  assert_query_equals "$1" "0" "$2"
}

assert_source_silver_reconciled() {
  local source_rows
  local silver_rows
  source_rows="$(postgres_query "
    SELECT order_id, customer_id, to_char(order_ts, 'YYYY-MM-DD HH24:MI:SS'), status, amount::text, country
    FROM source.orders ORDER BY order_id;
  ")"
  silver_rows="$(trino_query "
    SELECT CAST(order_id AS VARCHAR), customer_id, date_format(order_ts, '%Y-%m-%d %H:%i:%s'),
           status, CAST(amount AS VARCHAR), country
    FROM polaris.silver.orders_cdc ORDER BY order_id
  " | tr '\t' '|')"
  if [[ "$source_rows" != "$silver_rows" ]]; then
    echo "Source -> Silver reconciliation failed." >&2
    printf 'PostgreSQL:\n%s\nSilver:\n%s\n' "$source_rows" "$silver_rows" >&2
    return 1
  fi
  echo "Source -> Silver reconciliation: passed"
}

gold_reconciliation_sql="
WITH expected AS (
  SELECT CAST(order_ts AS DATE) AS order_date, country,
         count(*) AS order_count,
         CAST(sum(amount) AS DECIMAL(18,2)) AS gross_amount,
         CAST(sum(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS BIGINT) AS completed_order_count,
         CAST(sum(CASE WHEN status = 'COMPLETED' THEN amount ELSE DECIMAL '0.00' END) AS DECIMAL(18,2)) AS completed_amount
  FROM polaris.silver.orders_cdc GROUP BY 1, 2
), actual AS (
  SELECT order_date, country, order_count, gross_amount, completed_order_count, completed_amount
  FROM polaris.gold.daily_order_summary_cdc
), missing AS (
  SELECT * FROM expected EXCEPT SELECT * FROM actual
), unexpected AS (
  SELECT * FROM actual EXCEPT SELECT * FROM expected
)
SELECT CAST((SELECT count(*) FROM missing) + (SELECT count(*) FROM unexpected) AS VARCHAR)
"

assert_zone_quality() {
  echo "Running Bronze/Silver/Gold quality gates..."
  assert_zero "SELECT CAST(count(*) AS VARCHAR) FROM (SELECT kafka_partition, kafka_offset FROM polaris.bronze.orders_cdc_events GROUP BY 1,2 HAVING count(*) > 1)" "Bronze transport-key uniqueness"
  assert_zero "SELECT CAST(count(*) AS VARCHAR) FROM polaris.bronze.orders_cdc_events WHERE kafka_partition IS NULL OR kafka_partition < 0 OR kafka_offset IS NULL OR kafka_offset < 0 OR event_key IS NULL OR event_value IS NULL OR order_id IS NULL OR op IS NULL OR op NOT IN ('c','u','d','r') OR source_lsn IS NULL OR source_lsn < -1" "Bronze transport/event contract"
  assert_zero "SELECT CAST(count(*) AS VARCHAR) FROM (SELECT order_id FROM polaris.silver.orders_cdc GROUP BY 1 HAVING count(*) > 1)" "Silver business-key uniqueness"
  assert_zero "SELECT CAST(count(*) AS VARCHAR) FROM polaris.silver.orders_cdc WHERE order_id IS NULL OR customer_id IS NULL OR trim(customer_id) = '' OR order_ts IS NULL OR status IS NULL OR status NOT IN ('COMPLETED','CANCELLED','PENDING') OR amount IS NULL OR amount < 0 OR country IS NULL OR NOT regexp_like(country, '^[A-Z]{2}$') OR source_lsn IS NULL OR source_lsn < -1 OR kafka_partition IS NULL OR kafka_partition < 0 OR kafka_offset IS NULL OR kafka_offset < 0" "Silver current-state contract"
  assert_zero "SELECT CAST(count(*) AS VARCHAR) FROM (SELECT order_date, country FROM polaris.gold.daily_order_summary_cdc GROUP BY 1,2 HAVING count(*) > 1)" "Gold grain uniqueness"
  assert_zero "SELECT CAST(count(*) AS VARCHAR) FROM polaris.gold.daily_order_summary_cdc WHERE order_date IS NULL OR country IS NULL OR order_count <= 0 OR completed_order_count < 0 OR completed_order_count > order_count OR gross_amount IS NULL OR gross_amount < 0 OR completed_amount IS NULL OR completed_amount < 0 OR completed_amount > gross_amount" "Gold business invariants"
  assert_zero "$gold_reconciliation_sql" "Silver -> Gold grain reconciliation"
  assert_source_silver_reconciled
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
    ON CONFLICT (order_id) DO UPDATE SET customer_id=EXCLUDED.customer_id, order_ts=EXCLUDED.order_ts,
      status=EXCLUDED.status, amount=EXCLUDED.amount, country=EXCLUDED.country;
  " >/dev/null
}

echo "[1/10] Configure dedicated PostgreSQL replication role/publication and Debezium connector"
kubectl -n odp-data delete job cdc-orders-connector-setup --ignore-not-found --wait=true >/dev/null
kubectl apply -f "$connector_manifest" >/dev/null
kubectl -n odp-data wait --for=condition=complete job/cdc-orders-connector-setup --timeout=300s
kubectl -n odp-data exec deployment/debezium-connect -- curl -fsS http://localhost:8083/connectors/odp-orders/status | grep -q 'RUNNING'

echo "[2/10] Start Spark Structured Streaming materialization"
kubectl apply -f "$stream_manifest" >/dev/null
kubectl -n odp-data rollout status deployment/cdc-orders --timeout=420s

echo "[3/10] Establish canonical baseline"
reset_source
wait_for_query "$summary_sql" $'6\t685.75\t550.50' "baseline summary"
wait_for_query "$ids_sql" "1001,1002,1003,1004,1005,1006" "baseline IDs"

echo "[4/10] Assert baseline zone contracts and reconciliations"
assert_zone_quality

echo "[5/10] Apply update/delete/insert source transaction"
postgres_sql "UPDATE source.orders SET amount = 95.00 WHERE order_id = 1002; DELETE FROM source.orders WHERE order_id = 1003; INSERT INTO source.orders (order_id, customer_id, order_ts, status, amount, country) VALUES (1007, 'C005', TIMESTAMP '2026-08-27 08:30:00', 'COMPLETED', 70.00, 'MA') ON CONFLICT (order_id) DO UPDATE SET amount=EXCLUDED.amount, status=EXCLUDED.status;" >/dev/null
wait_for_query "$summary_sql" $'6\t725.50\t635.50' "mutated summary"
wait_for_query "$ids_sql" "1001,1002,1004,1005,1006,1007" "mutated IDs"

echo "[6/10] Assert mutation semantics and zone quality"
assert_query_equals "SELECT CAST(count(*) AS VARCHAR) FROM polaris.bronze.orders_cdc_events WHERE order_id = 1003 AND op = 'd'" "1" "Bronze delete-event retention"
assert_zone_quality

echo "[7/10] Restart streaming driver with the same checkpoint"
kubectl -n odp-data rollout restart deployment/cdc-orders >/dev/null
kubectl -n odp-data rollout status deployment/cdc-orders --timeout=420s
wait_for_query "$summary_sql" $'6\t725.50\t635.50' "post-restart summary"
assert_zone_quality

echo "[8/10] Apply a post-restart change and reconcile all zones"
postgres_sql "UPDATE source.orders SET amount = 130.00 WHERE order_id = 1001;" >/dev/null
wait_for_query "$summary_sql" $'6\t735.00\t645.00' "post-restart mutation"
assert_zone_quality

echo "[9/10] Verify retained transport lineage and replay safety"
bronze_count="$(trino_query 'SELECT CAST(count(*) AS VARCHAR) FROM polaris.bronze.orders_cdc_events')"
[[ "$bronze_count" =~ ^[0-9]+$ ]] && (( bronze_count >= 10 ))
assert_zero "SELECT CAST(count(*) AS VARCHAR) FROM (SELECT kafka_partition, kafka_offset FROM polaris.bronze.orders_cdc_events GROUP BY 1,2 HAVING count(*) > 1)" "Bronze replay idempotence"

echo "[10/10] Restore canonical source state for repeatability"
reset_source
wait_for_query "$summary_sql" $'6\t685.75\t550.50' "restored summary"
wait_for_query "$ids_sql" "1001,1002,1003,1004,1005,1006" "restored IDs"
assert_zone_quality

echo "CDC golden path passed with Bronze/Silver/Gold contracts, assertions and reconciliations."
