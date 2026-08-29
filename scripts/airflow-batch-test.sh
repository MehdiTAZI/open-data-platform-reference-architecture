#!/usr/bin/env bash
set -euo pipefail

logical_date="2026-08-29T02:00:00+00:00"

echo "Verifying Airflow DAG discovery..."
kubectl -n odp-system exec deployment/airflow -- \
  airflow dags list --output plain | grep -q 'batch_orders_snapshot'

echo "Executing DAG through Airflow's DAG test runner..."
kubectl -n odp-system exec deployment/airflow -- \
  airflow dags test batch_orders_snapshot "$logical_date"

echo "Validating Airflow-orchestrated publication through Trino..."
result="$(kubectl -n odp-data exec deployment/trino -- trino --output-format TSV_HEADER --execute \
  "SELECT CAST(sum(order_count) AS BIGINT) AS orders, CAST(sum(gross_amount) AS VARCHAR) AS gross, CAST(sum(completed_amount) AS VARCHAR) AS completed FROM polaris.gold.daily_order_summary")"
printf '%s\n' "$result"
grep -q $'6\t685.75\t550.50' <<<"$result"

echo "Airflow thin orchestration passed: DAG -> Kubernetes Job -> Spark batch application."
