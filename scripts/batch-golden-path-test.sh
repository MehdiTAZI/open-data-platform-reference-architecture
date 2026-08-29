#!/usr/bin/env bash
set -euo pipefail

job_manifest="examples/golden-paths/batch-orders/kubernetes/job.yaml"

run_once() {
  kubectl -n odp-data delete job batch-orders --ignore-not-found --wait=true >/dev/null
  kubectl apply -f "$job_manifest" >/dev/null

  if ! kubectl -n odp-data wait --for=condition=complete job/batch-orders --timeout=420s; then
    kubectl -n odp-data describe job batch-orders || true
    kubectl -n odp-data logs job/batch-orders --all-containers=true || true
    return 1
  fi

  kubectl -n odp-data logs job/batch-orders --all-containers=true
}

validate_business_result() {
  local bronze_count
  local summary

  bronze_count="$(kubectl -n odp-data exec deployment/trino -- trino --output-format TSV --execute \
    'SELECT count(*) FROM polaris.bronze.orders_snapshot')"
  [[ "${bronze_count//$'\r'/}" == "6" ]]

  summary="$(kubectl -n odp-data exec deployment/trino -- trino --output-format TSV_HEADER --execute \
    "SELECT CAST(sum(order_count) AS BIGINT) AS orders, CAST(sum(gross_amount) AS VARCHAR) AS gross, CAST(sum(completed_amount) AS VARCHAR) AS completed FROM polaris.gold.daily_order_summary")"
  printf '%s\n' "$summary"
  grep -q $'6\t685.75\t550.50' <<<"$summary"
}

echo "[1/4] First batch execution"
run_once

echo "[2/4] Validate published result through Trino"
validate_business_result

echo "[3/4] Replay identical source snapshot"
run_once

echo "[4/4] Validate idempotent business result"
validate_business_result

echo "Batch golden path passed: PostgreSQL -> Spark -> Iceberg/Polaris -> Trino, including replay."
