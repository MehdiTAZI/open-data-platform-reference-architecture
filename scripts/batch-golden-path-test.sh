#!/usr/bin/env bash
set -euo pipefail

job_manifest="examples/golden-paths/batch-orders/kubernetes/job.yaml"
quality_rules_per_run=11

run_once() {
  local logs
  kubectl -n odp-data delete job batch-orders --ignore-not-found --wait=true >/dev/null
  kubectl apply -f "$job_manifest" >/dev/null

  if ! kubectl -n odp-data wait --for=condition=complete job/batch-orders --timeout=420s; then
    kubectl -n odp-data describe job batch-orders || true
    kubectl -n odp-data logs job/batch-orders --all-containers=true || true
    return 1
  fi

  logs="$(kubectl -n odp-data logs job/batch-orders --all-containers=true)"
  printf '%s\n' "$logs"
  grep -q 'OPENLINEAGE_EVENT .*"eventType":"START"' <<<"$logs"
  grep -q 'OPENLINEAGE_EVENT .*"eventType":"COMPLETE"' <<<"$logs"
}

validate_business_result() {
  local expected_bronze="$1"
  local expected_runs="$2"
  local expected_dq=$((expected_runs * quality_rules_per_run))
  local bronze_count
  local silver_count
  local run_count
  local dq_count
  local blocking_dq_count
  local summary

  bronze_count="$(kubectl -n odp-data exec deployment/trino -- trino --output-format TSV --execute \
    'SELECT count(*) FROM polaris.bronze.orders_raw')"
  [[ "${bronze_count//$'\r'/}" == "$expected_bronze" ]]

  silver_count="$(kubectl -n odp-data exec deployment/trino -- trino --output-format TSV --execute \
    'SELECT count(*) FROM polaris.silver.orders')"
  [[ "${silver_count//$'\r'/}" == "6" ]]

  run_count="$(kubectl -n odp-data exec deployment/trino -- trino --output-format TSV --execute \
    "SELECT count(*) FROM polaris.platform.pipeline_runs WHERE status = 'SUCCESS'")"
  [[ "${run_count//$'\r'/}" == "$expected_runs" ]]

  dq_count="$(kubectl -n odp-data exec deployment/trino -- trino --output-format TSV --execute \
    'SELECT count(*) FROM polaris.platform.data_quality_results')"
  [[ "${dq_count//$'\r'/}" == "$expected_dq" ]]

  blocking_dq_count="$(kubectl -n odp-data exec deployment/trino -- trino --output-format TSV --execute \
    "SELECT count(*) FROM polaris.platform.data_quality_results WHERE status IN ('FAIL', 'QUARANTINE')")"
  [[ "${blocking_dq_count//$'\r'/}" == "0" ]]

  summary="$(kubectl -n odp-data exec deployment/trino -- trino --output-format TSV_HEADER --execute \
    "SELECT CAST(sum(order_count) AS BIGINT) AS orders, CAST(sum(gross_amount) AS VARCHAR) AS gross, CAST(sum(completed_amount) AS VARCHAR) AS completed FROM polaris.gold.daily_order_summary")"
  printf '%s\n' "$summary"
  grep -q $'6\t685.75\t550.50' <<<"$summary"
}

echo "[1/4] First batch execution"
run_once

echo "[2/4] Validate first publication, contract metrics and lineage"
validate_business_result 6 1

echo "[3/4] Replay identical source snapshot"
run_once

echo "[4/4] Validate append-only Bronze and idempotent Silver/Gold"
validate_business_result 12 2

echo "Batch golden path passed: Medallion replay, contract DQ metrics and OpenLineage events validated."
