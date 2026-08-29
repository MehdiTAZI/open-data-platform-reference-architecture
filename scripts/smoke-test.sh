#!/usr/bin/env bash
set -euo pipefail

cluster_name="odp-local"

kind get clusters | grep -qx "$cluster_name" || {
  echo "Missing Kind cluster: $cluster_name" >&2
  exit 1
}

kubectl cluster-info >/dev/null
kubectl get namespace odp-system odp-data odp-observability >/dev/null

echo "[1/7] PostgreSQL"
kubectl -n odp-data exec deployment/postgres -- \
  pg_isready -h localhost -p 5432 >/dev/null

echo "[2/7] Garage S3 endpoint"
kubectl -n odp-data exec deployment/garage -- /garage status >/dev/null

echo "[3/7] Kafka"
kubectl -n odp-data exec deployment/kafka -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka.odp-data.svc.cluster.local:9092 \
  --list >/dev/null

echo "[4/7] Polaris"
kubectl -n odp-system get job polaris-bootstrap -o jsonpath='{.status.succeeded}' | grep -q '^1$'
kubectl -n odp-system get deployment polaris -o jsonpath='{.status.availableReplicas}' | grep -Eq '^[1-9]'

echo "[5/7] Trino"
kubectl -n odp-data exec deployment/trino -- trino --execute 'SELECT 1' >/dev/null

echo "[6/7] Spark on Kubernetes"
./scripts/spark-smoke-test.sh

echo "[7/7] Airflow"
kubectl -n odp-system exec deployment/airflow -- \
  curl --fail --silent http://localhost:8080/api/v2/version >/dev/null

echo "All standalone smoke tests passed."
