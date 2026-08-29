#!/usr/bin/env bash
set -euo pipefail

cluster_name="odp-local"

kind get clusters | grep -qx "$cluster_name" || {
  echo "Missing Kind cluster: $cluster_name" >&2
  exit 1
}

kubectl cluster-info >/dev/null
kubectl get namespace odp-system odp-data odp-observability >/dev/null

echo "[1/10] PostgreSQL"
kubectl -n odp-data exec deployment/postgres -- pg_isready -h localhost -p 5432 >/dev/null

echo "[2/10] Garage S3 endpoint"
kubectl -n odp-data exec deployment/garage -- /garage status >/dev/null

echo "[3/10] Kafka"
kubectl -n odp-data exec deployment/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka.odp-data.svc.cluster.local:9092 --list >/dev/null

echo "[4/10] Polaris"
kubectl -n odp-system get job polaris-bootstrap -o jsonpath='{.status.succeeded}' | grep -q '^1$'
kubectl -n odp-system get deployment polaris -o jsonpath='{.status.availableReplicas}' | grep -Eq '^[1-9]'

echo "[5/10] Trino"
kubectl -n odp-data exec deployment/trino -- trino --execute 'SELECT 1' >/dev/null

echo "[6/10] Spark on Kubernetes"
./scripts/spark-smoke-test.sh

echo "[7/10] Airflow"
kubectl -n odp-system exec deployment/airflow -- curl --fail --silent http://localhost:8080/api/v2/version >/dev/null

echo "[8/10] OpenTelemetry Collector"
kubectl -n odp-observability get deployment otel-collector -o jsonpath='{.status.availableReplicas}' | grep -Eq '^[1-9]'

echo "[9/10] Prometheus"
kubectl -n odp-observability exec deployment/prometheus -- wget -qO- http://localhost:9090/-/ready >/dev/null

echo "[10/10] Grafana"
kubectl -n odp-observability exec deployment/grafana -- wget -qO- http://localhost:3000/api/health >/dev/null

echo "All standalone smoke tests passed."
