#!/usr/bin/env bash
set -euo pipefail

kind get clusters | grep -qx odp-local || { echo "Missing Kind cluster: odp-local" >&2; exit 1; }
kubectl cluster-info >/dev/null
kubectl get namespace odp-system odp-data odp-observability >/dev/null

echo "[1/12] PostgreSQL"
kubectl -n odp-data exec deployment/postgres -- pg_isready -h localhost -p 5432 >/dev/null

echo "[2/12] Garage S3 endpoint"
kubectl -n odp-data exec deployment/garage -- /garage status >/dev/null

echo "[3/12] Kafka"
kubectl -n odp-data exec deployment/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka.odp-data.svc.cluster.local:9092 --list >/dev/null

echo "[4/12] Debezium Kafka Connect"
kubectl -n odp-data exec deployment/debezium-connect -- curl -fsS http://localhost:8083/connectors >/dev/null

echo "[5/12] Polaris service and catalog"
kubectl -n odp-system get job polaris-bootstrap -o jsonpath='{.status.succeeded}' | grep -q '^1$'
kubectl -n odp-system get job polaris-catalog-setup -o jsonpath='{.status.succeeded}' | grep -q '^1$'

echo "[6/12] Trino"
kubectl -n odp-data exec deployment/trino -- trino --execute 'SELECT 1' >/dev/null

echo "[7/12] Spark on Kubernetes"
./scripts/spark-smoke-test.sh

echo "[8/12] Spark/Iceberg/Polaris/Trino interoperability"
./scripts/lakehouse-smoke-test.sh

echo "[9/12] Airflow"
kubectl -n odp-system exec deployment/airflow -- curl --fail --silent http://localhost:8080/api/v2/version >/dev/null

echo "[10/12] OpenTelemetry Collector"
kubectl -n odp-observability get deployment otel-collector -o jsonpath='{.status.availableReplicas}' | grep -Eq '^[1-9]'

echo "[11/12] Prometheus"
kubectl -n odp-observability exec deployment/prometheus -- wget -qO- http://localhost:9090/-/ready >/dev/null

echo "[12/12] Grafana"
kubectl -n odp-observability get deployment grafana -o jsonpath='{.status.availableReplicas}' | grep -Eq '^[1-9]'

echo "All standalone smoke tests passed."
