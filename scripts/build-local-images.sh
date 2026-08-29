#!/usr/bin/env bash
set -euo pipefail

cluster_name="odp-local"
runtime_image="odp/spark-iceberg:4.1.3-iceberg1.11.0"
batch_image="odp/batch-orders:0.1.0"

echo "Building $runtime_image"
docker build \
  --pull \
  --build-arg SPARK_IMAGE=apache/spark:4.1.3-python3 \
  --build-arg ICEBERG_VERSION=1.11.0 \
  --build-arg POSTGRES_JDBC_VERSION=42.7.7 \
  -t "$runtime_image" \
  platform/processing/spark

echo "Building $batch_image"
docker build \
  --build-arg RUNTIME_IMAGE="$runtime_image" \
  -t "$batch_image" \
  examples/golden-paths/batch-orders

if kind get clusters | grep -qx "$cluster_name"; then
  echo "Loading reference images into Kind cluster $cluster_name"
  kind load docker-image --name "$cluster_name" "$runtime_image" "$batch_image"
fi
