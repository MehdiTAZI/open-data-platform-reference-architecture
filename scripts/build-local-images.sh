#!/usr/bin/env bash
set -euo pipefail

cluster_name="odp-local"
image="odp/spark-iceberg:4.1.3-iceberg1.11.0"

echo "Building $image"
docker build \
  --pull \
  --build-arg SPARK_IMAGE=apache/spark:4.1.3-python3 \
  --build-arg ICEBERG_VERSION=1.11.0 \
  -t "$image" \
  platform/processing/spark

if kind get clusters | grep -qx "$cluster_name"; then
  echo "Loading $image into Kind cluster $cluster_name"
  kind load docker-image --name "$cluster_name" "$image"
fi
