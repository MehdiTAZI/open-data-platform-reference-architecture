#!/usr/bin/env bash
set -euo pipefail

cluster_name="odp-local"
runtime_image="odp/spark-iceberg:4.1.3-iceberg1.11.0"
batch_image="odp/batch-orders:0.1.0"
cdc_image="odp/cdc-orders:0.1.0"
airflow_image="odp/airflow-orchestrator:3.3.1-k8s10.21.1"
pgjdbc_sha256="6e0e4cc2d8cae902084f8a2b18728b073a6fd9d1f87c9d8bff8f298c18185b93"

echo "Building $runtime_image"
docker build \
  --pull \
  --build-arg SPARK_IMAGE=apache/spark:4.1.3-python3 \
  --build-arg SPARK_VERSION=4.1.3 \
  --build-arg ICEBERG_VERSION=1.11.0 \
  --build-arg POSTGRES_JDBC_VERSION=42.7.13 \
  --build-arg POSTGRES_JDBC_SHA256="$pgjdbc_sha256" \
  -t "$runtime_image" \
  platform/processing/spark

echo "Building $batch_image"
docker build \
  --build-arg RUNTIME_IMAGE="$runtime_image" \
  --build-arg PYYAML_VERSION=6.0.3 \
  -f examples/golden-paths/batch-orders/Dockerfile \
  -t "$batch_image" \
  .

echo "Building $cdc_image"
docker build \
  --build-arg RUNTIME_IMAGE="$runtime_image" \
  --build-arg PYYAML_VERSION=6.0.3 \
  -f examples/golden-paths/cdc-orders/Dockerfile \
  -t "$cdc_image" \
  .

echo "Building $airflow_image"
docker build \
  --build-arg AIRFLOW_IMAGE=apache/airflow:3.3.1 \
  --build-arg AIRFLOW_VERSION=3.3.1 \
  --build-arg KUBERNETES_PROVIDER_VERSION=10.21.1 \
  -f platform/orchestration/airflow/Dockerfile \
  -t "$airflow_image" \
  .

if kind get clusters | grep -qx "$cluster_name"; then
  # kind v0.31's image loader does not understand the containerd config v4
  # used by Kubernetes 1.36 Kind nodes. Import the Docker archive directly into
  # each node's k8s.io containerd namespace instead. This preserves Kubernetes
  # 1.36 while keeping local images available with imagePullPolicy=IfNotPresent.
  mkdir -p .local
  image_archive=".local/odp-reference-images.tar"
  images=("$runtime_image" "$batch_image" "$cdc_image" "$airflow_image")

  echo "Saving reference images for Kind cluster $cluster_name"
  docker save -o "$image_archive" "${images[@]}"

  while IFS= read -r node; do
    [[ -n "$node" ]] || continue
    echo "Importing reference images into Kind node $node"
    docker exec -i "$node" ctr --namespace k8s.io images import - < "$image_archive"
  done < <(kind get nodes --name "$cluster_name")

  rm -f "$image_archive"
fi
