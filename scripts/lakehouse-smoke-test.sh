#!/usr/bin/env bash
set -euo pipefail

pod="$(kubectl -n odp-data get pod -l app.kubernetes.io/name=spark-client -o jsonpath='{.items[0].metadata.name}')"
kubectl -n odp-data cp examples/00-lakehouse-smoke/lakehouse_smoke.py "$pod:/tmp/lakehouse_smoke.py"

kubectl -n odp-data exec "$pod" -- /bin/bash -ec '
  /opt/spark/bin/spark-submit \
    --master k8s://https://kubernetes.default.svc \
    --deploy-mode client \
    --name odp-lakehouse-smoke \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0 \
    --conf spark.kubernetes.namespace=odp-data \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=apache/spark:4.1.3-python3 \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=512m \
    --conf spark.driver.host=$(hostname -i) \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.polaris.type=rest \
    --conf spark.sql.catalog.polaris.uri=http://polaris.odp-system.svc.cluster.local:8181/api/catalog \
    --conf spark.sql.catalog.polaris.warehouse=lakehouse \
    --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
    --conf spark.sql.catalog.polaris.credential=root:${POLARIS_ROOT_SECRET} \
    --conf spark.sql.catalog.polaris.oauth2-server-uri=http://polaris.odp-system.svc.cluster.local:8181/api/catalog/v1/oauth/tokens \
    --conf spark.sql.catalog.polaris.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.polaris.s3.endpoint=http://garage.odp-data.svc.cluster.local:3900 \
    --conf spark.sql.catalog.polaris.s3.path-style-access=true \
    --conf spark.sql.catalog.polaris.s3.access-key-id=${ODP_GARAGE_ACCESS_KEY} \
    --conf spark.sql.catalog.polaris.s3.secret-access-key=${ODP_GARAGE_SECRET_KEY} \
    --conf spark.sql.catalog.polaris.client.region=garage \
    /tmp/lakehouse_smoke.py
'

result="$(kubectl -n odp-data exec deployment/trino -- trino --catalog lakehouse --schema reference --output-format TSV --execute "SELECT count(*) FROM engine_interop WHERE engine IN ('spark','trino-readable')")"
test "$result" = "2" || { echo "Unexpected Trino result: $result" >&2; exit 1; }

echo "Spark -> Iceberg -> Polaris -> S3 -> Trino interoperability passed."
