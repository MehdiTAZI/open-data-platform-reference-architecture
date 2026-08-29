#!/usr/bin/env bash
set -euo pipefail

sql_file="/tmp/odp-lakehouse-smoke.sql"

kubectl -n odp-data exec deployment/spark-client -- /bin/bash -ec "cat > ${sql_file} <<'SQL'
CREATE NAMESPACE IF NOT EXISTS polaris.smoke;
DROP TABLE IF EXISTS polaris.smoke.engine_interop;
CREATE TABLE polaris.smoke.engine_interop (id BIGINT, engine STRING) USING iceberg;
INSERT INTO polaris.smoke.engine_interop VALUES (1, 'spark'), (2, 'iceberg');
SELECT * FROM polaris.smoke.engine_interop ORDER BY id;
SQL
/opt/spark/bin/spark-sql \\
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \\
  --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \\
  --conf spark.sql.catalog.polaris.type=rest \\
  --conf spark.sql.catalog.polaris.uri=http://polaris.odp-system.svc.cluster.local:8181/api/catalog \\
  --conf spark.sql.catalog.polaris.warehouse=odp \\
  --conf spark.sql.catalog.polaris.credential=\${POLARIS_CLIENT_ID}:\${POLARIS_CLIENT_SECRET} \\
  --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \\
  --conf spark.sql.catalog.polaris.header.Polaris-Realm=odp \\
  --conf spark.sql.catalog.polaris.io-impl=org.apache.iceberg.aws.s3.S3FileIO \\
  --conf spark.sql.catalog.polaris.s3.endpoint=http://garage.odp-data.svc.cluster.local:3900 \\
  --conf spark.sql.catalog.polaris.s3.path-style-access=true \\
  --conf spark.sql.catalog.polaris.s3.region=garage \\
  --conf spark.sql.catalog.polaris.s3.access-key-id=\${GARAGE_ACCESS_KEY} \\
  --conf spark.sql.catalog.polaris.s3.secret-access-key=\${GARAGE_SECRET_KEY} \\
  -f ${sql_file}"

result="$(kubectl -n odp-data exec deployment/trino -- trino --output-format TSV_HEADER --execute "SELECT id, engine FROM polaris.smoke.engine_interop ORDER BY id")"
printf '%s\n' "$result"
grep -q $'1\tspark' <<<"$result"
grep -q $'2\ticeberg' <<<"$result"

echo "Spark -> Iceberg/Polaris/S3 -> Trino interoperability passed."
