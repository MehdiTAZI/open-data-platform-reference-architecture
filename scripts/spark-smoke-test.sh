#!/usr/bin/env bash
set -euo pipefail

spark_image="apache/spark:4.2.0-python3"

kubectl -n odp-data exec deployment/spark-client -- /bin/bash -ec "
  jar=\$(find /opt/spark/examples/jars -name 'spark-examples_*.jar' | head -n1)
  test -n \"\$jar\"
  /opt/spark/bin/spark-submit \\
    --master k8s://https://kubernetes.default.svc \\
    --deploy-mode client \\
    --name odp-spark-smoke \\
    --class org.apache.spark.examples.SparkPi \\
    --conf spark.kubernetes.namespace=odp-data \\
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \\
    --conf spark.kubernetes.container.image=${spark_image} \\
    --conf spark.executor.instances=1 \\
    --conf spark.executor.cores=1 \\
    --conf spark.executor.memory=384m \\
    --conf spark.driver.host=\$(hostname -i) \\
    --conf spark.driver.bindAddress=0.0.0.0 \\
    local://\$jar 5
"
