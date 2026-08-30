ARG SPARK_IMAGE=apache/spark:4.1.3-python3
FROM ${SPARK_IMAGE}

USER root
ARG SPARK_VERSION=4.1.3
ARG ICEBERG_VERSION=1.11.0
ARG POSTGRES_JDBC_VERSION=42.7.13
ARG POSTGRES_JDBC_SHA256=6e0e4cc2d8cae902084f8a2b18728b073a6fd9d1f87c9d8bff8f298c18185b93
ARG SCALA_BINARY_VERSION=2.13
ARG SPARK_MINOR=4.1
ARG MAVEN_BASE=https://repo1.maven.org/maven2
ARG PGJDBC_RELEASE_BASE=https://github.com/pgjdbc/pgjdbc/releases/download

RUN set -eux; \
    runtime="iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_BINARY_VERSION}"; \
    runtime_jar="${runtime}-${ICEBERG_VERSION}.jar"; \
    aws_jar="iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    pg_jar="postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    curl -fsSL "${MAVEN_BASE}/org/apache/iceberg/${runtime}/${ICEBERG_VERSION}/${runtime_jar}" -o "/opt/spark/jars/${runtime_jar}"; \
    curl -fsSL "${MAVEN_BASE}/org/apache/iceberg/${runtime}/${ICEBERG_VERSION}/${runtime_jar}.sha512" -o /tmp/runtime.sha512; \
    printf '%s  %s\n' "$(cat /tmp/runtime.sha512)" "/opt/spark/jars/${runtime_jar}" | sha512sum -c -; \
    curl -fsSL "${MAVEN_BASE}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/${aws_jar}" -o "/opt/spark/jars/${aws_jar}"; \
    curl -fsSL "${MAVEN_BASE}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/${aws_jar}.sha512" -o /tmp/aws.sha512; \
    printf '%s  %s\n' "$(cat /tmp/aws.sha512)" "/opt/spark/jars/${aws_jar}" | sha512sum -c -; \
    curl -fsSL "${PGJDBC_RELEASE_BASE}/REL${POSTGRES_JDBC_VERSION}/${pg_jar}" -o "/opt/spark/jars/${pg_jar}"; \
    printf '%s  %s\n' "${POSTGRES_JDBC_SHA256}" "/opt/spark/jars/${pg_jar}" | sha256sum -c -; \
    rm -f /tmp/*.sha512

# Resolve the official Spark Kafka connector and its transitive runtime dependencies
# at image-build time so production jobs do not depend on Maven network access.
RUN set -eux; \
    cat > /tmp/resolve-kafka.py <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("resolve-kafka-dependencies").getOrCreate()
spark.stop()
PY
RUN set -eux; \
    /opt/spark/bin/spark-submit \
      --master local[1] \
      --conf spark.jars.ivy=/tmp/ivy \
      --packages "org.apache.spark:spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}:${SPARK_VERSION}" \
      /tmp/resolve-kafka.py; \
    cp /tmp/ivy/jars/*.jar /opt/spark/jars/; \
    test -f "/opt/spark/jars/org.apache.spark_spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar"; \
    test -f "/opt/spark/jars/org.apache.spark_spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar"; \
    rm -rf /tmp/ivy /tmp/resolve-kafka.py

USER 185
