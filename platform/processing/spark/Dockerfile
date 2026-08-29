ARG SPARK_IMAGE=apache/spark:4.1.3-python3
FROM ${SPARK_IMAGE}

USER root
ARG ICEBERG_VERSION=1.11.0
ARG SCALA_BINARY_VERSION=2.13
ARG SPARK_MINOR=4.1
ARG MAVEN_BASE=https://repo1.maven.org/maven2/org/apache/iceberg

RUN set -eux; \
    runtime="iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_BINARY_VERSION}"; \
    runtime_jar="${runtime}-${ICEBERG_VERSION}.jar"; \
    aws_jar="iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fsSL "${MAVEN_BASE}/${runtime}/${ICEBERG_VERSION}/${runtime_jar}" -o "/opt/spark/jars/${runtime_jar}"; \
    curl -fsSL "${MAVEN_BASE}/${runtime}/${ICEBERG_VERSION}/${runtime_jar}.sha512" -o /tmp/runtime.sha512; \
    printf '%s  %s\n' "$(cat /tmp/runtime.sha512)" "/opt/spark/jars/${runtime_jar}" | sha512sum -c -; \
    curl -fsSL "${MAVEN_BASE}/iceberg-aws-bundle/${ICEBERG_VERSION}/${aws_jar}" -o "/opt/spark/jars/${aws_jar}"; \
    curl -fsSL "${MAVEN_BASE}/iceberg-aws-bundle/${ICEBERG_VERSION}/${aws_jar}.sha512" -o /tmp/aws.sha512; \
    printf '%s  %s\n' "$(cat /tmp/aws.sha512)" "/opt/spark/jars/${aws_jar}" | sha512sum -c -; \
    rm -f /tmp/*.sha512

USER 185
