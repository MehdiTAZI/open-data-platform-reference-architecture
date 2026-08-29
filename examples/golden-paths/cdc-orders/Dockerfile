ARG RUNTIME_IMAGE=odp/spark-iceberg:4.1.3-iceberg1.11.0
FROM maven:3.9.11-eclipse-temurin-17 AS dependencies
WORKDIR /build
COPY pom.xml .

# Copy only the Kafka connector runtime additions. Spark/Scala/Hadoop are already
# provided by the compatibility-locked base image and must not be overlaid with
# transitive versions from Maven.
RUN mvn -B -ntp dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DincludeArtifactIds=spark-sql-kafka-0-10_2.13,spark-token-provider-kafka-0-10_2.13,kafka-clients,commons-pool2,zstd-jni,snappy-java,lz4-java \
    -DoutputDirectory=/deps

FROM ${RUNTIME_IMAGE}
USER root

# Replace compression/Kafka client libraries as one controlled set so the Spark
# classpath contains one version of each implementation. The Spark/Scala/Hadoop
# libraries from the base image remain untouched.
RUN rm -f \
    /opt/spark/jars/kafka-clients-*.jar \
    /opt/spark/jars/lz4-java-*.jar \
    /opt/spark/jars/zstd-jni-*.jar \
    /opt/spark/jars/snappy-java-*.jar \
    /opt/spark/jars/commons-pool2-*.jar

COPY --from=dependencies /deps/ /opt/spark/jars/
COPY --chown=185:0 src/ /opt/odp/jobs/cdc-orders/
USER 185
