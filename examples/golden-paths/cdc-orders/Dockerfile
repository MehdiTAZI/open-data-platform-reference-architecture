ARG RUNTIME_IMAGE=odp/spark-iceberg:4.1.3-iceberg1.11.0
FROM maven:3.9.11-eclipse-temurin-17 AS dependencies
WORKDIR /build
COPY pom.xml .
RUN mvn -B -ntp dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/deps

FROM ${RUNTIME_IMAGE}
USER root
COPY --from=dependencies /deps/ /opt/spark/jars/
COPY --chown=185:0 src/ /opt/odp/jobs/cdc-orders/
USER 185
