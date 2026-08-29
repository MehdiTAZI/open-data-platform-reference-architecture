ARG RUNTIME_IMAGE=odp/spark-iceberg:4.1.3-iceberg1.11.0
FROM ${RUNTIME_IMAGE}

USER root
COPY --chown=185:0 src/ /opt/odp/jobs/batch-orders/
USER 185
