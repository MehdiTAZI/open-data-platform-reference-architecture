ARG RUNTIME_IMAGE=odp/spark-iceberg:4.1.3-iceberg1.11.0
FROM ${RUNTIME_IMAGE}

ARG PYYAML_VERSION=6.0.3

USER root
RUN python3 -m pip install --no-cache-dir "PyYAML==${PYYAML_VERSION}"
COPY --chown=185:0 examples/golden-paths/batch-orders/src/ /opt/odp/jobs/batch-orders/
COPY --chown=185:0 specs/data-contracts/examples/batch-orders.yaml /opt/odp/contracts/batch-orders.yaml
USER 185
