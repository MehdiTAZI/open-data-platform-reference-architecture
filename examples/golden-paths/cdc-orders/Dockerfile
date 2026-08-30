ARG RUNTIME_IMAGE=odp/spark-iceberg:4.1.3-iceberg1.11.0
FROM ${RUNTIME_IMAGE}

ARG PYYAML_VERSION=6.0.3

USER root
RUN python3 -m pip install --no-cache-dir "PyYAML==${PYYAML_VERSION}"
COPY --chown=185:0 platform/governance/data-quality/python/odp_data_quality/ /opt/odp/lib/odp_data_quality/
COPY --chown=185:0 examples/golden-paths/cdc-orders/src/ /opt/odp/jobs/cdc-orders/
COPY --chown=185:0 specs/data-contracts/examples/batch-orders.yaml /opt/odp/contracts/batch-orders.yaml
ENV PYTHONPATH=/opt/odp/lib:/opt/odp/jobs/cdc-orders
USER 185
