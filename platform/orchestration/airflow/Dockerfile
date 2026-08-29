ARG AIRFLOW_IMAGE=apache/airflow:3.3.1
FROM ${AIRFLOW_IMAGE}

ARG AIRFLOW_VERSION=3.3.1
ARG KUBERNETES_PROVIDER_VERSION=10.21.1

RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-cncf-kubernetes==${KUBERNETES_PROVIDER_VERSION}" \
    && pip check

COPY --chown=airflow:0 platform/orchestration/airflow/dags/ /opt/airflow/dags/
COPY --chown=airflow:0 examples/golden-paths/batch-orders/kubernetes/job.yaml /opt/airflow/job-templates/batch-orders.yaml
