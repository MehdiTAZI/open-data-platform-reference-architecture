from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator

DAG_ID = "batch_orders_snapshot"
JOB_TEMPLATE = "/opt/airflow/job-templates/batch-orders.yaml"

with DAG(
    dag_id=DAG_ID,
    description="Orchestrate the bounded orders snapshot golden path",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["odp", "golden-path", "batch", "commerce"],
) as dag:
    run_batch_orders = KubernetesJobOperator(
        task_id="run_batch_orders",
        name="batch-orders-airflow",
        namespace="odp-data",
        job_template_file=JOB_TEMPLATE,
        kubernetes_conn_id=None,
        in_cluster=True,
        wait_until_job_complete=True,
        job_poll_interval=5,
        get_logs=True,
        log_events_on_failure=True,
        random_name_suffix=True,
        deferrable=False,
        labels={
            "odp.io/orchestrated-by": "airflow",
            "odp.io/golden-path": "batch",
        },
    )
