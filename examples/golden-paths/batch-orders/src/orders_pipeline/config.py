import os
from dataclasses import dataclass


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


@dataclass(frozen=True)
class PipelineConfig:
    jdbc_url: str = "jdbc:postgresql://postgres.odp-data.svc.cluster.local:5432/platform"
    jdbc_table: str = "source.orders"
    contract_path: str = "/opt/odp/contracts/batch-orders.yaml"
    catalog: str = "polaris"
    bronze_table: str = "polaris.bronze.orders_raw"
    silver_table: str = "polaris.silver.orders"
    quarantine_table: str = "polaris.quarantine.orders"
    gold_table: str = "polaris.gold.daily_order_summary"
    run_table: str = "polaris.platform.pipeline_runs"
    dq_results_table: str = "polaris.platform.data_quality_results"

    @property
    def postgres_user(self) -> str:
        return required("POSTGRES_USER")

    @property
    def postgres_password(self) -> str:
        return required("POSTGRES_PASSWORD")
