from dataclasses import dataclass


@dataclass(frozen=True)
class CdcConfig:
    kafka_bootstrap: str = "kafka.odp-data.svc.cluster.local:9092"
    kafka_topic: str = "odp-commerce.source.orders"
    checkpoint_path: str = "/tmp/odp-cdc-orders-checkpoint"
    contract_path: str = "/opt/odp/contracts/batch-orders.yaml"
    bronze_table: str = "polaris.bronze.orders_cdc_events"
    silver_table: str = "polaris.silver.orders_cdc"
    quarantine_table: str = "polaris.quarantine.orders_cdc"
    gold_table: str = "polaris.gold.daily_order_summary_cdc"
    run_table: str = "polaris.platform.pipeline_runs"
    dq_results_table: str = "polaris.platform.data_quality_results"
    processed_events_table: str = "polaris.platform.cdc_processed_events"
