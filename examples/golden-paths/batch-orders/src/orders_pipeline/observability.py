import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

from .config import PipelineConfig
from .context import PipelineRun
from .quality import RuleMetric, metrics_dataframe
from .storage import append_table

OPENLINEAGE_PRODUCER = "https://github.com/MehdiTAZI/open-data-platform-reference-architecture"
OPENLINEAGE_SCHEMA = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"


def persist_quality_metrics(
    spark: SparkSession,
    config: PipelineConfig,
    run: PipelineRun,
    metrics: tuple[RuleMetric, ...],
) -> None:
    if not metrics:
        return
    append_table(
        spark,
        metrics_dataframe(spark, run.run_id, run.pipeline, run.contract_version, metrics),
        config.dq_results_table,
    )


def persist_run(
    spark: SparkSession,
    config: PipelineConfig,
    run: PipelineRun,
    source_count: int,
    valid_count: int,
    rejected_count: int,
    status: str,
) -> None:
    record = spark.createDataFrame(
        [
            (
                run.run_id,
                run.pipeline,
                run.contract_version,
                source_count,
                valid_count,
                rejected_count,
                status,
            )
        ],
        "run_id string, pipeline string, contract_version string, input_rows long, valid_rows long, rejected_rows long, status string",
    ).withColumn("recorded_at", F.current_timestamp())
    append_table(spark, record, config.run_table)


def emit_openlineage(
    run: PipelineRun,
    event_type: str,
    inputs: list[tuple[str, str]],
    outputs: list[tuple[str, str]],
) -> None:
    event = {
        "eventTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "eventType": event_type,
        "producer": OPENLINEAGE_PRODUCER,
        "schemaURL": OPENLINEAGE_SCHEMA,
        "run": {"runId": run.run_id},
        "job": {"namespace": "https://opendataplatform.dev/jobs", "name": run.pipeline},
        "inputs": [{"namespace": namespace, "name": name} for namespace, name in inputs],
        "outputs": [{"namespace": namespace, "name": name} for namespace, name in outputs],
    }
    print("OPENLINEAGE_EVENT " + json.dumps(event, separators=(",", ":"), sort_keys=True))
