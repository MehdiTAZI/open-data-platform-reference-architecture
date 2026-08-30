from pyspark.sql import DataFrame, functions as F

from ..context import PipelineRun

REQUIRED_SOURCE_COLUMNS = [
    "order_id",
    "customer_id",
    "order_ts",
    "status",
    "amount",
    "country",
]


def validate_source_shape(df: DataFrame) -> None:
    missing = [column for column in REQUIRED_SOURCE_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Source contract violation; missing columns: {missing}")


def transform(source: DataFrame, run: PipelineRun) -> DataFrame:
    validate_source_shape(source)
    values = [
        F.coalesce(F.col(column).cast("string"), F.lit("<NULL>"))
        for column in REQUIRED_SOURCE_COLUMNS
    ]
    return (
        source.select(*REQUIRED_SOURCE_COLUMNS)
        .withColumn("_run_id", F.lit(run.run_id))
        .withColumn("_pipeline", F.lit(run.pipeline))
        .withColumn("_contract_version", F.lit(run.contract_version))
        .withColumn("_source_system", F.lit("standalone-postgres"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", F.sha2(F.concat_ws("||", *values), 256))
    )
