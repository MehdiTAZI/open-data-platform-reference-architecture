from pyspark.sql import functions as F

from .config import PipelineConfig
from .context import PipelineRun
from .layers import bronze as bronze_layer
from .layers import gold as gold_layer
from .layers import silver as silver_layer
from .quality import classify
from .spark import build_spark
from .storage import append_table, ensure_namespaces, replace_table


def run_pipeline() -> int:
    config = PipelineConfig()
    run = PipelineRun.create()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        ensure_namespaces(spark)
        source = (
            spark.read.format("jdbc")
            .option("url", config.jdbc_url)
            .option("dbtable", config.jdbc_table)
            .option("user", config.postgres_user)
            .option("password", config.postgres_password)
            .option("driver", "org.postgresql.Driver")
            .load()
            .cache()
        )
        source_count = source.count()
        if source_count == 0:
            raise ValueError("Source snapshot is empty")

        bronze = bronze_layer.transform(source, run).cache()
        append_table(spark, bronze, config.bronze_table)

        candidate = silver_layer.transform(bronze)
        split = classify(candidate)
        valid = split.valid.cache()
        invalid = split.invalid.cache()
        valid_count = valid.count()
        invalid_count = invalid.count()

        if valid_count + invalid_count != source_count:
            raise RuntimeError(
                "Quality reconciliation failed: "
                f"source={source_count} valid={valid_count} invalid={invalid_count}"
            )
        if invalid_count:
            append_table(spark, invalid, config.quarantine_table)
        if valid_count == 0:
            raise RuntimeError("No valid rows remain after data quality classification")

        gold = gold_layer.transform(valid).cache()
        replace_table(valid, config.silver_table)
        replace_table(gold, config.gold_table)

        published_silver = spark.table(config.silver_table).count()
        published_orders = (
            spark.table(config.gold_table)
            .agg(F.sum("order_count").cast("long").alias("orders"))
            .first()["orders"]
        )
        if published_silver != valid_count or published_orders != valid_count:
            raise RuntimeError(
                "Publish reconciliation failed: "
                f"valid={valid_count} silver={published_silver} gold_orders={published_orders}"
            )

        run_record = spark.createDataFrame(
            [(run.run_id, run.pipeline, run.contract_version, source_count, valid_count, invalid_count, "SUCCESS")],
            "run_id string, pipeline string, contract_version string, input_rows long, valid_rows long, rejected_rows long, status string",
        ).withColumn("recorded_at", F.current_timestamp())
        append_table(spark, run_record, config.run_table)

        print(
            "BATCH_ORDERS_SUCCESS "
            f"run_id={run.run_id} source_rows={source_count} "
            f"silver_rows={valid_count} rejected_rows={invalid_count} gold_rows={gold.count()}"
        )
        return 0
    finally:
        spark.stop()
