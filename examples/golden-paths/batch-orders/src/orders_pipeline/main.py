from pyspark.sql import functions as F

from .config import PipelineConfig
from .context import PipelineRun
from .contracts import assert_required_columns, load_contract
from .layers import bronze as bronze_layer
from .layers import gold as gold_layer
from .layers import silver as silver_layer
from .observability import emit_openlineage, persist_quality_metrics, persist_run
from .quality import DataQualityFailure, evaluate
from .spark import build_spark
from .storage import append_table, ensure_namespaces, replace_table


def run_pipeline() -> int:
    config = PipelineConfig()
    contract = load_contract(config.contract_path)
    run = PipelineRun.create(contract.version)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    source_count = 0
    valid_count = 0
    invalid_count = 0
    run_recorded = False
    lineage_started = False
    lineage_inputs = [
        ("postgresql://postgres.odp-data.svc.cluster.local:5432", "platform.source.orders")
    ]
    lineage_outputs = [
        ("iceberg://polaris", "bronze.orders_raw"),
        ("iceberg://polaris", "silver.orders"),
        ("iceberg://polaris", "quarantine.orders"),
        ("iceberg://polaris", "gold.daily_order_summary"),
    ]

    try:
        ensure_namespaces(spark)
        emit_openlineage(run, "START", lineage_inputs, lineage_outputs)
        lineage_started = True

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
        assert_required_columns(source.columns, contract)
        source_count = source.count()

        bronze = bronze_layer.transform(source, run).cache()
        append_table(spark, bronze, config.bronze_table)

        candidate = silver_layer.transform(bronze)
        try:
            evaluation = evaluate(candidate, contract)
        except DataQualityFailure as exc:
            persist_quality_metrics(spark, config, run, exc.metrics)
            raise

        persist_quality_metrics(spark, config, run, evaluation.metrics)
        valid = evaluation.valid.cache()
        invalid = evaluation.invalid.cache()
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

        persist_run(
            spark,
            config,
            run,
            source_count=source_count,
            valid_count=valid_count,
            rejected_count=invalid_count,
            status="SUCCESS",
        )
        run_recorded = True
        emit_openlineage(run, "COMPLETE", lineage_inputs, lineage_outputs)

        print(
            "BATCH_ORDERS_SUCCESS "
            f"run_id={run.run_id} contract={contract.api_version} source_rows={source_count} "
            f"silver_rows={valid_count} rejected_rows={invalid_count} gold_rows={gold.count()}"
        )
        return 0
    except Exception:
        if not run_recorded:
            try:
                persist_run(
                    spark,
                    config,
                    run,
                    source_count=source_count,
                    valid_count=valid_count,
                    rejected_count=invalid_count,
                    status="FAILED",
                )
            except Exception as observability_error:
                print(f"RUN_METRIC_PERSIST_FAILED {observability_error}")
        if lineage_started:
            emit_openlineage(run, "FAIL", lineage_inputs, lineage_outputs)
        raise
    finally:
        spark.stop()
