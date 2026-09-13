import json
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, functions as F

from odp_data_quality.contracts import DATASET_RULE_TYPES, load_contract
from odp_data_quality.quality import DataQualityFailure, RuleMetric, evaluate, metrics_dataframe

from .config import CdcConfig
from .spark import build_spark
from .transforms import canonical_upserts, latest_change_per_order, parse_kafka_events

SILVER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_ts",
    "status",
    "amount",
    "country",
    "order_date",
    "_event_id",
    "_source_lsn",
    "_source_tx_id",
    "_source_ts",
    "_cdc_op",
    "_kafka_topic",
    "_kafka_partition",
    "_kafka_offset",
    "_run_id",
    "_pipeline",
    "_contract_version",
    "_source_system",
    "_ingested_at",
    "_record_hash",
]


def ensure_namespaces(spark: SparkSession) -> None:
    for namespace in ("bronze", "silver", "gold", "quarantine", "platform"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS polaris.{namespace}")


def append_table(spark: SparkSession, df: DataFrame, table_name: str) -> None:
    writer = df.writeTo(table_name).using("iceberg")
    if spark.catalog.tableExists(table_name):
        writer.append()
    else:
        writer.create()


def emit_lineage(run_id: str, event_type: str, config: CdcConfig) -> None:
    event = {
        "eventTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "eventType": event_type,
        "producer": "https://github.com/MehdiTAZI/open-data-platform-reference-architecture",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "run": {"runId": run_id},
        "job": {"namespace": "https://opendataplatform.dev/jobs", "name": "cdc-orders"},
        "inputs": [{"namespace": "kafka://odp-local", "name": config.kafka_topic}],
        "outputs": [
            {"namespace": "iceberg://polaris", "name": "bronze.orders_cdc_events"},
            {"namespace": "iceberg://polaris", "name": "silver.orders_cdc"},
            {"namespace": "iceberg://polaris", "name": "quarantine.orders_cdc"},
            {"namespace": "iceberg://polaris", "name": "gold.daily_order_summary_cdc"},
        ],
    }
    print("OPENLINEAGE_EVENT " + json.dumps(event, separators=(",", ":"), sort_keys=True))


def prefixed_metrics(prefix: str, metrics: tuple[RuleMetric, ...]) -> tuple[RuleMetric, ...]:
    return tuple(replace(metric, rule_name=f"{prefix}.{metric.rule_name}") for metric in metrics)


def persist_metrics(
    spark: SparkSession,
    config: CdcConfig,
    run_id: str,
    contract_version: str,
    metrics: tuple[RuleMetric, ...],
) -> None:
    if metrics:
        append_table(
            spark,
            metrics_dataframe(spark, run_id, "cdc-orders", contract_version, metrics),
            config.dq_results_table,
        )


def persist_run(
    spark: SparkSession,
    config: CdcConfig,
    run_id: str,
    contract_version: str,
    input_rows: int,
    valid_rows: int,
    rejected_rows: int,
    status: str,
) -> None:
    row = spark.createDataFrame(
        [(run_id, "cdc-orders", contract_version, input_rows, valid_rows, rejected_rows, status)],
        "run_id string, pipeline string, contract_version string, input_rows long, valid_rows long, rejected_rows long, status string",
    ).withColumn("recorded_at", F.current_timestamp())
    append_table(spark, row, config.run_table)


def _anti_join_event_ids(spark: SparkSession, df: DataFrame, table_name: str) -> DataFrame:
    current = df.dropDuplicates(["_event_id"])
    if not spark.catalog.tableExists(table_name):
        return current
    known = spark.table(table_name).select("_event_id").distinct()
    return current.join(known, "_event_id", "left_anti")


def unseen_bronze_events(spark: SparkSession, config: CdcConfig, batch: DataFrame) -> DataFrame:
    return _anti_join_event_ids(spark, batch, config.bronze_table)


def unprocessed_events(spark: SparkSession, config: CdcConfig, batch: DataFrame) -> DataFrame:
    return _anti_join_event_ids(spark, batch, config.processed_events_table)


def append_quarantine_once(spark: SparkSession, config: CdcConfig, invalid: DataFrame) -> int:
    pending = _anti_join_event_ids(spark, invalid, config.quarantine_table).cache()
    count = pending.count()
    if count:
        append_table(spark, pending, config.quarantine_table)
    return count


def mark_processed(spark: SparkSession, config: CdcConfig, events: DataFrame, run_id: str) -> None:
    markers = (
        events.select(
            "_event_id",
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            "_source_lsn",
        )
        .dropDuplicates(["_event_id"])
        .withColumn("pipeline", F.lit("cdc-orders"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("processed_at", F.current_timestamp())
    )
    append_table(spark, markers, config.processed_events_table)


def merge_deletes(spark: SparkSession, table_name: str, deletes: DataFrame) -> int:
    count = deletes.select("order_id").distinct().count()
    if count == 0 or not spark.catalog.tableExists(table_name):
        return count
    deletes.select("order_id").distinct().createOrReplaceTempView("cdc_order_deletes")
    spark.sql(
        f"""
        MERGE INTO {table_name} target
        USING cdc_order_deletes source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN DELETE
        """
    )
    return count


def merge_upserts(spark: SparkSession, table_name: str, valid: DataFrame) -> int:
    count = valid.count()
    if count == 0:
        return 0
    source = valid.select(*SILVER_COLUMNS)
    if not spark.catalog.tableExists(table_name):
        source.writeTo(table_name).using("iceberg").create()
        return count
    source.createOrReplaceTempView("cdc_order_upserts")
    spark.sql(
        f"""
        MERGE INTO {table_name} target
        USING cdc_order_upserts source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET
          customer_id = source.customer_id,
          order_ts = source.order_ts,
          status = source.status,
          amount = source.amount,
          country = source.country,
          order_date = source.order_date,
          _event_id = source._event_id,
          _source_lsn = source._source_lsn,
          _source_tx_id = source._source_tx_id,
          _source_ts = source._source_ts,
          _cdc_op = source._cdc_op,
          _kafka_topic = source._kafka_topic,
          _kafka_partition = source._kafka_partition,
          _kafka_offset = source._kafka_offset,
          _run_id = source._run_id,
          _pipeline = source._pipeline,
          _contract_version = source._contract_version,
          _source_system = source._source_system,
          _ingested_at = source._ingested_at,
          _record_hash = source._record_hash
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    return count


def rebuild_gold(spark: SparkSession, config: CdcConfig) -> None:
    silver = spark.table(config.silver_table)
    gold = (
        silver.groupBy("order_date", "country")
        .agg(
            F.count(F.lit(1)).cast("long").alias("order_count"),
            F.sum("amount").cast("decimal(20,2)").alias("gross_amount"),
            F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(F.lit(0)))
            .cast("decimal(20,2)")
            .alias("completed_amount"),
        )
    )
    gold.writeTo(config.gold_table).using("iceberg").createOrReplace()


def run_pipeline() -> int:
    config = CdcConfig()
    contract = load_contract(config.contract_path)
    row_contract = replace(
        contract,
        quality_rules=tuple(rule for rule in contract.quality_rules if rule.type not in DATASET_RULE_TYPES),
    )
    contract_version = contract.version
    run_id = str(uuid4())
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_namespaces(spark)
    emit_lineage(run_id, "START", config)

    counters = {"input": 0, "valid": 0, "rejected": 0}
    run_recorded = False

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        nonlocal counters
        deduplicated = batch_df.dropDuplicates(["_event_id"]).cache()

        bronze_candidates = unseen_bronze_events(spark, config, deduplicated).cache()
        bronze_count = bronze_candidates.count()
        if bronze_count:
            bronze = (
                bronze_candidates.withColumn("_run_id", F.lit(run_id))
                .withColumn("_pipeline", F.lit("cdc-orders"))
                .withColumn("_contract_version", F.lit(contract_version))
                .withColumn("_ingested_at", F.current_timestamp())
            )
            append_table(spark, bronze, config.bronze_table)

        pending = unprocessed_events(spark, config, deduplicated).cache()
        pending_count = pending.count()
        if pending_count == 0:
            print(
                f"CDC_MICROBATCH_NOOP run_id={run_id} batch_id={batch_id} "
                f"bronze_appended={bronze_count}"
            )
            return
        counters["input"] += pending_count

        latest = latest_change_per_order(pending).cache()
        deletes = latest.filter(F.col("_cdc_op") == "d")
        upserts = canonical_upserts(latest, run_id, contract_version).cache()

        valid_upserts = upserts
        rejected_count = 0
        if upserts.count() > 0:
            try:
                ingress = evaluate(upserts, row_contract)
            except DataQualityFailure as exc:
                persist_metrics(
                    spark,
                    config,
                    run_id,
                    contract_version,
                    prefixed_metrics("ingress", exc.metrics),
                )
                raise
            persist_metrics(
                spark,
                config,
                run_id,
                contract_version,
                prefixed_metrics("ingress", ingress.metrics),
            )
            valid_upserts = ingress.valid.cache()
            invalid = ingress.invalid.cache()
            rejected_count = invalid.count()
            if rejected_count:
                append_quarantine_once(spark, config, invalid)

        deleted_count = merge_deletes(spark, config.silver_table, deletes)
        upserted_count = merge_upserts(spark, config.silver_table, valid_upserts)
        counters["valid"] += deleted_count + upserted_count
        counters["rejected"] += rejected_count

        state = (
            spark.table(config.silver_table).cache()
            if spark.catalog.tableExists(config.silver_table)
            else valid_upserts.limit(0).cache()
        )
        try:
            state_evaluation = evaluate(state, contract)
        except DataQualityFailure as exc:
            persist_metrics(
                spark,
                config,
                run_id,
                contract_version,
                prefixed_metrics("state", exc.metrics),
            )
            raise
        persist_metrics(
            spark,
            config,
            run_id,
            contract_version,
            prefixed_metrics("state", state_evaluation.metrics),
        )
        if state_evaluation.invalid.count() > 0:
            raise RuntimeError("Trusted CDC Silver state violates quarantine-level contract rules")
        if spark.catalog.tableExists(config.silver_table):
            rebuild_gold(spark, config)

        # Only after trusted state and Gold have been published do these event IDs
        # become committed processing state. A failure before this point is replayed.
        mark_processed(spark, config, pending, run_id)

        print(
            f"CDC_MICROBATCH_SUCCESS run_id={run_id} batch_id={batch_id} "
            f"pending_events={pending_count} bronze_appended={bronze_count} "
            f"upserts={upserted_count} deletes={deleted_count} rejected={rejected_count}"
        )

    try:
        kafka = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap)
            .option("subscribe", config.kafka_topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        events = parse_kafka_events(kafka)
        query = (
            events.writeStream.foreachBatch(process_batch)
            .option("checkpointLocation", config.checkpoint_path)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        persist_run(
            spark,
            config,
            run_id,
            contract_version,
            counters["input"],
            counters["valid"],
            counters["rejected"],
            "SUCCESS",
        )
        run_recorded = True
        emit_lineage(run_id, "COMPLETE", config)
        print(
            f"CDC_ORDERS_SUCCESS run_id={run_id} input_rows={counters['input']} "
            f"valid_changes={counters['valid']} rejected_rows={counters['rejected']}"
        )
        return 0
    except Exception:
        if not run_recorded:
            try:
                persist_run(
                    spark,
                    config,
                    run_id,
                    contract_version,
                    counters["input"],
                    counters["valid"],
                    counters["rejected"],
                    "FAILED",
                )
            except Exception as metric_error:
                print(f"CDC_RUN_METRIC_PERSIST_FAILED {metric_error}")
        emit_lineage(run_id, "FAIL", config)
        raise
    finally:
        spark.stop()
