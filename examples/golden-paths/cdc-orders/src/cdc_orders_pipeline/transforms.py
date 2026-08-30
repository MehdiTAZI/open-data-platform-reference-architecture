from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

_ORDER_SCHEMA = StructType(
    [
        StructField("order_id", LongType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_ts", LongType(), True),
        StructField("status", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("country", StringType(), True),
    ]
)
_SOURCE_SCHEMA = StructType(
    [
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("db", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("txId", LongType(), True),
        StructField("lsn", LongType(), True),
    ]
)
_ENVELOPE_SCHEMA = StructType(
    [
        StructField("before", _ORDER_SCHEMA, True),
        StructField("after", _ORDER_SCHEMA, True),
        StructField("source", _SOURCE_SCHEMA, True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
    ]
)


def parse_kafka_events(kafka_df: DataFrame) -> DataFrame:
    parsed = kafka_df.withColumn("_event", F.from_json(F.col("value").cast("string"), _ENVELOPE_SCHEMA))
    return (
        parsed.select(
            F.sha2(
                F.concat_ws(":", F.col("topic"), F.col("partition").cast("string"), F.col("offset").cast("string")),
                256,
            ).alias("_event_id"),
            F.col("topic").alias("_kafka_topic"),
            F.col("partition").cast("int").alias("_kafka_partition"),
            F.col("offset").cast("long").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.col("value").cast("string").alias("_raw_value"),
            F.col("_event.op").alias("_cdc_op"),
            F.col("_event.source.lsn").cast("long").alias("_source_lsn"),
            F.col("_event.source.txId").cast("long").alias("_source_tx_id"),
            F.to_timestamp(F.from_unixtime(F.col("_event.source.ts_ms") / F.lit(1000))).alias("_source_ts"),
            F.coalesce(F.col("_event.after.order_id"), F.col("_event.before.order_id")).cast("long").alias("order_id"),
            F.col("_event.after.customer_id").alias("customer_id"),
            F.col("_event.after.order_ts").cast("long").alias("_order_ts_ms"),
            F.col("_event.after.status").alias("status"),
            F.col("_event.after.amount").alias("_amount_raw"),
            F.col("_event.after.country").alias("country"),
        )
        .filter(F.col("_cdc_op").isin("r", "c", "u", "d"))
        .filter(F.col("order_id").isNotNull())
    )


def latest_change_per_order(events: DataFrame) -> DataFrame:
    order = Window.partitionBy("order_id").orderBy(
        F.col("_source_lsn").desc_nulls_last(),
        F.col("_kafka_partition").desc(),
        F.col("_kafka_offset").desc(),
    )
    return events.withColumn("_change_rank", F.row_number().over(order)).filter("_change_rank = 1").drop("_change_rank")


def canonical_upserts(changes: DataFrame, run_id: str, contract_version: str) -> DataFrame:
    return (
        changes.filter(F.col("_cdc_op").isin("r", "c", "u"))
        .select(
            F.col("order_id").cast("long").alias("order_id"),
            F.trim("customer_id").alias("customer_id"),
            F.to_timestamp(F.from_unixtime(F.col("_order_ts_ms") / F.lit(1000))).alias("order_ts"),
            F.upper(F.trim("status")).alias("status"),
            F.col("_amount_raw").cast("decimal(18,2)").alias("amount"),
            F.upper(F.trim("country")).alias("country"),
            "_event_id",
            "_source_lsn",
            "_source_tx_id",
            "_source_ts",
            "_cdc_op",
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
        )
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_pipeline", F.lit("cdc-orders"))
        .withColumn("_contract_version", F.lit(contract_version))
        .withColumn("_source_system", F.lit("postgresql-cdc"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn(
            "_record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("order_id").cast("string"),
                    F.coalesce(F.col("customer_id"), F.lit("<null>")),
                    F.coalesce(F.col("order_ts").cast("string"), F.lit("<null>")),
                    F.coalesce(F.col("status"), F.lit("<null>")),
                    F.coalesce(F.col("amount").cast("string"), F.lit("<null>")),
                    F.coalesce(F.col("country"), F.lit("<null>")),
                ),
                256,
            ),
        )
    )
