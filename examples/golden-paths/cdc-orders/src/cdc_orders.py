import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.sql.window import Window


ALLOWED_OPERATIONS = ("c", "u", "d", "r")
ALLOWED_STATUSES = ("COMPLETED", "CANCELLED", "PENDING")


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def assert_no_violations(df, condition, message: str) -> None:
    if df.filter(condition).limit(1).count() > 0:
        raise ValueError(message)


def assert_unique(df, keys, message: str) -> None:
    if df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count() > 0:
        raise ValueError(message)


def build_spark() -> SparkSession:
    polaris_secret = required("POLARIS_CLIENT_SECRET")
    garage_access = required("GARAGE_ACCESS_KEY")
    garage_secret = required("GARAGE_SECRET_KEY")

    return (
        SparkSession.builder.appName("odp-cdc-orders")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", "http://polaris.odp-system.svc.cluster.local:8181/api/catalog")
        .config("spark.sql.catalog.polaris.warehouse", "odp")
        .config("spark.sql.catalog.polaris.credential", f"root:{polaris_secret}")
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.header.Polaris-Realm", "odp")
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.polaris.s3.endpoint", "http://garage.odp-data.svc.cluster.local:3900")
        .config("spark.sql.catalog.polaris.s3.path-style-access", "true")
        .config("spark.sql.catalog.polaris.s3.region", "garage")
        .config("spark.sql.catalog.polaris.s3.access-key-id", garage_access)
        .config("spark.sql.catalog.polaris.s3.secret-access-key", garage_secret)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


order_schema = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer_id", StringType()),
        StructField("order_ts", LongType()),
        StructField("status", StringType()),
        StructField("amount", StringType()),
        StructField("country", StringType()),
    ]
)

source_schema = StructType([StructField("lsn", LongType()), StructField("ts_ms", LongType())])

envelope_schema = StructType(
    [
        StructField("before", order_schema),
        StructField("after", order_schema),
        StructField("source", source_schema),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
    ]
)


def ensure_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS polaris.bronze.orders_cdc_events (
          kafka_partition INT,
          kafka_offset BIGINT,
          kafka_timestamp TIMESTAMP,
          event_key STRING,
          event_value STRING,
          order_id BIGINT,
          op STRING,
          source_lsn BIGINT,
          source_ts_ms BIGINT,
          processed_at TIMESTAMP
        ) USING iceberg
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS polaris.silver.orders_cdc (
          order_id BIGINT,
          customer_id STRING,
          order_ts TIMESTAMP,
          status STRING,
          amount DECIMAL(18,2),
          country STRING,
          source_lsn BIGINT,
          source_ts_ms BIGINT,
          kafka_partition INT,
          kafka_offset BIGINT,
          updated_at TIMESTAMP
        ) USING iceberg
        """
    )


def validate_bronze_input(decoded, batch_id: int) -> None:
    assert_no_violations(
        decoded,
        F.col("event_value").isNull()
        | F.col("event_key").isNull()
        | F.col("payload").isNull()
        | F.col("payload.source").isNull()
        | F.col("payload.op").isNull()
        | ~F.col("payload.op").isin(*ALLOWED_OPERATIONS)
        | F.col("kafka_partition").isNull()
        | (F.col("kafka_partition") < 0)
        | F.col("kafka_offset").isNull()
        | (F.col("kafka_offset") < 0),
        f"CDC batch {batch_id} violates the Bronze transport/event contract",
    )


def validate_silver(silver, batch_id: int) -> None:
    assert_unique(silver, ["order_id"], f"CDC batch {batch_id} produced duplicate Silver order_id values")
    assert_no_violations(
        silver,
        F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | (F.length(F.trim(F.col("customer_id"))) == 0)
        | F.col("order_ts").isNull()
        | F.col("status").isNull()
        | ~F.col("status").isin(*ALLOWED_STATUSES)
        | F.col("amount").isNull()
        | (F.col("amount") < 0)
        | F.col("country").isNull()
        | ~F.col("country").rlike("^[A-Z]{2}$")
        | F.col("source_lsn").isNull()
        | (F.col("source_lsn") < -1)
        | F.col("kafka_partition").isNull()
        | (F.col("kafka_partition") < 0)
        | F.col("kafka_offset").isNull()
        | (F.col("kafka_offset") < 0),
        f"CDC batch {batch_id} violates the Silver current-state contract",
    )


def validate_gold(gold, batch_id: int) -> None:
    assert_unique(gold, ["order_date", "country"], f"CDC batch {batch_id} produced duplicate Gold grain rows")
    assert_no_violations(
        gold,
        F.col("order_date").isNull()
        | F.col("country").isNull()
        | (F.col("order_count") <= 0)
        | (F.col("completed_order_count") < 0)
        | (F.col("completed_order_count") > F.col("order_count"))
        | F.col("gross_amount").isNull()
        | (F.col("gross_amount") < 0)
        | F.col("completed_amount").isNull()
        | (F.col("completed_amount") < 0)
        | (F.col("completed_amount") > F.col("gross_amount")),
        f"CDC batch {batch_id} violates the Gold serving contract",
    )


def validate_silver_gold_reconciliation(silver, gold, batch_id: int) -> None:
    zero_decimal = F.lit(0).cast("decimal(18,2)")
    silver_metrics = silver.agg(
        F.count("*").cast("long").alias("order_count"),
        F.coalesce(F.sum("amount"), zero_decimal).cast("decimal(18,2)").alias("gross_amount"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("completed_order_count"),
        F.coalesce(
            F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(zero_decimal)),
            zero_decimal,
        )
        .cast("decimal(18,2)")
        .alias("completed_amount"),
    ).first()
    gold_metrics = gold.agg(
        F.coalesce(F.sum("order_count"), F.lit(0)).cast("long").alias("order_count"),
        F.coalesce(F.sum("gross_amount"), zero_decimal).cast("decimal(18,2)").alias("gross_amount"),
        F.coalesce(F.sum("completed_order_count"), F.lit(0)).cast("long").alias("completed_order_count"),
        F.coalesce(F.sum("completed_amount"), zero_decimal).cast("decimal(18,2)").alias("completed_amount"),
    ).first()
    metric_names = ("order_count", "gross_amount", "completed_order_count", "completed_amount")
    mismatches = [name for name in metric_names if silver_metrics[name] != gold_metrics[name]]
    if mismatches:
        raise ValueError(
            f"CDC batch {batch_id} failed Silver->Gold reconciliation for: {', '.join(mismatches)}"
        )


def process_batch(spark: SparkSession, batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    decoded = batch_df.withColumn("payload", F.from_json("event_value", envelope_schema))
    validate_bronze_input(decoded, batch_id)

    enriched = (
        decoded.withColumn("order_id", F.coalesce("payload.after.order_id", "payload.before.order_id"))
        .withColumn("source_lsn", F.coalesce(F.col("payload.source.lsn"), F.lit(-1).cast("long")))
        .withColumn("source_ts_ms", F.col("payload.source.ts_ms"))
        .withColumn("op", F.col("payload.op"))
    )
    assert_no_violations(
        enriched,
        F.col("order_id").isNull(),
        f"CDC batch {batch_id} contains an event without order_id",
    )

    # Bronze is the replay/audit ledger: raw payload plus source/transport metadata only.
    bronze = enriched.select(
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.col("event_key"),
        F.col("event_value"),
        F.col("order_id"),
        F.col("op"),
        F.col("source_lsn"),
        F.col("source_ts_ms"),
        F.current_timestamp().alias("processed_at"),
    )
    bronze.createOrReplaceTempView("incoming_cdc_bronze")
    spark.sql(
        """
        MERGE INTO polaris.bronze.orders_cdc_events t
        USING incoming_cdc_bronze s
          ON t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    # Silver is the typed, normalized and deduplicated current business state.
    current = enriched.select(
        F.col("order_id"),
        F.col("op"),
        F.col("payload.after.customer_id").alias("customer_id"),
        F.timestamp_millis(F.col("payload.after.order_ts")).alias("order_ts"),
        F.upper(F.trim(F.col("payload.after.status"))).alias("status"),
        F.col("payload.after.amount").cast("decimal(18,2)").alias("amount"),
        F.upper(F.trim(F.col("payload.after.country"))).alias("country"),
        F.col("source_lsn"),
        F.col("source_ts_ms"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.current_timestamp().alias("updated_at"),
    )
    assert_no_violations(
        current,
        (F.col("op") != "d")
        & (
            F.col("customer_id").isNull()
            | (F.length(F.trim(F.col("customer_id"))) == 0)
            | F.col("order_ts").isNull()
            | F.col("status").isNull()
            | ~F.col("status").isin(*ALLOWED_STATUSES)
            | F.col("amount").isNull()
            | (F.col("amount") < 0)
            | F.col("country").isNull()
            | ~F.col("country").rlike("^[A-Z]{2}$")
        ),
        f"CDC batch {batch_id} violates the Orders contract before Silver promotion",
    )

    latest_window = Window.partitionBy("order_id").orderBy(
        F.col("source_lsn").desc(), F.col("kafka_partition").desc(), F.col("kafka_offset").desc()
    )
    latest = current.withColumn("_rank", F.row_number().over(latest_window)).filter("_rank = 1").drop("_rank")
    latest.createOrReplaceTempView("incoming_cdc_current")

    # Equal LSN+offset is the same transport event and must be a physical no-op on replay.
    newer = "(s.source_lsn > t.source_lsn OR (s.source_lsn = t.source_lsn AND s.kafka_offset > t.kafka_offset))"
    spark.sql(
        f"""
        MERGE INTO polaris.silver.orders_cdc t
        USING incoming_cdc_current s
          ON t.order_id = s.order_id
        WHEN MATCHED AND {newer} AND s.op = 'd' THEN DELETE
        WHEN MATCHED AND {newer} AND s.op <> 'd' THEN UPDATE SET
          customer_id = s.customer_id,
          order_ts = s.order_ts,
          status = s.status,
          amount = s.amount,
          country = s.country,
          source_lsn = s.source_lsn,
          source_ts_ms = s.source_ts_ms,
          kafka_partition = s.kafka_partition,
          kafka_offset = s.kafka_offset,
          updated_at = s.updated_at
        WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT (
          order_id, customer_id, order_ts, status, amount, country,
          source_lsn, source_ts_ms, kafka_partition, kafka_offset, updated_at
        ) VALUES (
          s.order_id, s.customer_id, s.order_ts, s.status, s.amount, s.country,
          s.source_lsn, s.source_ts_ms, s.kafka_partition, s.kafka_offset, s.updated_at
        )
        """
    )

    silver = spark.table("polaris.silver.orders_cdc")
    validate_silver(silver, batch_id)

    # Gold owns a declared business grain and derives only from validated Silver.
    gold = (
        silver.withColumn("order_date", F.to_date("order_ts"))
        .groupBy("order_date", "country")
        .agg(
            F.count("*").cast("long").alias("order_count"),
            F.sum("amount").cast("decimal(18,2)").alias("gross_amount"),
            F.sum(F.when(F.col("status") == "COMPLETED", F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("completed_order_count"),
            F.sum(
                F.when(F.col("status") == "COMPLETED", F.col("amount"))
                .otherwise(F.lit(0).cast("decimal(18,2)"))
            )
            .cast("decimal(18,2)")
            .alias("completed_amount"),
        )
    )
    validate_gold(gold, batch_id)
    validate_silver_gold_reconciliation(silver, gold, batch_id)
    gold.writeTo("polaris.gold.daily_order_summary_cdc").using("iceberg").createOrReplace()
    print(
        "CDC_BATCH_SUCCESS "
        f"batch_id={batch_id} bronze_events={bronze.count()} silver_rows={silver.count()} "
        f"gold_rows={gold.count()} quality_gates=passed"
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)

    topic = os.environ.get("CDC_TOPIC", "odp.source.orders")
    checkpoint = required("CHECKPOINT_LOCATION")

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka.odp-data.svc.cluster.local:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", "1000")
        .load()
        .select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("event_key"),
            F.col("value").cast("string").alias("event_value"),
        )
    )

    query = (
        stream.writeStream.queryName("odp-orders-cdc")
        .foreachBatch(lambda df, batch_id: process_batch(spark, df, batch_id))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="5 seconds")
        .start()
    )
    Path("/tmp/odp-cdc-ready").touch()
    query.awaitTermination()


if __name__ == "__main__":
    main()
