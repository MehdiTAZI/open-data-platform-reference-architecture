import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.sql.window import Window


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


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

source_schema = StructType(
    [
        StructField("lsn", LongType()),
        StructField("ts_ms", LongType()),
    ]
)

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


def process_batch(spark: SparkSession, batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    decoded = batch_df.withColumn("payload", F.from_json("event_value", envelope_schema))
    invalid = decoded.filter(F.col("payload.op").isNull() | ~F.col("payload.op").isin("c", "u", "d", "r"))
    if invalid.limit(1).count() > 0:
        raise ValueError(f"CDC batch {batch_id} contains an unsupported or malformed operation")

    enriched = (
        decoded.withColumn("order_id", F.coalesce("payload.after.order_id", "payload.before.order_id"))
        .withColumn("source_lsn", F.coalesce(F.col("payload.source.lsn"), F.lit(-1).cast("long")))
        .withColumn("source_ts_ms", F.col("payload.source.ts_ms"))
        .withColumn("op", F.col("payload.op"))
    )
    if enriched.filter(F.col("order_id").isNull()).limit(1).count() > 0:
        raise ValueError(f"CDC batch {batch_id} contains an event without order_id")

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

    latest_window = Window.partitionBy("order_id").orderBy(
        F.col("source_lsn").desc(), F.col("kafka_partition").desc(), F.col("kafka_offset").desc()
    )
    latest = current.withColumn("_rank", F.row_number().over(latest_window)).filter("_rank = 1").drop("_rank")
    latest.createOrReplaceTempView("incoming_cdc_current")

    newer = "(s.source_lsn > t.source_lsn OR (s.source_lsn = t.source_lsn AND s.kafka_offset >= t.kafka_offset))"
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
    gold = (
        silver.withColumn("order_date", F.to_date("order_ts"))
        .groupBy("order_date", "country")
        .agg(
            F.countDistinct("order_id").cast("long").alias("order_count"),
            F.sum("amount").cast("decimal(18,2)").alias("gross_amount"),
            F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(F.lit(0)))
            .cast("decimal(18,2)")
            .alias("completed_amount"),
        )
    )
    gold.writeTo("polaris.gold.daily_order_summary_cdc").using("iceberg").createOrReplace()
    print(f"CDC_BATCH_SUCCESS batch_id={batch_id} events={bronze.count()} current_rows={silver.count()}")


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
