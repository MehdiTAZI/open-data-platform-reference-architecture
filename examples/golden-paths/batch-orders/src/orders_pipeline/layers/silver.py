from pyspark.sql import DataFrame, functions as F


def transform(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.col("order_id").cast("long").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.col("order_ts").cast("timestamp").alias("order_ts"),
        F.to_date("order_ts").alias("order_date"),
        F.upper(F.trim("status")).alias("status"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.upper(F.trim("country")).alias("country"),
        "_run_id",
        "_pipeline",
        "_contract_version",
        "_source_system",
        "_ingested_at",
        "_record_hash",
    )
