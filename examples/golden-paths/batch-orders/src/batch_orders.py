import os
import sys
from decimal import Decimal

from pyspark.sql import SparkSession, functions as F


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
        SparkSession.builder.appName("odp-batch-orders")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config(
            "spark.sql.catalog.polaris.uri",
            "http://polaris.odp-system.svc.cluster.local:8181/api/catalog",
        )
        .config("spark.sql.catalog.polaris.warehouse", "odp")
        .config("spark.sql.catalog.polaris.credential", f"root:{polaris_secret}")
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.header.Polaris-Realm", "odp")
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(
            "spark.sql.catalog.polaris.s3.endpoint",
            "http://garage.odp-data.svc.cluster.local:3900",
        )
        .config("spark.sql.catalog.polaris.s3.path-style-access", "true")
        .config("spark.sql.catalog.polaris.s3.region", "garage")
        .config("spark.sql.catalog.polaris.s3.access-key-id", garage_access)
        .config("spark.sql.catalog.polaris.s3.secret-access-key", garage_secret)
        .getOrCreate()
    )


def validate_source(df) -> int:
    row_count = df.count()
    if row_count == 0:
        raise ValueError("Source snapshot is empty")

    required_columns = [
        "order_id",
        "customer_id",
        "order_ts",
        "status",
        "amount",
        "country",
    ]
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Source contract violation; missing columns: {missing}")

    null_condition = None
    for column in required_columns:
        current = F.col(column).isNull()
        null_condition = current if null_condition is None else (null_condition | current)
    if df.filter(null_condition).limit(1).count() > 0:
        raise ValueError("Source contract violation; required column contains NULL")

    if df.filter(F.col("amount") < F.lit(Decimal("0.00"))).limit(1).count() > 0:
        raise ValueError("Source contract violation; negative amount")

    duplicates = df.groupBy("order_id").count().filter(F.col("count") > 1).limit(1).count()
    if duplicates:
        raise ValueError("Source contract violation; duplicate order_id")

    return row_count


def replace_iceberg_table(df, table_name: str) -> None:
    df.writeTo(table_name).using("iceberg").createOrReplace()


def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    postgres_user = required("POSTGRES_USER")
    postgres_password = required("POSTGRES_PASSWORD")
    jdbc_url = "jdbc:postgresql://postgres.odp-data.svc.cluster.local:5432/platform"

    source = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "source.orders")
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .cache()
    )

    source_count = validate_source(source)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")

    bronze = (
        source.withColumn("_source_system", F.lit("standalone-postgres"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    silver = bronze.select(
        F.col("order_id").cast("long").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.col("order_ts").cast("timestamp").alias("order_ts"),
        F.to_date("order_ts").alias("order_date"),
        F.upper(F.trim("status")).alias("status"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.upper(F.trim("country")).alias("country"),
        F.col("_source_system"),
        F.col("_ingested_at"),
    )

    gold = (
        silver.groupBy("order_date", "country")
        .agg(
            F.countDistinct("order_id").cast("long").alias("order_count"),
            F.sum("amount").cast("decimal(18,2)").alias("gross_amount"),
            F.sum(
                F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(
                    F.lit(Decimal("0.00"))
                )
            )
            .cast("decimal(18,2)")
            .alias("completed_amount"),
        )
        .orderBy("order_date", "country")
    )

    replace_iceberg_table(bronze, "polaris.bronze.orders_snapshot")
    replace_iceberg_table(silver, "polaris.silver.orders")
    replace_iceberg_table(gold, "polaris.gold.daily_order_summary")

    silver_count = spark.table("polaris.silver.orders").count()
    if silver_count != source_count:
        raise RuntimeError(
            f"Publish verification failed: source={source_count}, silver={silver_count}"
        )

    print(
        f"BATCH_ORDERS_SUCCESS source_rows={source_count} "
        f"silver_rows={silver_count} gold_rows={gold.count()}"
    )
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
