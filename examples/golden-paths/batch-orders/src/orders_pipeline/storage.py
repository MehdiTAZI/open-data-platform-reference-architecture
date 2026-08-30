from pyspark.sql import DataFrame, SparkSession


def ensure_namespaces(spark: SparkSession) -> None:
    for namespace in ("bronze", "silver", "gold", "quarantine", "platform"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS polaris.{namespace}")


def append_table(spark: SparkSession, df: DataFrame, table_name: str) -> None:
    writer = df.writeTo(table_name).using("iceberg")
    if spark.catalog.tableExists(table_name):
        writer.append()
    else:
        writer.create()


def replace_table(df: DataFrame, table_name: str) -> None:
    df.writeTo(table_name).using("iceberg").createOrReplace()
