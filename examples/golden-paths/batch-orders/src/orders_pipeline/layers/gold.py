from decimal import Decimal

from pyspark.sql import DataFrame, functions as F


def transform(silver: DataFrame) -> DataFrame:
    return (
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
