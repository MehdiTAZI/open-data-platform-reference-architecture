from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

ACCEPTED_STATUSES = ["COMPLETED", "CANCELLED", "PENDING"]


@dataclass(frozen=True)
class QualitySplit:
    valid: DataFrame
    invalid: DataFrame


def classify(df: DataFrame) -> QualitySplit:
    duplicate_window = Window.partitionBy("order_id")
    flagged = df.withColumn("_duplicate_count", F.count(F.lit(1)).over(duplicate_window))

    violations = [
        F.when(F.col("order_id").isNull(), F.lit("order_id_not_null")),
        F.when(F.col("customer_id").isNull() | (F.length(F.col("customer_id")) == 0), F.lit("customer_id_not_null")),
        F.when(F.col("order_ts").isNull(), F.lit("order_ts_not_null")),
        F.when(F.col("status").isNull(), F.lit("status_not_null")),
        F.when(~F.col("status").isin(ACCEPTED_STATUSES), F.lit("status_accepted_values")),
        F.when(F.col("amount").isNull(), F.lit("amount_not_null")),
        F.when(F.col("amount") < F.lit(0), F.lit("amount_non_negative")),
        F.when(F.col("country").isNull() | (F.length(F.col("country")) != 2), F.lit("country_iso2_shape")),
        F.when(F.col("_duplicate_count") > 1, F.lit("order_id_unique")),
    ]

    flagged = flagged.withColumn(
        "_dq_errors",
        F.filter(F.array(*violations), lambda value: value.isNotNull()),
    )

    valid = flagged.filter(F.size("_dq_errors") == 0).drop("_duplicate_count", "_dq_errors")
    invalid = (
        flagged.filter(F.size("_dq_errors") > 0)
        .drop("_duplicate_count")
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    return QualitySplit(valid=valid, invalid=invalid)
