from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from .contracts import DATASET_RULE_TYPES, DataContract, QualityRule

_DURATION_RE = re.compile(r"^([1-9][0-9]*)(s|m|h|d)$")
_DURATION_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


@dataclass(frozen=True)
class RuleMetric:
    rule_name: str
    rule_type: str
    severity: str
    action: str
    evaluated_rows: int
    violation_count: int
    status: str


@dataclass(frozen=True)
class QualityEvaluation:
    valid: DataFrame
    invalid: DataFrame
    metrics: tuple[RuleMetric, ...]


class DataQualityFailure(RuntimeError):
    def __init__(self, metrics: tuple[RuleMetric, ...]):
        failed = [metric.rule_name for metric in metrics if metric.status == "FAIL"]
        super().__init__(f"Data quality fail rules violated: {failed}")
        self.metrics = metrics


def _duration_seconds(value: str) -> int:
    match = _DURATION_RE.fullmatch(value)
    if not match:
        raise ValueError(f"Invalid duration {value!r}; expected <integer>[s|m|h|d]")
    amount, unit = match.groups()
    return int(amount) * _DURATION_SECONDS[unit]


def _prepare_row_conditions(
    df: DataFrame, rules: tuple[QualityRule, ...]
) -> tuple[DataFrame, dict[str, Column], tuple[str, ...]]:
    flagged = df
    conditions: dict[str, Column] = {}
    temporary_columns: list[str] = []

    for index, rule in enumerate(rules):
        column = F.col(rule.column) if rule.column else None
        if rule.type == "unique":
            duplicate_column = f"_dq_unique_count_{index}"
            flagged = flagged.withColumn(
                duplicate_column,
                F.count(F.lit(1)).over(Window.partitionBy(rule.column)),
            )
            temporary_columns.append(duplicate_column)
            conditions[rule.name] = F.col(duplicate_column) > 1
        elif rule.type == "not_null":
            conditions[rule.name] = column.isNull()
        elif rule.type == "not_blank":
            conditions[rule.name] = column.isNull() | (F.length(F.trim(column.cast("string"))) == 0)
        elif rule.type == "accepted_values":
            conditions[rule.name] = column.isNotNull() & (~column.isin(list(rule.values)))
        elif rule.type == "range":
            condition = F.lit(False)
            if rule.minimum is not None:
                condition = condition | (column < F.lit(rule.minimum))
            if rule.maximum is not None:
                condition = condition | (column > F.lit(rule.maximum))
            conditions[rule.name] = column.isNotNull() & condition
        elif rule.type == "regex":
            conditions[rule.name] = column.isNotNull() & (~column.cast("string").rlike(rule.pattern))
        else:
            raise ValueError(f"Rule {rule.name} is not a row-level rule")

    return flagged, conditions, tuple(temporary_columns)


def _status(rule: QualityRule, violations: int) -> str:
    if violations == 0:
        return "PASS"
    if rule.action == "warn":
        return "WARN"
    if rule.action == "quarantine":
        return "QUARANTINE"
    return "FAIL"


def _dataset_metric(df: DataFrame, rule: QualityRule, row_count: int) -> RuleMetric:
    if rule.type == "row_count":
        violated = (
            (rule.minimum is not None and row_count < rule.minimum)
            or (rule.maximum is not None and row_count > rule.maximum)
        )
    elif rule.type == "freshness":
        latest = df.agg(F.max(F.col(rule.column)).alias("latest")).first()["latest"]
        if latest is None:
            violated = True
        else:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            violated = (now - latest).total_seconds() > _duration_seconds(rule.max_age)
    else:
        raise ValueError(f"Rule {rule.name} is not a dataset-level rule")

    violations = 1 if violated else 0
    return RuleMetric(
        rule_name=rule.name,
        rule_type=rule.type,
        severity=rule.severity,
        action=rule.action,
        evaluated_rows=row_count,
        violation_count=violations,
        status=_status(rule, violations),
    )


def evaluate(df: DataFrame, contract: DataContract) -> QualityEvaluation:
    row_rules = tuple(rule for rule in contract.quality_rules if rule.type not in DATASET_RULE_TYPES)
    dataset_rules = tuple(rule for rule in contract.quality_rules if rule.type in DATASET_RULE_TYPES)

    flagged, conditions, temporary_columns = _prepare_row_conditions(df, row_rules)
    row_count = flagged.count()

    aggregates = [
        F.sum(F.when(conditions[rule.name], F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias(f"metric_{index}")
        for index, rule in enumerate(row_rules)
    ]
    aggregate_values = flagged.agg(*aggregates).first() if aggregates else None

    metrics: list[RuleMetric] = []
    for index, rule in enumerate(row_rules):
        violations = int(aggregate_values[f"metric_{index}"] or 0)
        metrics.append(
            RuleMetric(
                rule_name=rule.name,
                rule_type=rule.type,
                severity=rule.severity,
                action=rule.action,
                evaluated_rows=row_count,
                violation_count=violations,
                status=_status(rule, violations),
            )
        )
    metrics.extend(_dataset_metric(flagged, rule, row_count) for rule in dataset_rules)
    metric_tuple = tuple(metrics)

    if any(metric.status == "FAIL" for metric in metric_tuple):
        raise DataQualityFailure(metric_tuple)

    quarantine_rules = tuple(rule for rule in row_rules if rule.action == "quarantine")
    warning_rules = tuple(rule for rule in row_rules if rule.action == "warn")

    error_values = [
        F.when(conditions[rule.name], F.lit(rule.name)) for rule in quarantine_rules
    ]
    warning_values = [
        F.when(conditions[rule.name], F.lit(rule.name)) for rule in warning_rules
    ]
    empty_array = F.array().cast("array<string>")

    flagged = flagged.withColumn(
        "_dq_errors",
        F.filter(F.array(*error_values), lambda value: value.isNotNull()) if error_values else empty_array,
    ).withColumn(
        "_dq_warnings",
        F.filter(F.array(*warning_values), lambda value: value.isNotNull()) if warning_values else empty_array,
    )

    valid = flagged.filter(F.size("_dq_errors") == 0).drop(
        *temporary_columns, "_dq_errors", "_dq_warnings"
    )
    invalid = (
        flagged.filter(F.size("_dq_errors") > 0)
        .drop(*temporary_columns)
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    return QualityEvaluation(valid=valid, invalid=invalid, metrics=metric_tuple)


def metrics_dataframe(
    spark: SparkSession,
    run_id: str,
    pipeline: str,
    contract_version: str,
    metrics: tuple[RuleMetric, ...],
) -> DataFrame:
    rows = [
        (
            run_id,
            pipeline,
            contract_version,
            metric.rule_name,
            metric.rule_type,
            metric.severity,
            metric.action,
            metric.evaluated_rows,
            metric.violation_count,
            metric.status,
        )
        for metric in metrics
    ]
    return spark.createDataFrame(
        rows,
        "run_id string, pipeline string, contract_version string, rule_name string, rule_type string, severity string, action string, evaluated_rows long, violation_count long, status string",
    ).withColumn("recorded_at", F.current_timestamp())
