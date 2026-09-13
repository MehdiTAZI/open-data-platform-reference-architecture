from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

SUPPORTED_API_VERSION = "odp/v1alpha2"
ROW_RULE_TYPES = {"not_null", "not_blank", "unique", "accepted_values", "range", "regex"}
DATASET_RULE_TYPES = {"freshness", "row_count"}
RULE_TYPES = ROW_RULE_TYPES | DATASET_RULE_TYPES
ACTIONS = {"fail", "quarantine", "warn"}
SEVERITIES = {"error", "warning"}


@dataclass(frozen=True)
class QualityRule:
    name: str
    type: str
    severity: str
    action: str
    column: str | None = None
    values: tuple[Any, ...] = ()
    minimum: float | None = None
    maximum: float | None = None
    pattern: str | None = None
    max_age: str | None = None


@dataclass(frozen=True)
class DataContract:
    api_version: str
    name: str
    owner: str
    fields: tuple[dict[str, Any], ...]
    quality_rules: tuple[QualityRule, ...]

    @property
    def version(self) -> str:
        return self.api_version.split("/", maxsplit=1)[-1]


def _required(mapping: dict[str, Any], key: str, context: str) -> Any:
    if key not in mapping:
        raise ValueError(f"{context} is missing required property: {key}")
    return mapping[key]


def _parse_rule(raw: dict[str, Any]) -> QualityRule:
    name = str(_required(raw, "name", "quality rule"))
    rule_type = str(_required(raw, "type", f"quality rule {name}"))
    severity = str(_required(raw, "severity", f"quality rule {name}"))
    action = str(_required(raw, "action", f"quality rule {name}"))
    if rule_type not in RULE_TYPES:
        raise ValueError(f"Unsupported quality rule type {rule_type!r} for {name}")
    if severity not in SEVERITIES:
        raise ValueError(f"Unsupported severity {severity!r} for {name}")
    if action not in ACTIONS:
        raise ValueError(f"Unsupported action {action!r} for {name}")
    if severity == "warning" and action != "warn":
        raise ValueError(f"Warning rule {name} must use action=warn")
    if severity == "error" and action == "warn":
        raise ValueError(f"Error rule {name} cannot use action=warn")
    if rule_type in DATASET_RULE_TYPES and action == "quarantine":
        raise ValueError(f"Dataset rule {name} cannot use action=quarantine")
    column = raw.get("column")
    if rule_type != "row_count" and not column:
        raise ValueError(f"Quality rule {name} requires a column")
    if rule_type == "accepted_values" and not raw.get("values"):
        raise ValueError(f"accepted_values rule {name} requires values")
    if rule_type == "range" and "min" not in raw and "max" not in raw:
        raise ValueError(f"range rule {name} requires min and/or max")
    if rule_type == "regex" and not raw.get("pattern"):
        raise ValueError(f"regex rule {name} requires pattern")
    if rule_type == "freshness" and not raw.get("maxAge"):
        raise ValueError(f"freshness rule {name} requires maxAge")
    if rule_type == "row_count" and "min" not in raw and "max" not in raw:
        raise ValueError(f"row_count rule {name} requires min and/or max")
    return QualityRule(
        name=name,
        type=rule_type,
        severity=severity,
        action=action,
        column=str(column) if column else None,
        values=tuple(raw.get("values", ())),
        minimum=float(raw["min"]) if "min" in raw else None,
        maximum=float(raw["max"]) if "max" in raw else None,
        pattern=str(raw["pattern"]) if "pattern" in raw else None,
        max_age=str(raw["maxAge"]) if "maxAge" in raw else None,
    )


def load_contract(path: str | Path) -> DataContract:
    contract_path = Path(path)
    raw = yaml.safe_load(contract_path.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"Data contract must be an object: {contract_path}")
    api_version = str(_required(raw, "apiVersion", "data contract"))
    if api_version != SUPPORTED_API_VERSION:
        raise ValueError(f"Unsupported data contract version {api_version!r}; expected {SUPPORTED_API_VERSION!r}")
    if raw.get("kind") != "DataContract":
        raise ValueError("Data contract kind must be DataContract")
    metadata = _required(raw, "metadata", "data contract")
    spec = _required(raw, "spec", "data contract")
    fields = tuple(_required(spec, "schema", "data contract spec"))
    rules = tuple(_parse_rule(rule) for rule in _required(spec, "quality", "data contract spec"))
    field_names = [str(field["name"]) for field in fields]
    if len(field_names) != len(set(field_names)):
        raise ValueError("Data contract contains duplicate schema field names")
    rule_names = [rule.name for rule in rules]
    if len(rule_names) != len(set(rule_names)):
        raise ValueError("Data contract contains duplicate quality rule names")
    unknown_columns = sorted({rule.column for rule in rules if rule.column is not None and rule.column not in field_names})
    if unknown_columns:
        raise ValueError(f"Quality rules reference unknown contract columns: {unknown_columns}")
    return DataContract(
        api_version=api_version,
        name=str(_required(metadata, "name", "data contract metadata")),
        owner=str(_required(metadata, "owner", "data contract metadata")),
        fields=fields,
        quality_rules=rules,
    )


def assert_required_columns(actual_columns: list[str], contract: DataContract) -> None:
    expected = {str(field["name"]) for field in contract.fields}
    missing = sorted(expected - set(actual_columns))
    if missing:
        raise ValueError(f"Source does not satisfy contract; missing columns: {missing}")
