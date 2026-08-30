# ADR 012 — Contract-Driven Data Quality

- Status: Accepted
- Date: 2026-08-30

## Context

Data quality controls that are embedded directly in application code are difficult to govern, review, reuse and evolve independently from transformation logic. The platform already treats data contracts as versioned architecture artifacts, so quality expectations should be executable from the same contract rather than duplicated in Spark code.

## Decision

Data contracts use `odp/v1alpha2` and define quality rules as structured objects with a stable rule name, rule type, severity and action.

Supported reference rule types are:

- row-level: `not_null`, `not_blank`, `unique`, `accepted_values`, `range`, `regex`;
- dataset-level: `freshness`, `row_count`.

Actions have explicit runtime semantics:

- `quarantine`: violating rows are excluded from trusted Silver and retained in Quarantine with the violated rule names;
- `fail`: any violation fails the complete pipeline run;
- `warn`: violations are measured but do not block or quarantine the data.

Warning severity must use `warn`. Error severity may use `quarantine` or `fail`. Dataset-level rules cannot quarantine individual rows.

The application image packages the exact contract version it executes. CI validates contract documents against the JSON Schema, while the runtime loader applies additional semantic checks such as rule-specific required fields and references to known schema columns.

Every evaluated rule is persisted to `polaris.platform.data_quality_results` using the pipeline `run_id`. This table is an operational interface for observability and SLO reporting; application code must not depend on a dashboard product.

## Consequences

### Positive

- quality intent is reviewable as data, not hidden inside transformation code;
- the same contract can be compiled by future Spark, dbt or other adapters;
- rule outcomes are queryable and attributable to a pipeline run;
- fail/warn/quarantine behavior is explicit and testable;
- schema and quality evolution are versioned together.

### Trade-offs

- the contract DSL becomes a compatibility surface and must evolve deliberately;
- some rule types require dataset actions and therefore trigger Spark actions/aggregations;
- application images need a YAML parser or an equivalent build-time contract compiler;
- cross-dataset and semantic business rules may exceed the generic DSL and should remain application-specific until a reusable pattern emerges.

## Alternatives considered

### Hardcode expectations in Spark

Rejected as the primary pattern because governance metadata and runtime behavior diverge and rules become difficult to reuse.

### Adopt Great Expectations or Soda as the contract format

Not selected for the portable core. Those tools remain valid adapters, but the architectural contract should not be owned by one quality product.

### Use dbt tests for all quality rules

Not selected because Bronze/Silver processing, CDC and streaming pipelines require quality enforcement outside dbt execution boundaries.
