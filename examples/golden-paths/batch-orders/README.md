# Golden Path 01 — Batch Orders

This golden path is the reference implementation for a production-shaped bounded batch pipeline. It demonstrates explicit Medallion responsibilities, auditable replay, executable data contracts, data-quality quarantine, reconciliation, operational metrics, OpenLineage events and independent serving through Trino.

```text
PostgreSQL source.orders
        |
        | JDBC bounded snapshot
        v
     Spark 4.1.3
        |
        +--> Bronze:     polaris.bronze.orders_raw          append-only run history
        |
        +--> Contract-driven DQ classification
        |        |--> valid      -> polaris.silver.orders
        |        `--> invalid    -> polaris.quarantine.orders
        |
        +--> Gold:       polaris.gold.daily_order_summary
        |
        +--> Operations: polaris.platform.pipeline_runs
        |                polaris.platform.data_quality_results
        |
        `--> OpenLineage START / COMPLETE / FAIL events
        |
        v
Apache Iceberg / Apache Polaris / S3-compatible storage
        |
        v
      Trino
```

## Executable data contract

The application image packages `specs/data-contracts/examples/batch-orders.yaml`. The contract is therefore versioned and promoted together with the pipeline image rather than being an out-of-band document.

The `odp/v1alpha2` quality DSL supports:

- `not_null`
- `not_blank`
- `unique`
- `accepted_values`
- `range`
- `regex`
- `freshness`
- `row_count`

Every rule declares both **severity** and **action**:

| Action | Semantics |
|---|---|
| `quarantine` | Reject only violating rows from trusted Silver and preserve them with `_dq_errors`. |
| `fail` | Fail the complete run when a dataset-level or critical rule is violated. |
| `warn` | Record the violation but continue publishing the row/dataset. |

The runtime validates rule names, referenced columns, action/severity compatibility and rule-specific required properties before processing data.

## Layer semantics

### Bronze — source aligned and auditable

Bronze preserves the source business fields and adds only operational metadata:

- `_run_id`
- `_pipeline`
- `_contract_version`
- `_source_system`
- `_ingested_at`
- `_record_hash`

Bronze is append-only for this pattern. Replaying an identical source snapshot therefore creates another auditable ingestion run instead of overwriting history.

### Silver — canonical and trusted

Silver canonicalizes data types and representation, then evaluates the executable contract. Valid rows are published to `polaris.silver.orders`. Invalid rows are separated from the trusted dataset and appended to `polaris.quarantine.orders` with `_dq_errors`, optional `_dq_warnings` and `_quarantined_at`.

The current contract validates required identifiers/timestamps, non-blank customer IDs, accepted order statuses, non-negative amounts, ISO2 country-code shape, unique business keys, non-empty snapshots and source freshness.

### Gold — business-facing

Gold contains business aggregates only. `daily_order_summary` is derived exclusively from trusted Silver rows. Publication is reconciled so the number of orders represented in Gold must equal the number of trusted Silver rows.

## Data-quality observability

Each rule produces one record per run in:

```text
polaris.platform.data_quality_results
```

with:

```text
run_id
pipeline
contract_version
rule_name
rule_type
severity
action
evaluated_rows
violation_count
status              # PASS | WARN | QUARANTINE | FAIL
recorded_at
```

This makes quality results queryable independently of Spark logs and provides a stable input for future Grafana/SLO dashboards.

## OpenLineage baseline

The job emits OpenLineage RunEvents to stdout using the same `run_id` as the operational and quality tables:

```text
START
  -> processing / DQ / publication
COMPLETE
```

Failures emit `FAIL`. The event declares PostgreSQL as the input dataset and Bronze, Silver, Quarantine and Gold as output datasets. Stdout is the standalone transport; a production profile can route the same events to an OpenLineage-compatible backend/collector without changing the dataset model.

## Replay and idempotency

This pattern distinguishes **audit idempotency** from **business-state idempotency**:

- Bronze intentionally records every run, including replay of an identical snapshot.
- Silver is replaced with the trusted state of the current bounded snapshot.
- Gold is deterministically rebuilt from Silver.
- `pipeline_runs` records successful and failed executions.
- `data_quality_results` records the rule-level outcome for every evaluated run.

The end-to-end test runs the same snapshot twice and verifies that Bronze grows from 6 to 12 rows while Silver and Gold remain at the same six business orders. It also validates DQ metric persistence and OpenLineage `START/COMPLETE` events.

## Code structure

```text
src/
├── batch_orders.py              # thin executable entry point
└── orders_pipeline/
    ├── config.py                # runtime configuration
    ├── context.py               # PipelineRun metadata
    ├── contracts.py             # YAML contract loading and semantic validation
    ├── spark.py                 # Spark/Iceberg session construction
    ├── storage.py               # Iceberg publishing primitives
    ├── quality.py               # contract compiler, actions and rule metrics
    ├── observability.py         # run metrics + OpenLineage events
    ├── main.py                  # pipeline composition
    └── layers/
        ├── bronze.py
        ├── silver.py
        └── gold.py

tests/
├── test_contracts.py
├── test_layers.py
├── test_quality.py
└── run_tests.py
```

Orchestration remains deliberately thin: Airflow starts the Kubernetes job and contains no business transformation logic.

## Test pyramid

The application has three validation levels:

1. **Contract validation** checks YAML instances against the DataContract JSON Schema in CI and validates semantic constraints in the application loader.
2. **Unit/data tests** execute Spark locally inside the compatibility-locked application image and validate Medallion transformations, `fail/warn/quarantine` behavior and rule metrics.
3. **End-to-end tests** execute the job on the standalone Kubernetes stack and validate Spark -> Iceberg/Polaris -> Trino, replay, reconciliation, DQ persistence and lineage events.

Run the complete local golden path with:

```bash
make local-up
make batch-golden-path-test
```

## Production evolution

The next iterations should add:

- source transaction/snapshot isolation semantics;
- incremental extraction/high-watermark variant;
- OpenLineage HTTP transport and dedicated lineage backend;
- Prometheus/OpenTelemetry export of pipeline and DQ metrics;
- backfill parameters and explicit logical dates;
- Iceberg partition/file-maintenance policy;
- immutable application-image digest promotion;
- CDC and streaming golden paths that reuse the same contract/DQ semantics.
