# Golden Path 01 — Batch Orders

This golden path is the reference implementation for a production-shaped bounded batch pipeline. It demonstrates explicit Medallion responsibilities, auditable replay, data-quality quarantine, reconciliation and independent serving through Trino.

```text
PostgreSQL source.orders
        |
        | JDBC bounded snapshot
        v
     Spark 4.1.3
        |
        +--> Bronze:     polaris.bronze.orders_raw          append-only run history
        |
        +--> Silver DQ classification
        |        |--> valid      -> polaris.silver.orders
        |        `--> invalid    -> polaris.quarantine.orders
        |
        +--> Gold:       polaris.gold.daily_order_summary
        |
        `--> Operations: polaris.platform.pipeline_runs
        |
        v
Apache Iceberg / Apache Polaris / S3-compatible storage
        |
        v
      Trino
```

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

Silver canonicalizes data types and representation, then applies data-quality classification. Valid rows are published to `polaris.silver.orders`. Invalid rows are separated from the trusted dataset and appended to `polaris.quarantine.orders` with `_dq_errors` and `_quarantined_at`.

Current quality rules cover:

- required business keys and fields;
- unique `order_id` within the snapshot;
- accepted order statuses;
- non-negative amount;
- two-character country-code shape.

### Gold — business-facing

Gold contains business aggregates only. `daily_order_summary` is derived exclusively from trusted Silver rows. Publication is reconciled so the number of orders represented in Gold must equal the number of trusted Silver rows.

## Replay and idempotency

This pattern distinguishes **audit idempotency** from **business-state idempotency**:

- Bronze intentionally records every run, including replay of an identical snapshot.
- Silver is replaced with the trusted state of the current bounded snapshot.
- Gold is deterministically rebuilt from Silver.
- `pipeline_runs` records successful runs and row-level reconciliation metrics.

The end-to-end test runs the same snapshot twice and verifies that Bronze grows from 6 to 12 rows while Silver and Gold remain at the same six business orders.

## Code structure

```text
src/
├── batch_orders.py              # thin executable entry point
└── orders_pipeline/
    ├── config.py                # runtime configuration
    ├── context.py               # PipelineRun metadata
    ├── spark.py                 # Spark/Iceberg session construction
    ├── storage.py               # Iceberg publishing primitives
    ├── quality.py               # DQ classification and quarantine
    ├── main.py                  # pipeline composition
    └── layers/
        ├── bronze.py
        ├── silver.py
        └── gold.py

tests/
├── test_layers.py
└── test_quality.py
```

Orchestration remains deliberately thin: Airflow starts the Kubernetes job and contains no business transformation logic.

## Test pyramid

The application now has two validation levels:

1. **Unit/data tests** execute Spark locally inside the compatibility-locked application image and validate Medallion transformations plus DQ behavior.
2. **End-to-end tests** execute the job on the standalone Kubernetes stack and validate Spark -> Iceberg/Polaris -> Trino, replay and reconciliation.

Run the complete local golden path with:

```bash
make local-up
make batch-golden-path-test
```

## Production evolution

The next iterations should add:

- contract-driven generation of quality expectations;
- configurable fail/warn/quarantine actions by rule severity;
- source transaction/snapshot isolation semantics;
- incremental extraction/high-watermark variant;
- OpenLineage emission and pipeline metrics;
- backfill parameters and explicit logical dates;
- Iceberg partition/file-maintenance policy;
- failure-run persistence in the operational metadata table;
- immutable application-image digest promotion.
