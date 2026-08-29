# Golden Path 01 — Batch Orders

This example is the first executable application built on the reference platform. It demonstrates a deterministic relational batch snapshot flowing through a Medallion-style Iceberg lakehouse and being consumed independently by Trino.

```text
PostgreSQL source.orders
        |
        | JDBC snapshot
        v
     Spark 4.1.3
        |
        +--> Bronze: polaris.bronze.orders_snapshot
        |
        +--> Silver: polaris.silver.orders
        |
        +--> Gold:   polaris.gold.daily_order_summary
        |
        v
Apache Iceberg 1.11 / Apache Polaris / S3-compatible storage
        |
        v
      Trino 483
```

## Run

Start the standalone platform first:

```bash
make local-up
make batch-golden-path-test
```

The test deliberately runs the pipeline twice and checks that the published business result is unchanged. That validates replay/idempotency for this snapshot pattern.

## Pattern semantics

This is a **bounded snapshot batch** pattern. The source table is read as one logical snapshot and the target tables are atomically replaced table-by-table through Iceberg. It is suitable for small/medium reference datasets, periodic dimensions and bounded extracts where complete replay is acceptable.

It is intentionally not presented as the answer for high-volume change capture. The CDC golden path will own log-based incremental processing through Debezium and Kafka.

## Quality gates

The job fails before publish when:

- the source snapshot is empty;
- a required field is missing or null;
- `order_id` is duplicated;
- an amount is negative.

The source contract is versioned under `specs/data-contracts/examples/batch-orders.yaml`.

## Idempotency and replay

`createOrReplace()` provides an atomic Iceberg commit for each table. Re-running the same bounded source snapshot therefore produces the same business state without duplicate rows. Operational fields such as `_ingested_at` are expected to change on replay.

This example does not claim a cross-table distributed transaction across Bronze, Silver and Gold. A failure after one table commit is recovered by replaying the complete deterministic job.

## Production evolution

A production implementation should additionally decide, based on workload profile:

- consistent source snapshot / transaction isolation;
- JDBC partitioning and bounded parallelism;
- incremental high-watermark extraction where CDC is not available;
- dedicated source credentials and workload identity;
- source-side read replicas where appropriate;
- data quality quarantine and reconciliation policy;
- partition evolution and file-size tuning;
- SLO/alerting and OpenLineage emission;
- orchestrated retries and backfill parameters;
- immutable application image digest promotion.

Airflow orchestration is added as a thin control-plane layer in the next V0.3 iteration; business logic stays in this application image.
