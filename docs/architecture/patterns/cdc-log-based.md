# Pattern — Log-based CDC to an Idempotent Lakehouse Materialization

## Intent

Capture database changes from a transaction log without repeatedly scanning source tables, transport them durably, and materialize trusted queryable state while remaining safe under replay.

## Reference flow

```text
PostgreSQL WAL -> Debezium -> Kafka -> Spark Structured Streaming -> Iceberg
                                         |                    |
                                      checkpoint        Bronze -> Silver -> Gold
```

## Delivery model

The platform assumes **at-least-once delivery** across failure boundaries. Exactly-once is not claimed across PostgreSQL, Kafka, Spark and Iceberg as one distributed transaction.

Correctness comes from layered recovery contracts: PostgreSQL replication slots track WAL consumption; Kafka Connect persists connector offsets; Kafka retains ordered per-key events; Spark persists offsets/state in a durable checkpoint; Bronze retains raw event/source/transport identity; Silver applies idempotent state transitions; Gold derives only from validated Silver and is reconciled to its declared grain.

## Medallion responsibilities

### Bronze

Bronze is the source/replay ledger, not a cleansed business table. Preserve enough raw source and transport metadata to audit, diagnose and replay. Reject malformed or unidentifiable events before acceptance; do not silently repair business fields here.

### Silver

Silver is the trusted business-state boundary. Parse/type values, normalize controlled fields, apply deletes, deduplicate by business key, and retain ordering lineage. A current-state table rejects stale transitions and exact transport replay. Domain/nullability assertions execute before promotion and persisted-table assertions verify key uniqueness.

### Gold

Gold declares a consumption grain and derives exclusively from trusted Silver. Aggregates have business invariants and are reconciled to Silver at that grain. Gold must not reach back to Bronze and bypass Silver quality rules.

## Ordering and deletes

The source LSN is the primary source-order marker. Kafka offset is retained as transport-order marker and replay identity. `-1` is an explicit standalone fallback if an initial snapshot does not expose an LSN; normal streaming records use PostgreSQL LSN ordering.

Deletes are first-class events: Bronze retains delete evidence; current-state Silver removes the business row only for a newer delete; Gold reflects the resulting Silver state.

## Quality and reconciliation

Quality is enforced at two levels:

- write-path assertions stop malformed or invalid data before promotion;
- E2E acceptance assertions query persisted Iceberg via Trino and verify Bronze uniqueness, Silver contracts, Gold invariants, PostgreSQL -> Silver equality, and Silver -> Gold grain equality.

This prevents Spark-only tests from masking catalog, storage or serving-engine defects. See ADR-011.

## Recovery

A consumer restart resumes from its Structured Streaming checkpoint. Duplicate delivery is safe because Bronze ignores an already-seen Kafka partition/offset and Silver ignores older/equal transitions. If the checkpoint is intentionally discarded, replay from retained Kafka remains possible subject to retention.

## Operational requirements

Production must monitor replication slot lag, retained WAL bytes, connector task state, Kafka consumer lag, checkpoint health, streaming batch duration, Iceberg commit failures, data-quality failures and end-to-end freshness. Replication-slot outage/runbooks must define disk-pressure thresholds, retention constraints and re-snapshot decisions.

At production scale, whole-table assertions from the standalone reference should become incremental or be delegated to a governed data-quality framework. The zone contracts and reconciliation semantics remain mandatory.
