# ADR-011 — Enforce Zone Quality Gates and Cross-Zone Reconciliation

- Status: Accepted
- Date: 2026-08-29

## Context

A medallion architecture is not just three schema names. Each zone needs a distinct responsibility, promotion rules, and evidence that data did not silently drift while moving between zones.

CDC also has failure modes that basic row-count smoke tests miss: duplicate transport events, malformed envelopes, stale state transitions, invalid business values, duplicate business keys, incorrect aggregates, and replay after restart.

## Decision

Every executable golden pipeline MUST define and test explicit contracts at each promotion boundary.

### Bronze — source and replay truth

Bronze preserves the source event and transport metadata with minimal interpretation. For CDC, the raw key/value, operation, source LSN, Kafka partition and Kafka offset are retained.

Assertions cover valid parseable events, supported CDC operations, non-null transport identity and business key, non-negative Kafka partition/offset, uniqueness of `(kafka_partition, kafka_offset)`, and retention of delete/replay metadata. Business cleansing does not belong in Bronze.

### Silver — conformed business state

Silver owns typed, normalized, deduplicated business entities. CDC deletes are applied as state transitions, so a current-state Silver table contains only active rows.

Assertions cover unique business keys, required fields, accepted business domains, non-negative monetary values, normalized country codes, and retained ordering metadata. Invalid non-delete rows fail before they become accepted Silver state.

### Gold — consumption contract

Gold is built only from validated Silver data and declares a stable business grain. The orders reference pipeline uses `(order_date, country)`.

Assertions cover unique business grain, non-negative measures, `completed_order_count <= order_count`, `completed_amount <= gross_amount`, and exact reconciliation back to Silver at the declared grain.

### Two layers of enforcement

1. **Write-path assertions** fail fast inside Spark before invalid data is promoted.
2. **Acceptance assertions** query persisted Iceberg through Trino and reconcile PostgreSQL -> Silver and Silver -> Gold.

Using a second query engine makes the acceptance proof cover the catalog, storage format and serving path rather than only Spark's in-process view.

### Failure handling

The reference pipeline is fail-fast for contract violations. A production implementation may add quarantine/DLQ handling, but it MUST preserve the same promotion rule: failed records are not silently promoted into trusted zones.

A quarantine path is not added merely to make the demo appear feature-complete; it needs explicit ownership, retention, replay and remediation runbooks.

## Consequences

### Positive

- zone names encode real trust boundaries;
- replay/idempotence defects are detectable;
- source/current-state drift is detectable;
- aggregate drift is detected at business grain;
- golden paths demonstrate production-style data quality rather than connectivity only.

### Trade-offs

- whole-table assertions are acceptable for the small standalone reference dataset but must be incremental or framework-backed at production scale;
- fail-fast behavior can stop freshness when upstream data is invalid, so production needs alerting and an explicit remediation/quarantine operating model;
- contracts and tests must evolve together when schemas or business domains change.
