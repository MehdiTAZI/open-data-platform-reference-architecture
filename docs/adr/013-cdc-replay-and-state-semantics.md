# ADR 013 — CDC replay and state semantics

- Status: Accepted
- Date: 2026-08-30

## Context

The CDC golden path consumes PostgreSQL changes through Debezium and Kafka, then materializes current business state in Iceberg with Spark Structured Streaming.

Kafka and Spark provide offsets and checkpoints, but a reference implementation must remain correct when a Spark driver is recreated, a local checkpoint is lost, or a failure happens between independent Iceberg commits. Using Bronze presence as proof that an event has been fully processed is unsafe: Bronze may commit successfully while Silver, Gold or quality publication fails afterwards.

The pipeline also needs to preserve raw CDC history while exposing deterministic current state and explicit delete behavior.

## Decision

Use three separate concepts:

1. **Immutable CDC event identity** — every Kafka record is identified by SHA-256 of `(topic, partition, offset)` and stored as `_event_id`.
2. **Bronze audit state** — `polaris.bronze.orders_cdc_events` stores each CDC event once. Bronze proves that an event was captured, not that downstream publication completed.
3. **Processing commit state** — `polaris.platform.cdc_processed_events` records an event only after trusted Silver state, contract checks and Gold publication complete successfully.

For each available micro-batch:

```text
Kafka records
    |
    +--> anti-join Bronze event IDs --> append new Bronze audit events
    |
    +--> anti-join processed event IDs --> pending events
                                         |
                                         +--> latest change / business key
                                         +--> contract-driven DQ
                                         +--> quarantine invalid upserts
                                         +--> Iceberg MERGE upserts
                                         +--> Iceberg DELETEs
                                         +--> validate complete Silver state
                                         +--> rebuild Gold
                                         `--> append processing commit markers
```

The Spark checkpoint is an optimization. Correctness is additionally protected by durable Iceberg event identity and processing-commit state.

## Operation semantics

Debezium operations are interpreted as follows:

- `r` — initial snapshot row; treated as an upsert.
- `c` — create; treated as an upsert.
- `u` — update; treated as an upsert.
- `d` — delete; deletes the matching business key from Silver.

Within pending events for a business key, source LSN is the primary ordering signal, with Kafka partition/offset used as deterministic tie-breakers.

Gold is rebuilt from the complete trusted Silver state, not incrementally patched. This keeps the reference implementation deterministic and easy to validate; incremental aggregate maintenance is a future optimization.

## Failure semantics

A failure before the processing marker is written leaves the event pending. On replay:

- Bronze is not duplicated because `_event_id` is already present.
- Silver upserts and deletes are safe to reapply through deterministic Iceberg mutations.
- quarantine insertion is deduplicated by `_event_id`.
- Gold is deterministically rebuilt.
- the processing marker is written only after successful publication.

This deliberately provides **effectively-once materialization semantics** on top of replayable at-least-once transport rather than claiming distributed exactly-once transactions across Kafka and multiple Iceberg tables.

## Data quality

CDC reuses the platform `odp_data_quality` package and the same `DataContract` as the batch golden path.

Two quality scopes are evaluated:

- `ingress.*` — row-level rules applied to candidate upserts.
- `state.*` — the complete resulting Silver state, including dataset-level assertions such as row-count and freshness.

`quarantine`, `warn` and `fail` retain the semantics defined in ADR 012.

## Consequences

### Positive

- Bronze remains immutable and auditable.
- partial downstream failures do not turn captured events into lost business changes.
- replay does not depend solely on an ephemeral Spark checkpoint.
- batch and CDC share one Data Contract and quality engine.
- deletes are explicit and testable.
- transport semantics and materialization semantics are clearly separated.

### Trade-offs

- an additional Iceberg processing-state table is required.
- every replay performs durable anti-joins against Bronze and processing markers.
- only one writer per CDC state table is assumed in this golden path; concurrent writers require stronger coordination.
- intermediate changes for the same key within one available batch are collapsed to the latest state for materialization, while all raw events remain in Bronze.

## Production evolution

A production profile should additionally define:

- durable remote Spark checkpoints;
- Kafka authentication, TLS and ACLs;
- Debezium offset/config/status topic durability and replication;
- PostgreSQL replication-slot monitoring and WAL retention alerts;
- schema-change compatibility policy;
- poison-event handling and dead-letter operational workflow;
- controlled concurrent-writer semantics;
- incremental Gold maintenance where justified by scale;
- Iceberg compaction, snapshot expiration and orphan-file cleanup;
- OpenLineage transport to a lineage backend rather than log-only emission.
