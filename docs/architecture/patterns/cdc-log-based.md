# Pattern — Log-based CDC to an Idempotent Lakehouse Materialization

## Intent

Capture database changes from a transaction log without repeatedly scanning source tables, transport them durably, and materialize a queryable current state while remaining safe under replay.

## Reference flow

```text
PostgreSQL WAL -> Debezium -> Kafka -> Spark Structured Streaming -> Iceberg
                                         |                    |
                                      checkpoint            MERGE
```

## Delivery model

The platform assumes **at-least-once delivery** across failure boundaries. Exactly-once is not claimed across PostgreSQL, Kafka, Spark and Iceberg as one distributed transaction.

Correctness comes from layered recovery contracts:

1. PostgreSQL replication slot tracks WAL consumption.
2. Kafka Connect persists connector offsets.
3. Kafka retains ordered per-key events.
4. Spark persists streaming offsets/state in a durable checkpoint.
5. Bronze stores the source LSN plus Kafka partition/offset.
6. Silver applies idempotent state transitions with Iceberg MERGE.

## Ordering

The source LSN is the primary source-order marker. Kafka offset is retained as a transport-order marker and replay identity. Consumers must not discard these fields before a deterministic materialization boundary exists.

## Deletes

Deletes are first-class events. Current-state tables remove the business row only when the delete event is at least as new as the row already materialized.

## Recovery

A consumer restart resumes from its Structured Streaming checkpoint. Duplicate delivery is safe because Bronze ignores an already-seen Kafka partition/offset and Silver ignores older state transitions. If the checkpoint is intentionally discarded, replay from retained Kafka data remains possible subject to retention.

## Operational requirements

Production must monitor replication slot lag, retained WAL bytes, connector task state, Kafka consumer lag, checkpoint health, streaming batch duration, Iceberg commit failures and end-to-end freshness.

Replication slots can retain unbounded WAL if left unmanaged. Outage/runbook design must therefore include disk-pressure thresholds, retention constraints and re-snapshot decisions.
