# ADR-010: At-least-once CDC with idempotent Iceberg materialization

- Status: Accepted
- Date: 2026-08-29
- Owners: Platform Architecture

## Context

The CDC path crosses PostgreSQL WAL, Debezium/Kafka Connect, Kafka, Spark Structured Streaming and Iceberg. These systems do not participate in one distributed transaction. Recovery of a source or consumer can legitimately cause events to be observed again.

## Decision

Model the CDC reference architecture as **at-least-once delivery with idempotent materialization**.

The application retains PostgreSQL source LSN and Kafka partition/offset. Bronze deduplicates transport events on `(partition, offset)`. Silver uses `order_id` plus monotonic source/transport metadata to reject stale transitions and applies current state through Iceberg MERGE. Spark Structured Streaming persists offsets in a durable checkpoint.

## Consequences

- Duplicate delivery is expected and tested rather than hidden.
- A checkpoint restart must not change business state.
- Transport metadata remains available for reconciliation and replay.
- Production Kafka retention must cover realistic recovery windows.
- A broken/lost checkpoint can be rebuilt by replay only while required Kafka data still exists.
- The architecture does not market an end-to-end exactly-once guarantee it cannot prove.
