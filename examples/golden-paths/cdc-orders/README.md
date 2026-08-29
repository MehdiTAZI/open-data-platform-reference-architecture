# Golden Path 02 — PostgreSQL CDC to Iceberg

This example demonstrates log-based CDC from PostgreSQL into an Iceberg current-state model while preserving transport metadata and honest delivery semantics.

```text
PostgreSQL WAL (pgoutput)
        |
        v
Debezium PostgreSQL Connector
        |
        v
Kafka topic: odp.source.orders
        |
        v
Spark Structured Streaming
        |
        +--> Bronze: polaris.bronze.orders_cdc_events
        +--> Silver: polaris.silver.orders_cdc
        +--> Gold:   polaris.gold.daily_order_summary_cdc
        |
        v
Polaris / Iceberg / S3-compatible storage
        |
        v
      Trino
```

## Delivery semantics

The reference model is deliberately **at-least-once**, not falsely labelled exactly-once. Debezium and Kafka Connect persist source offsets, Spark Structured Streaming persists its checkpoint, and the Iceberg application makes replay idempotent:

- Bronze is deduplicated by Kafka `(partition, offset)`.
- Silver is materialized with an Iceberg `MERGE` keyed by `order_id` and ordered by PostgreSQL source LSN/Kafka offset.
- Deletes are applied only when the event is not older than the materialized row.
- Gold is deterministically rebuilt from current Silver state for this small reference dataset.

The raw Debezium envelope is retained in Bronze for audit/replay.

## Run

```bash
make local-up
make cdc-golden-path-test
```

The acceptance test validates initial snapshot, insert/update/delete processing, Trino consumption, a streaming-driver restart using the same checkpoint, a post-restart mutation, and restoration to the canonical six-row source dataset.

## PostgreSQL security

The connector uses a dedicated `debezium` role with `LOGIN REPLICATION` plus SELECT/USAGE on the captured source. It does not reuse the platform/admin credential. The publication is explicitly created for `source.orders`; `publication.autocreate.mode` is disabled.

## WAL / slot operations

A replication slot retains WAL required by the connector. Production must monitor slot lag and retained bytes, alert before disk pressure, define connector outage limits, and document slot recreation/re-snapshot procedures. The standalone profile caps retained slot WAL to keep a workstation failure bounded; this is not a universal production setting.

## Production evolution

- hardened/vendor-supported Kafka Connect runtime instead of Debezium's evaluation container image;
- TLS/SASL and workload identity for Kafka;
- HA Kafka Connect workers and production internal-topic replication;
- durable external Spark checkpoint storage;
- schema registry/contract compatibility where event evolution requires it;
- dedicated replication credentials from an external secret manager;
- WAL/slot lag dashboards and alerts;
- DLQ/quarantine policy for malformed events;
- table maintenance/compaction for CDC write patterns;
- explicit SLOs for end-to-end change latency.
