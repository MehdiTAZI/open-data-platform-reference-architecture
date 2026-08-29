# Golden Path 02 — PostgreSQL CDC to Iceberg

This example demonstrates log-based CDC from PostgreSQL into an Iceberg current-state model with explicit medallion-zone responsibilities, data quality gates, transport lineage and honest delivery semantics.

```text
PostgreSQL WAL -> Debezium -> Kafka -> Spark Structured Streaming
                                         |
                                         +--> Bronze: polaris.bronze.orders_cdc_events
                                         |      raw event + source/transport lineage
                                         +--> Silver: polaris.silver.orders_cdc
                                         |      typed, normalized current business state
                                         +--> Gold: polaris.gold.daily_order_summary_cdc
                                                grain: (order_date, country)
                                                        |
                                                        v
                                             Polaris / Iceberg / Garage -> Trino
```

## Zone contracts

| Zone | Responsibility | Promotion / assertions |
|---|---|---|
| Bronze | Source/replay truth. Keep raw Debezium payload plus Kafka/PostgreSQL lineage. | Parseable event, supported `c/u/d/r`, business key, valid transport identity, unique `(partition, offset)`. No business cleansing. |
| Silver | Trusted current-state orders. Typed/normalized attributes with CDC deletes applied. | Unique `order_id`, required fields, status domain, non-negative amount, two-letter country code, retained ordering lineage. |
| Gold | Consumption-ready daily country summary derived only from Silver. | Unique `(order_date, country)`, non-negative measures, completed metrics bounded by totals, exact Silver-to-Gold reconciliation. |

The Spark write path fails fast before invalid records are promoted. The E2E acceptance test independently queries persisted Iceberg through Trino and performs PostgreSQL -> Silver and Silver -> Gold reconciliation. See `docs/adr/011-zone-quality-gates-and-reconciliation.md`.

## Delivery semantics

The reference model is deliberately **at-least-once**, not falsely labelled exactly-once. Debezium/Kafka Connect persist source offsets, Spark persists its checkpoint, and the application makes replay idempotent:

- Bronze is deduplicated by Kafka `(partition, offset)`.
- Silver uses Iceberg `MERGE` keyed by `order_id`, ordered by PostgreSQL source LSN/Kafka offset.
- Deletes are applied only when the event is newer than materialized state.
- Exact replay is a physical no-op on current state.
- Gold is deterministically rebuilt from validated Silver for this small reference dataset.

## Run

```bash
make local-up
make cdc-golden-path-test
```

The acceptance test validates initial snapshot; Bronze lineage/uniqueness; Silver keys, domains and nullability; Gold grain/invariants; PostgreSQL -> Silver full-row reconciliation; Silver -> Gold grain-level reconciliation; insert/update/delete; retained delete evidence; streaming restart using the same checkpoint; post-restart changes; replay safety; and restoration to the canonical source dataset.

## PostgreSQL security

The connector uses a dedicated `debezium` role with `LOGIN REPLICATION` plus SELECT/USAGE on the captured source. It does not reuse the platform/admin credential. The publication is explicitly created for `source.orders`; `publication.autocreate.mode` is disabled.

## WAL / slot operations

A replication slot retains WAL required by the connector. Production must monitor slot lag and retained bytes, alert before disk pressure, define connector outage limits, and document slot recreation/re-snapshot procedures. The standalone profile caps retained slot WAL to keep a workstation failure bounded; this is not a universal production setting.

## Production evolution

- hardened/vendor-supported Kafka Connect runtime instead of Debezium's evaluation image;
- TLS/SASL and workload identity for Kafka;
- HA Kafka Connect workers and production internal-topic replication;
- durable external Spark checkpoint storage;
- schema registry/contract compatibility where event evolution requires it;
- dedicated replication credentials from an external secret manager;
- WAL/slot lag dashboards and alerts;
- explicit quarantine/DLQ ownership, retention, replay and remediation runbooks;
- scalable incremental data-quality execution while preserving the same zone contracts;
- Iceberg maintenance/compaction for CDC write patterns;
- explicit SLOs for end-to-end change latency.
