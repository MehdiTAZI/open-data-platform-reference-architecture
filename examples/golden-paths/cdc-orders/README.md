# Golden Path 02 — CDC Orders

This golden path demonstrates a replay-safe Change Data Capture pipeline from PostgreSQL into an Iceberg lakehouse. It complements the bounded batch reference with an event-driven path while reusing the same Data Contract, quality engine and Medallion responsibilities.

```text
PostgreSQL source.orders
        |
        | logical replication / pgoutput
        v
Debezium PostgreSQL Connector
        |
        v
Kafka: odp-commerce.source.orders
        |
        v
Spark Structured Streaming (availableNow)
        |
        +--> Bronze:     polaris.bronze.orders_cdc_events
        |                 immutable CDC event audit
        |
        +--> Contract DQ
        |        |--> trusted upserts
        |        `--> quarantine: polaris.quarantine.orders_cdc
        |
        +--> Silver:     polaris.silver.orders_cdc
        |                 current state via MERGE / DELETE
        |
        +--> Gold:       polaris.gold.daily_order_summary_cdc
        |
        +--> DQ results: polaris.platform.data_quality_results
        |
        +--> Run audit:  polaris.platform.pipeline_runs
        |
        `--> Commit log: polaris.platform.cdc_processed_events
```

## Source capture

The standalone PostgreSQL instance enables logical replication with bounded slot WAL retention. Debezium uses `pgoutput`, a filtered publication for `source.orders`, and a dedicated replication slot.

The connector publishes to:

```text
odp-commerce.source.orders
```

The initial Debezium snapshot produces `r` events; subsequent inserts, updates and deletes produce `c`, `u` and `d` operations.

The standalone connector configuration is registered idempotently by `scripts/register-cdc-connector.py` and waits until both the connector and task are `RUNNING`.

## Bronze — immutable CDC event audit

Each Kafka record receives a deterministic event identity:

```text
_event_id = SHA256(topic + partition + offset)
```

Bronze preserves the parsed Debezium event plus Kafka/source metadata, including:

- Kafka topic, partition and offset;
- Debezium operation;
- PostgreSQL source LSN and transaction ID;
- source timestamp;
- raw event value;
- pipeline run and contract metadata.

An event is appended to Bronze only if its `_event_id` is not already present.

Bronze answers **“was this event captured?”**. It does not answer **“was this event fully published downstream?”**.

## Durable processing commit state

`polaris.platform.cdc_processed_events` is intentionally separate from Bronze.

An event is marked processed only after:

1. row-level contract checks;
2. quarantine handling;
3. Silver MERGE/DELETE;
4. complete Silver-state validation;
5. Gold publication.

This protects replay across partial failures. If Bronze commits and a later step fails, the event remains absent from `cdc_processed_events` and is therefore retried.

## Silver — current trusted state

For each pending business key, the pipeline selects the latest change using:

1. PostgreSQL source LSN;
2. Kafka partition and offset as deterministic tie-breakers.

Operations are applied as:

| Debezium op | Silver action |
|---|---|
| `r` | upsert |
| `c` | upsert |
| `u` | upsert |
| `d` | delete |

Upserts are canonicalized before quality evaluation:

- typed `order_id`;
- normalized status and country;
- `decimal(18,2)` amount;
- canonical timestamp/date;
- source and event metadata.

Trusted upserts are materialized with Iceberg `MERGE`. Deletes use an explicit Iceberg matched delete.

## Contract-driven quality

The CDC path imports the shared platform package:

```text
platform/governance/data-quality/python/odp_data_quality
```

It executes the same `odp/v1alpha2` Data Contract used by the batch example.

Two scopes are persisted in `polaris.platform.data_quality_results`:

- `ingress.<rule>` for incoming upsert candidates;
- `state.<rule>` for the complete trusted Silver state after mutations.

The same actions are supported across batch and CDC:

- `quarantine` — isolate invalid data;
- `warn` — publish while recording the violation;
- `fail` — fail the run.

Quarantine writes are deduplicated by `_event_id`, making retries safe if a later publication step fails.

## Gold — deterministic business state

`polaris.gold.daily_order_summary_cdc` is rebuilt from the complete Silver current state after every successful micro-batch.

This is deliberately simple and deterministic for a reference architecture. At larger scale, incremental aggregate maintenance can replace full rebuilding after its correctness and late-event semantics are explicitly defined.

## Checkpoint and replay semantics

The Spark query uses Structured Streaming with `availableNow` and a checkpoint. The standalone checkpoint is intentionally ephemeral.

Correctness does **not** depend only on that checkpoint:

```text
Kafka replay from earliest
        |
        +--> Bronze event IDs prevent duplicate audit rows
        |
        +--> processed-event IDs select unfinished work
        |
        +--> idempotent MERGE / DELETE converges Silver
        |
        `--> deterministic Gold rebuild converges aggregates
```

This is best described as **effectively-once materialization over replayable at-least-once transport**, not a distributed exactly-once transaction across Kafka and all Iceberg tables.

See ADR 013 for the decision and failure model.

## Observability

Every execution uses one run ID across:

- `pipeline_runs`;
- rule-level DQ results;
- processing commit markers;
- OpenLineage `START`, `COMPLETE` or `FAIL` events.

The current reference emits OpenLineage events to application logs. A production deployment should send them to an OpenLineage-compatible backend.

## Code structure

```text
examples/golden-paths/cdc-orders/
├── Dockerfile
├── README.md
├── kubernetes/
│   └── job.yaml
├── src/
│   ├── cdc_orders.py
│   └── cdc_orders_pipeline/
│       ├── config.py
│       ├── main.py
│       ├── spark.py
│       └── transforms.py
└── tests/
    ├── run_tests.py
    └── test_transforms.py
```

Platform-side CDC assets are separate from application logic:

```text
deployment/kubernetes/standalone/debezium.yaml
scripts/register-cdc-connector.py
platform/processing/spark/Dockerfile
platform/governance/data-quality/python/odp_data_quality/
```

## Validation

After the standalone platform is running:

```bash
make cdc-golden-path-test
```

The E2E test validates:

1. Debezium connector registration and `RUNNING` state;
2. initial six-row PostgreSQL snapshot;
3. six unique Bronze events and processing commits;
4. Silver and Gold publication;
5. a source update, insert and delete;
6. three additional CDC events;
7. correct current Silver state;
8. expected Gold totals;
9. replay with no duplicate Bronze or processed events;
10. no failed CDC pipeline runs.

The expected change scenario is:

```text
UPDATE order 1002: amount 80.00 -> 95.00
INSERT order 1007: amount 75.00, COMPLETED
DELETE order 1003: amount 45.25, CANCELLED
```

Final expected aggregate:

```text
orders           = 6
gross_amount     = 730.50
completed_amount = 640.50
```

## Production evolution

Before using the pattern as a production topology, add or replace:

- durable remote Spark checkpoints;
- HA/managed Kafka with TLS, authentication and ACLs;
- durable Kafka Connect config/offset/status topics with appropriate replication;
- PostgreSQL replication-slot and WAL-retention monitoring;
- workload identity and secret-manager-backed credentials;
- Debezium schema-change compatibility policy;
- operational dead-letter/poison-event workflow;
- Iceberg maintenance and retention policies;
- concurrent-writer controls;
- autoscaling and backpressure SLOs;
- lineage delivery to a backend;
- incremental Gold maintenance where scale requires it.
