# Pattern — Idempotent Batch Snapshot to Iceberg

## Intent

Ingest a bounded relational snapshot into a lakehouse while preserving deterministic replay, explicit quality gates and separation between application logic and orchestration.

## Flow

```text
relational source -> Spark JDBC -> quality gate -> Bronze -> Silver -> Gold
                                           \____________________________/
                                                Iceberg atomic commits
```

## Invariants

1. The extraction has a documented logical boundary.
2. Source contract validation happens before publishing the new snapshot.
3. Every target table is written with an atomic Iceberg table commit.
4. Replaying the same source snapshot does not duplicate business records.
5. Orchestration contains no transformation logic.
6. A failed multi-table run is recovered by deterministic replay; it is not described as a distributed transaction.

## When to use

Use for bounded source datasets, periodic full snapshots, reference/master datasets and workloads for which complete replay is operationally acceptable.

## When not to use

Do not use full snapshot extraction as a substitute for CDC when source volume, source load, freshness or change semantics require incremental processing.

## Scaling adaptations

- use JDBC partition columns and explicit lower/upper bounds;
- cap source concurrency to protect operational systems;
- land a source-consistent extraction marker/run ID;
- partition Iceberg tables based on measured access patterns, not source layout;
- compact small files separately from ingestion;
- retain reconciliation metrics for source/target counts and business totals;
- parameterize backfills by business interval rather than editing code.
