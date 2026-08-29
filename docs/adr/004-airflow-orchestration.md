# ADR-004: Apache Airflow as reference orchestrator

- Status: Accepted
- Date: 2026-08-29
- Owners: Platform Architecture

## Context

The golden path needs scheduling, dependency management, retries, backfills and operational visibility without embedding transformation logic in the orchestrator.

## Decision

Use Apache Airflow as the reference workflow orchestrator. DAGs coordinate independently testable jobs/models and should remain thin.

## Alternatives considered

Dagster, Prefect and cloud-native orchestrators remain valid alternatives. They are documented rather than simultaneously implemented.

## Consequences

Airflow metadata persistence, executor choice, secrets integration, DAG delivery, HA scheduler/web components and upgrade compatibility require production engineering.
