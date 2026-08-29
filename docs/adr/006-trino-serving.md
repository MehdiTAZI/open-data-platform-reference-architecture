# ADR-006: Trino as reference analytical serving engine

- Status: Accepted
- Date: 2026-08-29
- Owners: Platform Architecture

## Decision

Use Trino as the default interactive SQL and federation layer over Iceberg and supported external systems.

## Rationale

Serving is kept separate from storage and batch compute, demonstrating independent scaling and engine interoperability.

## Production implications

Coordinator/worker availability, workload management, authentication, authorization, query limits, catalog credential isolation and observability must be configured explicitly.
