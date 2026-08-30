# ADR 010 — Medallion Layer Semantics

- Status: Accepted
- Date: 2026-08-30

## Context

Bronze, Silver and Gold need consistent engineering semantics across pipelines, especially for replay, quality and publication.

## Decision

### Bronze
- Preserve source business fields with minimal transformation.
- Add run, source, contract-version, ingestion-time and record-hash metadata.
- Prefer append-only ingestion history.

### Silver
- Apply canonical types and representations.
- Apply deduplication and data-quality rules.
- Separate invalid rows through explicit quarantine or fail behavior.
- Reconcile valid plus rejected rows to the input boundary.

### Gold
- Derive only from trusted Silver data.
- Contain business-facing metrics or serving models.
- Validate business invariants and critical reconciliations before publication.

## Replay semantics

Business-state idempotency does not require erasing ingestion history. An identical replay may append a new Bronze run while producing the same Silver and Gold state.

## Consequences

Pipelines need run metadata, reconciliation metrics and explicit quality behavior. Bronze also requires retention and maintenance policies because replay preserves ingestion history.
