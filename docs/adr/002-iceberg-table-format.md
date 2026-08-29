# ADR-002: Apache Iceberg as the reference lakehouse table format

- Status: Accepted
- Date: 2026-08-29
- Owners: Platform Architecture

## Context

The platform needs an open table abstraction over object storage that separates compute from storage and supports interoperable analytical engines.

## Decision

Use Apache Iceberg as the reference table format and expose catalog interaction through an implementation-neutral catalog interface where practical.

## Alternatives considered

- Delta Lake
- Apache Hudi

Both remain valid workload-dependent alternatives. The repository does not attempt to implement all formats in its golden path.

## Consequences

The architecture must explicitly manage catalog availability, metadata lifecycle, object-store consistency/security, compaction/maintenance and engine compatibility.

## Production implications

Catalog and object storage are stateful critical dependencies. Backup/recovery responsibilities and table maintenance procedures must be documented before Tier 0/1 use.
