# ADR-007: Garage for standalone S3-compatible object storage

- Status: Accepted
- Date: 2026-08-29
- Owners: Platform Architecture

## Context

The standalone environment needs an actively maintained S3-compatible object store so that lakehouse clients exercise an object-storage interface rather than a local filesystem. MinIO Community Edition changed distribution in 2026 and its historical public repository was archived, making it a weaker default for a new reproducible reference environment.

## Decision

Use Garage 2.3.0 as the standalone S3-compatible object store.

This decision is **not** a recommendation to use Garage as the universal production storage layer. Production deployments should normally use the target cloud's durable object storage or another explicitly supported S3-compatible implementation selected through an environment-specific ADR.

## Consequences

- The local environment validates S3 endpoint, region and credential behavior.
- Garage runs as a single-node, replication-factor-1 instance locally.
- Local object data is ephemeral by default.
- Workloads must not depend on Garage-specific APIs.

## Production implications

Production object storage requires durability, encryption, IAM/workload identity, lifecycle, audit, network controls, backup/recovery assumptions and tested Iceberg compatibility. A single-node Garage instance does not satisfy those requirements.
