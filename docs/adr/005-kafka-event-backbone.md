# ADR-005: Apache Kafka as reference event backbone

- Status: Accepted
- Date: 2026-08-29
- Owners: Platform Architecture

## Context

CDC and streaming golden paths need durable event transport, replay and independent consumer groups.

## Decision

Use Apache Kafka as the reference event backbone. Production deployments should prefer operator/managed-service lifecycle automation and modern metadata mode rather than bespoke node administration.

## Alternatives considered

Apache Pulsar and cloud-native streaming services are credible alternatives for specific organizations.

## Consequences

Topic governance, quotas, retention, partitioning, schema compatibility, authentication/authorization, multi-AZ durability and capacity become explicit platform responsibilities.
