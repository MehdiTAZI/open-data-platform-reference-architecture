# ADR-008: Apache Polaris as the reference Iceberg REST catalog

- Status: Accepted
- Date: 2026-08-29
- Owners: Platform Architecture

## Context

The lakehouse requires a catalog contract that is independent from any single compute engine and supports Iceberg REST interoperability.

## Decision

Use Apache Polaris as the reference implementation of the Iceberg REST catalog.

The standalone profile uses Polaris with PostgreSQL relational JDBC persistence rather than an in-memory metastore so restart and metadata lifecycle behavior are closer to production.

## Alternatives considered

- engine-specific Hive-compatible metastores;
- cloud-vendor proprietary catalogs;
- other Iceberg REST-compatible catalog services.

## Consequences

The platform owns catalog availability, schema migration, realm bootstrap, authentication configuration and storage credential integration.

## Production implications

Production Polaris deployments require HA replicas, durable HA PostgreSQL, stable OAuth signing keys, realm-header validation, controlled bootstrap credentials, backups, schema-upgrade procedures and observability. The standalone deployment only implements a subset of these controls.
