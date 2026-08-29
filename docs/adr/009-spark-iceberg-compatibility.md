# ADR-009: Select the supported Spark/Iceberg compatibility intersection

- Status: Accepted
- Date: 2026-08-29
- Owners: Platform Architecture

## Context

Spark 4.2.0 is newer than Spark 4.1.3, but Apache Iceberg 1.11.0 currently publishes maintained integration artifacts for Spark 4.0 and 4.1, not Spark 4.2.

Selecting the independently newest release of every platform component can create an unsupported combination even when each product is individually stable.

## Decision

Use Spark **4.1.3** with Iceberg **1.11.0** and Scala **2.13** until Iceberg publishes and maintains a Spark 4.2 integration artifact.

The standalone Spark image includes `iceberg-spark-runtime-4.1_2.13:1.11.0` and `iceberg-aws-bundle:1.11.0` with SHA-512 verification during build.

## Consequences

- component upgrades are compatibility-matrix decisions, not independent version bumps;
- Dependabot or release automation must not automatically upgrade Spark across a minor compatibility boundary;
- upgrading to Spark 4.2 requires an ADR update and successful multi-engine integration tests.
