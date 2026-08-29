# ADR-003: Apache Spark as the default general-purpose compute engine

- Status: Accepted
- Date: 2026-08-29
- Owners: Platform Architecture

## Context

The golden path requires one primary engine for scalable batch transformation and streaming scenarios without multiplying operational stacks.

## Decision

Use Apache Spark as the default general-purpose compute engine. Spark Structured Streaming covers the initial streaming golden path. Apache Flink remains an optional extension for streaming-centric workloads requiring its processing model or latency/state characteristics.

## Alternatives considered

- Apache Flink as the universal default
- Apache Beam with multiple runners
- Warehouse-native transformation only

## Consequences

Spark version/runtime compatibility, executor isolation, autoscaling, shuffle behavior, dependency packaging and observability become standard platform concerns.
