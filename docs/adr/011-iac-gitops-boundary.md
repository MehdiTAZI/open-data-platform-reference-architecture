# ADR 011 — OpenTofu and GitOps Ownership Boundary

- Status: Accepted
- Date: 2026-08-30

## Context

Cloud infrastructure and Kubernetes platform software need different lifecycles. Managing the same Kubernetes resources from both OpenTofu and GitOps creates competing control planes and unclear ownership.

## Decision

OpenTofu owns cloud infrastructure and Kubernetes foundations, including networking, managed Kubernetes, IAM/workload identity, KMS, object storage, DNS/TLS prerequisites and cloud observability prerequisites.

GitOps owns software deployed onto Kubernetes, including Kafka, Spark runtime configuration, Airflow, Polaris, Trino, observability agents and application workloads.

OpenTofu may create bootstrap resources required for GitOps, but it must not become the long-term reconciler for GitOps-owned application manifests.

## Consequences

- Cloud resources and platform applications can evolve independently.
- Drift ownership is explicit.
- Kubernetes software rollback stays in the GitOps workflow.
- Provider-specific infrastructure remains isolated from portable workload contracts.
