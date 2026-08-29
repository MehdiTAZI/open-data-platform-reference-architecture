# ADR-001: Kubernetes as the reference runtime

- Status: Accepted
- Date: 2026-08-29
- Owners: Platform Architecture

## Context

The reference architecture requires a consistent runtime model for standalone development and industrialized environments while preserving portability and declarative operations.

## Decision

Use Kubernetes as the default runtime abstraction. Kind is the standalone reference implementation; managed Kubernetes services are preferred for production cloud deployments unless requirements justify another runtime.

## Alternatives considered

- Direct VM/systemd deployment: simpler for individual services but weaker consistency and self-service automation.
- Cloud-specific serverless runtimes: operationally attractive for selected workloads but inappropriate as the vendor-neutral core abstraction.

## Consequences

Kubernetes becomes an important platform dependency and requires cluster lifecycle, policy, observability, capacity and security engineering. Application teams should consume platform abstractions rather than raw cluster complexity where possible.

## Production implications

Production requires multi-node failure-domain design, workload identity, default-deny network policy, controlled ingress/egress, admission policy, resource governance, upgrade strategy and managed control-plane consideration.
