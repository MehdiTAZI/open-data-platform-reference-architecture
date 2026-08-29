# Production Readiness Standard

A deployment is not production-ready until all applicable controls below are satisfied or explicitly risk-accepted.

## Delivery and supply chain

- [ ] Versioned source and reviewed pull request
- [ ] Reproducible build
- [ ] Dependency and container scanning
- [ ] Secret scanning
- [ ] SBOM generated for release artifacts
- [ ] Immutable artifact/image version; production preferably references digest
- [ ] Provenance/signing strategy documented

## Runtime

- [ ] Resource requests/limits
- [ ] Readiness/liveness/startup probes as applicable
- [ ] Pod disruption and anti-affinity/topology strategy for HA services
- [ ] Explicit persistence/storage class
- [ ] NetworkPolicy default-deny posture with documented allowed flows
- [ ] Non-root/restricted security context when supported

## Identity and security

- [ ] Workload identity or short-lived credential model
- [ ] Least-privilege authorization
- [ ] External secret manager for production
- [ ] Encryption in transit
- [ ] Encryption at rest
- [ ] Audit events retained according to policy

## Operations

- [ ] Owner and on-call/escalation path
- [ ] Metrics, logs and dashboards
- [ ] Alerts tied to actionable SLO symptoms
- [ ] Capacity assumptions
- [ ] Upgrade procedure
- [ ] Rollback procedure
- [ ] Backup and restore procedure for stateful services
- [ ] Restore tested
- [ ] RTO/RPO documented
- [ ] Known failure modes/runbook

## Validation

- [ ] Configuration schema validation
- [ ] Unit tests where applicable
- [ ] Integration tests
- [ ] Smoke tests
- [ ] End-to-end golden-path test
- [ ] Failure/recovery test for Tier 0/1 services

Local mode is intentionally smaller, but it must use the same interfaces/configuration contracts and must never be used as evidence that HA/DR production requirements are satisfied.
