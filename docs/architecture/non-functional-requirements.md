# Non-Functional Requirements

These are reference requirements. Concrete deployments must derive values from business criticality and workload tiering rather than blindly copy targets.

| Dimension | Reference requirement |
|---|---|
| Availability | Tiered SLO; production control-plane services target >=99.9% where business-required |
| Durability | Storage configuration must document durability assumptions and replication responsibility |
| RPO/RTO | Defined per data product/platform service; restore procedures tested |
| Scalability | Horizontal scaling preferred for stateless/compute services; storage/metadata limits documented |
| Security | Least privilege, encrypted transport, encrypted persistent data, auditable administrative access |
| Isolation | Environment and workload isolation via identity, namespace, network and resource boundaries |
| Portability | Core logical architecture must not require a single cloud provider |
| Operability | Metrics, logs, health endpoints, runbooks and ownership required |
| Upgradeability | Version policy, compatibility checks, rollback path and maintenance procedure required |
| Cost | Domain/product/environment attribution required for production resources |
| Compliance | Classification, retention and access evidence represented as versioned controls |
| Testability | Automated contract/integration/E2E tests must cover golden paths |

## Workload tiers

- **Tier 0** — platform control-plane or business-critical service. HA, tested DR, strict change control.
- **Tier 1** — production data product with explicit consumer SLO.
- **Tier 2** — non-critical production analytics; relaxed recovery targets.
- **Tier 3** — development, experimentation and ephemeral workloads.

The repository examples default to Tier 3 locally and document what must change to reach Tier 0/1.
