# Control Plane and Data Plane

## Control plane responsibilities

- declarative platform configuration
- data-product and data-contract registration
- identity/policy lifecycle
- orchestration metadata
- catalog/governance metadata
- GitOps reconciliation
- platform APIs and self-service workflows

## Data plane responsibilities

- ingestion execution
- event transport
- batch/stream computation
- object/table storage
- SQL query execution
- data-product materialization and serving

## Boundary rules

- Control-plane credentials must not become general-purpose data-plane credentials.
- Data workloads consume scoped identities, not platform administrator identities.
- Production metadata services are treated as stateful dependencies with backup/recovery requirements.
- A compromised tenant workload should not be able to mutate platform configuration or other tenants by default.
- Network flows cross boundaries through explicit allow rules.

## Availability

Control-plane outages should not unnecessarily destroy already-running data-plane workloads. Conversely, data-plane saturation must not starve platform management components. Production deployments therefore require resource and failure-domain isolation beyond the namespace model shown in local mode.
