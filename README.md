# Open Data Platform Reference Architecture

> Vendor-neutral, production-oriented and executable reference architecture for building modern enterprise Data Platforms.

This repository defines **architecture first, technology second**. It models the capabilities, security boundaries, governance controls, operational requirements and delivery practices required for a modern Data Platform, then provides replaceable reference implementations for those capabilities.

The target is deliberately dual-mode:

- **Standalone / local** — runnable on a developer workstation for learning, architecture validation and integration testing.
- **Production / industrialized** — deployable through Infrastructure as Code, Kubernetes, CI/CD, GitOps, policy enforcement, observability and environment-specific configuration.

## Architecture principles

1. **Capabilities over products** — ingestion, processing, storage, orchestration, serving, governance, security and observability are architectural capabilities. Products are implementations.
2. **Open interfaces and standards** — prefer portable standards and open protocols to proprietary coupling.
3. **Security by design** — workload identity, least privilege, encryption, secrets management and auditability are platform primitives.
4. **Everything as code** — infrastructure, platform configuration, policies, contracts, data products, quality expectations and observability are versioned.
5. **One golden path, explicit alternatives** — the repository implements one coherent reference stack and documents alternatives through ADRs instead of creating an unmaintainable product zoo.
6. **Production parity** — local environments are smaller, not architecturally different.
7. **Observable and operable by default** — telemetry, health checks, SLOs, lineage and runbooks are part of the design.
8. **Data products and contracts** — ownership, quality, classification, retention and service expectations are machine-readable.
9. **Automation before manual operations** — repeatable builds, tests, deployment and recovery are mandatory.
10. **Portable core, cloud adapters** — the logical platform remains cloud-neutral; AWS, Azure and GCP are deployment targets.

## Reference stack

| Capability | Default reference implementation |
|---|---|
| Container platform | Kubernetes |
| Infrastructure as Code | OpenTofu |
| Local Kubernetes | Kind |
| Local object storage | MinIO (S3-compatible) |
| Event streaming | Apache Kafka |
| CDC | Debezium / Kafka Connect |
| Batch & general-purpose compute | Apache Spark |
| Lakehouse table format | Apache Iceberg |
| SQL serving / federation | Trino |
| Orchestration | Apache Airflow |
| Analytics transformation | dbt |
| Lineage standard | OpenLineage |
| Telemetry standard | OpenTelemetry |
| Metrics / dashboards | Prometheus / Grafana |
| Policy enforcement | Open Policy Agent |
| GitOps | Argo CD |
| CI/CD | GitHub Actions |

The reference stack is **replaceable by design**. Decisions and alternatives are documented under [`docs/adr`](docs/adr).

## Logical architecture

```text
                                  DATA SOURCES
                     DB / SaaS / APIs / Files / Events
                                      |
                 +--------------------+--------------------+
                 |                    |                    |
               Batch                 CDC               Streaming
                 |                    |                    |
                 +--------------------+--------------------+
                                      |
                           INGESTION & MESSAGING
                                      |
                  +-------------------+-------------------+
                  |                                       |
             Object Storage                         Event Streaming
                  |                                       |
                  +-------------------+-------------------+
                                      |
                                  LAKEHOUSE
                              Apache Iceberg
                                      |
                    +-----------------+-----------------+
                    |                 |                 |
                  Spark             Trino             Flink*
                    |                 |                 |
                    +-----------------+-----------------+
                                      |
                               DATA PRODUCTS
                                      |
                         BI / APIs / ML / Sharing

  Cross-cutting planes:
  --------------------------------------------------------------------
  Security & Identity | Governance & Quality | Observability & SRE
  Platform Engineering | CI/CD & GitOps | FinOps | Metadata & Lineage
  --------------------------------------------------------------------

  *Flink is an optional extension; Spark is the default processing engine.
```

## Repository map

```text
.
├── architecture/              # Reference diagrams, capability model and patterns
├── docs/
│   ├── architecture/          # Logical/physical architecture and NFRs
│   ├── adr/                   # Architecture Decision Records
│   ├── governance/            # Ownership, classification, quality, retention
│   ├── operations/            # SLOs, monitoring, DR, capacity, FinOps
│   └── security/              # Threat model, IAM, secrets, encryption, network
├── specs/
│   ├── data-contracts/        # Machine-readable data contracts
│   ├── data-products/         # Machine-readable data product definitions
│   └── policies/              # Policy-as-code inputs
├── platform/                  # Capability implementations
├── infrastructure/            # OpenTofu modules and environment composition
├── deployment/                # Kubernetes, Helm and GitOps deployment assets
├── examples/                  # End-to-end golden paths
├── tests/                     # Unit, contract, integration, infra and E2E tests
├── scripts/                   # Developer and CI utility scripts
└── .github/                   # CI workflows and contribution automation
```

## Delivery roadmap

### V0.1 — Architecture foundation

- Capability model and architecture principles
- Production/non-production design constraints
- Security model and non-functional requirements
- ADR framework and initial technology decisions
- Data product and data contract specifications
- Repository quality gates and CI skeleton

### V0.2 — Standalone executable platform

- Kind-based local Kubernetes platform
- MinIO, Kafka, Spark, Iceberg, Trino and Airflow
- Repeatable `make local-up` / `make local-down`
- Health checks and smoke tests

### V0.3 — Golden pipelines

- Batch: PostgreSQL → Spark → Iceberg → Trino
- CDC: PostgreSQL → Debezium → Kafka → Spark → Iceberg
- Streaming: Kafka → Structured Streaming → Iceberg
- dbt transformations, data quality and OpenLineage

### V1.0 — Production reference

- Environment promotion and GitOps
- Cloud adapters for AWS, Azure and GCP
- Workload identity and secret-manager integration
- Observability, SLOs, runbooks and alerting
- Backup/restore, DR and capacity guidance
- FinOps tagging and cost allocation
- Policy enforcement and supply-chain security

## Quick start

The standalone implementation is intentionally built to converge toward:

```bash
make doctor
make bootstrap
make local-up
make smoke-test
```

The commands are added incrementally as the executable platform is introduced. No production deployment should rely on implicit defaults; production configuration is environment-specific and validated through CI.

## Production readiness model

A component is considered production-ready in this repository only when it has:

- version-pinned dependencies/images;
- least-privilege identity and secret handling;
- explicit resource requests/limits;
- liveness/readiness/startup probes where applicable;
- high-availability and disruption guidance;
- persistent-state and backup/restore strategy;
- metrics, logs and health checks;
- upgrade and rollback procedure;
- configuration validation;
- automated tests and security scanning;
- documented SLO/RTO/RPO expectations;
- reproducible deployment through IaC/GitOps.

Local examples may reduce replicas and persistence requirements, but they must preserve the same logical contracts and interfaces.

## Status

This repository is being built iteratively. Until `v1.0.0`, APIs, schemas and layouts may evolve. Production-readiness claims apply only to components explicitly marked as such in their documentation.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
