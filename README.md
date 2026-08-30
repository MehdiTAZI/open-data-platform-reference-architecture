# Open Data Platform Reference Architecture

> Vendor-neutral, production-oriented and executable reference architecture for building modern enterprise Data Platforms.

This repository defines **architecture first, technology second**. It models the capabilities, security boundaries, governance controls, operational requirements and delivery practices required for a modern Data Platform, then provides replaceable reference implementations for those capabilities.

The target is deliberately dual-mode:

- **Standalone / local** — a production-shaped environment on Kind for learning, architecture validation and integration testing.
- **Production / industrialized** — deployable through Infrastructure as Code, Kubernetes, CI/CD, GitOps, policy enforcement, observability and environment-specific configuration.

> **Important:** standalone is intentionally small and partially ephemeral. It validates production interfaces; it is not itself a production topology. See [`docs/operations/standalone.md`](docs/operations/standalone.md) and [`docs/operations/production-readiness.md`](docs/operations/production-readiness.md).

## Architecture principles

1. **Capabilities over products** — ingestion, processing, storage, orchestration, serving, governance, security and observability are architectural capabilities. Products are implementations.
2. **Open interfaces and standards** — prefer portable standards and open protocols to proprietary coupling.
3. **Security by design** — workload identity, least privilege, encryption, secrets management and auditability are platform primitives.
4. **Everything as code** — infrastructure, platform configuration, policies, contracts, data products, quality expectations and observability are versioned.
5. **One golden path, explicit alternatives** — implement one coherent reference stack and document alternatives through ADRs.
6. **Production parity through contracts** — environments may use smaller or managed implementations, but workload-facing interfaces stay compatible.
7. **Observable and operable by default** — telemetry, health checks, SLOs, lineage and runbooks are part of the design.
8. **Data products and contracts** — ownership, quality, classification, retention and service expectations are machine-readable.
9. **Automation before manual operations** — repeatable builds, tests, deployment and recovery are mandatory.
10. **Portable core, cloud adapters** — the logical platform remains cloud-neutral; AWS, Azure and GCP are deployment targets.

## Reference stack

| Capability | Reference implementation |
|---|---|
| Container platform | Kubernetes |
| Infrastructure as Code | OpenTofu |
| Local Kubernetes | Kind |
| Standalone object storage | Garage (S3-compatible) |
| Production object storage | Cloud-native / explicitly supported S3-compatible storage |
| Event streaming | Apache Kafka |
| CDC | Debezium / Kafka Connect |
| Batch & streaming compute | Apache Spark |
| Lakehouse table format | Apache Iceberg |
| Iceberg REST catalog | Apache Polaris |
| Catalog persistence | PostgreSQL |
| SQL serving / federation | Trino |
| Orchestration | Apache Airflow |
| Analytics transformation | dbt |
| Data quality control plane | ODP Data Contracts + shared Spark adapter |
| Lineage standard | OpenLineage |
| Telemetry standard | OpenTelemetry |
| Metrics / dashboards | Prometheus / Grafana |
| Policy enforcement | Open Policy Agent |
| GitOps | Argo CD |
| CI/CD | GitHub Actions |

All runtime versions are centrally tracked in [`config/versions.yaml`](config/versions.yaml). Production release pipelines are expected to lock deployable images to immutable digests.

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
                               Iceberg REST
                                      |
                              Apache Polaris
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

## Runtime contracts

The standalone and production environments share contracts rather than identical physical implementations:

```text
                      STABLE PLATFORM CONTRACTS
                               |
        +----------------------+----------------------+
        |                      |                      |
   Kubernetes API           S3 API             Iceberg REST
        |                      |                      |
     Kafka API             Spark Jobs           Trino / Airflow
        |                      |                      |
        +----------------------+----------------------+
                               |
                 environment-specific topology
```

See [`docs/architecture/runtime-interfaces.md`](docs/architecture/runtime-interfaces.md).

## Repository map

```text
.
├── config/                    # Central compatibility/version inputs
├── docs/
│   ├── architecture/          # Principles, runtime model, NFRs and patterns
│   ├── adr/                   # Architecture Decision Records
│   ├── governance/            # Ownership, classification and quality guidance
│   ├── operations/            # SLOs, monitoring, DR, capacity and FinOps
│   └── security/              # Threat model and security guidance
├── specs/
│   ├── data-contracts/        # Machine-readable schemas and quality expectations
│   └── data-products/         # Machine-readable data product definitions
├── platform/                  # Reusable platform capabilities and shared libraries
├── infrastructure/            # OpenTofu bootstrap, modules and cloud compositions
├── deployment/                # Kubernetes and GitOps deployment assets
├── examples/golden-paths/     # Executable batch and CDC reference applications + tests
├── scripts/                   # Developer, integration and CI utility scripts
└── .github/                   # CI, security, IaC and standalone integration workflows
```

## Standalone quick start

Recommended Docker allocation: **8 CPUs, 12 GiB RAM, 20 GiB free disk**.

```bash
make doctor
make validate
make local-up
make smoke-test
```

Run the executable data-engineering golden paths:

```bash
make batch-golden-path-test
make cdc-golden-path-test
```

Inspect the environment:

```bash
make local-status
```

Generated local credentials are written to `.local/credentials.env` and never committed.

Stop and remove everything:

```bash
make local-down
```

The current standalone profile boots:

```text
Kind / Kubernetes
├── odp-system
│   ├── Apache Polaris + PostgreSQL-backed metastore
│   └── Apache Airflow
├── odp-data
│   ├── Garage S3-compatible object storage
│   ├── Apache Kafka (single-node KRaft)
│   ├── Debezium / Kafka Connect
│   ├── PostgreSQL with logical replication enabled
│   ├── Apache Spark client + dynamic Kubernetes executors
│   └── Trino
└── odp-observability
    ├── OpenTelemetry Collector
    ├── Prometheus
    └── Grafana
```

## Executable golden paths

### Batch snapshot

[`examples/golden-paths/batch-orders`](examples/golden-paths/batch-orders) demonstrates:

```text
PostgreSQL -> Spark -> Bronze -> Silver / Quarantine -> Gold -> Iceberg/Polaris -> Trino
```

It includes append-only Bronze audit history, contract-driven DQ, rule metrics, OpenLineage run events, reconciliation and replay validation.

### Change Data Capture

[`examples/golden-paths/cdc-orders`](examples/golden-paths/cdc-orders) demonstrates:

```text
PostgreSQL -> Debezium -> Kafka -> Spark Structured Streaming
           -> Bronze -> Silver MERGE/DELETE -> Gold -> Iceberg/Polaris -> Trino
```

It includes update/insert/delete handling, durable event identity, a separate processing-commit table, contract-driven quality, quarantine, OpenLineage and replay after checkpoint loss or partial downstream failure.

## Delivery roadmap

### V0.1 — Architecture foundation ✅

- Capability model and architecture principles
- Production/non-production design constraints
- Security model and non-functional requirements
- ADR framework and initial technology decisions
- Data product and data contract specifications
- Repository quality gates and CI skeleton

### V0.2 — Standalone executable platform ✅

- Kind-based local Kubernetes runtime
- centrally pinned component versions
- generated standalone-only secrets
- PostgreSQL source/metadata services
- S3-compatible Garage storage
- Kafka 4.x KRaft event backbone
- Debezium / Kafka Connect CDC runtime
- PostgreSQL-backed Apache Polaris catalog
- Spark-on-Kubernetes runtime with Iceberg and Kafka connectors
- Trino serving runtime
- Airflow standalone orchestration runtime
- Prometheus, Grafana and OpenTelemetry baseline
- default-deny networking plus explicit standalone allow rules
- resource constraints, probes and smoke tests
- scheduled and PR-gated full-stack integration workflow

### V0.3 — Golden pipelines 🚧

Implemented:

- ✅ Batch: PostgreSQL → Spark → Iceberg/Polaris → Trino
- ✅ CDC: PostgreSQL → Debezium → Kafka → Spark Structured Streaming → Iceberg
- ✅ shared Data Contract-driven quality with `fail` / `quarantine` / `warn`
- ✅ rule-level DQ results and OpenLineage run events

Remaining V0.3 extensions:

- Streaming: Kafka → Structured Streaming → Iceberg with watermark/late-event semantics
- dbt analytical transformations and tests
- schema-evolution compatibility scenarios
- explicit backfill/incremental batch variant

### V1.0 — Production reference

- Environment promotion and GitOps
- Cloud adapters beyond the implemented AWS reference composition
- production workload identity and secret-manager integrations by workload
- production observability, SLOs, runbooks and alerting
- backup/restore, DR and capacity validation
- FinOps tagging and cost allocation
- policy enforcement and supply-chain security
- immutable release promotion and drift management

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

The project is currently **pre-v1.0**. APIs, schemas and layouts may evolve. Production-readiness claims apply only to components explicitly marked as such in their documentation.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
