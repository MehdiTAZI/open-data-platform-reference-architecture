# Standalone Runtime

The standalone profile is a **small production-shaped environment**, not a production deployment.

## Goals

It validates the same architectural contracts used by industrialized deployments:

- Kubernetes as the runtime API;
- S3-compatible object storage contract;
- Kafka protocol for event streaming;
- PostgreSQL-backed durable catalog metadata;
- Iceberg REST catalog through Apache Polaris;
- Spark-on-Kubernetes execution;
- Trino SQL serving;
- Airflow orchestration API.

The local profile intentionally reduces replicas, persistence, authentication complexity and infrastructure size.

## Requirements

Recommended workstation allocation:

- 8 CPU cores available to Docker;
- 12 GiB RAM available to Docker;
- 20 GiB free disk space;
- Docker, Kind, kubectl, OpenSSL and Python 3.

## Start

```bash
make doctor
make validate
make local-up
make smoke-test
```

Generated credentials are stored under `.local/credentials.env`, which is ignored by Git. They are standalone-only, randomly generated on each bootstrap and must never be reused in production.

## Inspect

```bash
make local-status
```

For temporary host access, use `kubectl port-forward` rather than publishing every service:

```bash
kubectl -n odp-data port-forward svc/trino 8080:8080
kubectl -n odp-system port-forward svc/airflow 8081:8080
kubectl -n odp-system port-forward svc/polaris 8181:8181
kubectl -n odp-data port-forward svc/garage 3900:3900
```

## Stop

```bash
make local-down
```

## Intentional standalone reductions

| Capability | Standalone | Production expectation |
|---|---|---|
| Kubernetes | 1 Kind control-plane node | managed/HA cluster across failure domains |
| Object storage | Garage single node, ephemeral | durable cloud/S3-compatible object store with redundancy |
| Kafka | 1 combined KRaft broker/controller | isolated KRaft controllers and multiple brokers |
| PostgreSQL | 1 ephemeral pod | managed/HA PostgreSQL with backup/PITR |
| Polaris | 1 replica | HA replicas, controlled OAuth keys, durable metastore |
| Spark | client pod + dynamic executor pods | job-specific drivers, quotas, autoscaling and controlled images |
| Trino | 1 coordinator/worker | dedicated coordinator + scalable workers |
| Airflow | `airflow standalone` | HA production topology, external DB and production executor |
| Network | default-deny + local contract allows | workload-specific ingress/egress and controlled external destinations |
| Secrets | generated Kubernetes Secrets | external secret manager + workload identity |

No item in the standalone column is, by itself, evidence of production readiness.
