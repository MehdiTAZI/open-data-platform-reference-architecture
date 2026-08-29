# Airflow Reference Orchestration

Airflow is implemented as a **control-plane orchestrator**, not as a transformation runtime.

The reference DAG under `dags/batch_orders_snapshot.py` does one thing: it creates and supervises the already-versioned `batch-orders` Kubernetes Job. All JDBC extraction, quality validation, Medallion transformations and Iceberg publishing remain inside the independently versioned application image.

```text
Airflow DAG
   |
   | KubernetesJobOperator
   v
Kubernetes Job: batch-orders
   |
   v
Spark application image
   |
   +--> PostgreSQL
   +--> Iceberg / Polaris / object storage
```

## Why this boundary matters

- DAG changes do not become a second implementation of business logic.
- The Spark application can run without Airflow for recovery, testing and alternative orchestrators.
- The same immutable application artifact is used for manual, scheduled and recovery execution.
- Airflow receives only the Kubernetes permissions needed to create and observe Jobs in `odp-data`.

This implements ADR-004: Airflow orchestrates; business logic stays outside DAGs.

## Standalone

The local image extends Apache Airflow 3.3.1 with the pinned `apache-airflow-providers-cncf-kubernetes` provider. It uses in-cluster Kubernetes authentication through the `airflow-orchestrator` ServiceAccount.

`airflow standalone` is still a development topology. Production requires the normal HA Airflow architecture, an external metadata database, production authentication, secret management, persistent logs and explicit scheduler/API/worker sizing.

## RBAC

The standalone orchestrator can create, inspect, watch, patch and delete Kubernetes Jobs in `odp-data`, and inspect their Pods/logs/events. It cannot mutate Deployments, Secrets, ConfigMaps or platform services.

Production should split permissions further by namespace/domain where required and use cloud/workload identity for any external APIs.
