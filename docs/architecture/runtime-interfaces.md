# Runtime Interfaces

Industrialization depends on stable interfaces rather than identical local and cloud implementations.

| Interface | Contract | Standalone implementation | Production mapping |
|---|---|---|---|
| Runtime | Kubernetes API | Kind | EKS / AKS / GKE / conformant Kubernetes |
| Object storage | S3 API | Garage | S3 / compatible managed object store |
| Event backbone | Kafka protocol | Apache Kafka single-node KRaft | HA Kafka service/cluster |
| Table catalog | Iceberg REST | Apache Polaris | HA Polaris or compatible managed catalog |
| Metadata persistence | PostgreSQL protocol | PostgreSQL pod | managed/HA PostgreSQL |
| Processing | Spark-on-Kubernetes | Spark 4.2 client + executors | Spark jobs on Kubernetes |
| SQL serving | Trino HTTP/JDBC | single-node Trino | coordinator + worker pools |
| Orchestration | Airflow API/DAG model | standalone Airflow | HA Airflow deployment |

A local implementation may be replaced in production only when the external contract used by workloads remains compatible or the migration is explicitly governed by an ADR.
