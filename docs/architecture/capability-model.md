# Data Platform Capability Model

## Core capabilities

### Ingestion
Batch/file import, API acquisition, CDC, streaming ingestion, schema validation, replay and quarantine.

### Messaging and streaming backbone
Durable event transport, consumer isolation, schema compatibility, retention and replay.

### Storage and lakehouse
Object storage, table format, catalog, lifecycle management, compaction and retention.

### Processing
Batch, incremental and stream processing with workload isolation and reproducible dependency packaging.

### Orchestration
Dependency coordination, scheduling, retries, backfills and event-triggered execution. Business transformation logic should remain in executable jobs/models rather than orchestration definitions.

### Transformation
Reusable SQL/code transformations, tests and semantic modelling.

### Serving
Interactive/federated SQL, APIs, sharing and downstream consumption patterns.

## Cross-cutting capabilities

### Identity and security
Authentication, workload identity, authorization, secrets, encryption, network controls and audit.

### Governance
Ownership, catalog, classification, retention, lineage, policy and stewardship workflows.

### Data quality and observability
Freshness, completeness, validity, volume, schema drift and pipeline/data-product health.

### Platform observability and SRE
Metrics, logs, traces, SLOs, alerting, capacity, incident response, backup/recovery and upgrades.

### Platform engineering
IaC, CI/CD, GitOps, golden paths, templates, self-service and developer experience.

### FinOps
Resource attribution, cost allocation, budgets, utilization and workload efficiency.

## Implementation rule

Directories and APIs should expose these capabilities first. Technology-specific implementations live underneath and may be replaced without changing data-product contracts.
