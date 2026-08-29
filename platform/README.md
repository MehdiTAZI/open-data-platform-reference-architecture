# Platform Implementations

This directory is organized by **capability**, not product.

The executable standalone assets currently live under `deployment/kubernetes/standalone` while capability-specific source code, images and configuration will progressively be introduced here.

Planned capability boundaries:

```text
platform/
├── ingestion/
│   ├── batch/
│   ├── cdc/
│   └── streaming/
├── processing/
│   └── spark/
├── orchestration/
│   └── airflow/
├── storage/
│   └── iceberg/
├── serving/
│   └── trino/
├── governance/
├── security/
└── observability/
```

Every implementation must document its interface contract, local profile, production profile, security model, operational ownership and replacement/migration boundary.
