# Lakehouse Runtime Contract

The executable reference separates four responsibilities:

```text
Spark / Trino
     |
     | Iceberg REST + OAuth2
     v
Apache Polaris
     |
     | Iceberg metadata
     v
Apache Iceberg
     |
     | S3 API
     v
Object Storage
```

## Standalone

- Spark 4.1.3 writes Iceberg 1.11.0 tables.
- Trino 483 reads and writes the same tables.
- Polaris 1.7.0 owns the Iceberg REST catalog and persists its service metadata in PostgreSQL.
- Garage 2.3.0 provides the local S3-compatible object storage contract.
- engine credentials and storage credentials are generated locally and are intentionally ephemeral.

## Production mapping

Production deployments must replace standalone root/static credentials with dedicated principals, least privilege, workload identity and/or storage credential vending. Object storage must be a tested, durable service. Polaris and its relational metadata store require HA, backup and restore procedures.

## Interoperability acceptance test

`make lakehouse-smoke-test` proves that:

1. Spark authenticates to Polaris over Iceberg REST;
2. Spark creates a namespace and Iceberg table;
3. Spark writes data files to S3-compatible storage;
4. Polaris commits catalog metadata;
5. Trino independently resolves the same table through Polaris;
6. Trino reads the exact rows Spark wrote.

This test deliberately validates interfaces between engines instead of only testing each component in isolation.
