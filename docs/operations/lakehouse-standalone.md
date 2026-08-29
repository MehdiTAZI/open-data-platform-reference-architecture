# Standalone Lakehouse Path

The standalone environment validates one shared metadata/storage path across multiple engines:

```text
Spark 4.1.3
   |
Iceberg 1.11 REST client
   |
Apache Polaris 1.7.0
   |
+-------------------+
| metadata          | files
v                   v
PostgreSQL       Garage / S3
                    ^
                    |
                 Trino 483
```

## Why Spark 4.1 rather than 4.2

Iceberg 1.11.0 officially publishes Spark integration artifacts for Spark 4.0 and Spark 4.1. The reference uses Spark 4.1.3 so the engine/runtime combination is upstream-supported.

## Credential model

Garage does not expose an AWS STS credential-vending contract used by the production AWS pattern. For standalone only, Polaris is configured with `stsUnavailable=true`, and Spark/Trino receive the generated Garage access key from Kubernetes Secrets.

Production environments must prefer workload identity / STS-style short-lived credentials where supported. Static S3 access keys in the standalone profile are a local compatibility mechanism, not a production recommendation.

## Validation

Run:

```bash
make lakehouse-smoke-test
```

The test:

1. submits Spark with the official Iceberg 1.11 Spark 4.1 runtime;
2. creates a namespace and Iceberg table through Polaris;
3. writes deterministic records to the S3-compatible warehouse;
4. reads the same table with Trino through the same Polaris REST catalog;
5. fails if the engines do not observe the same data.

The Iceberg dependencies are currently resolved from Maven for the standalone smoke test. A production runtime image must bake and checksum-verify these artifacts before V1.0 release qualification.
