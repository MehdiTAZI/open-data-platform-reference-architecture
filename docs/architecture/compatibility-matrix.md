# Runtime Compatibility Matrix

The reference stack is selected as a **tested compatibility set**, not by independently choosing the newest version of each product.

| Component | Version | Compatibility contract |
|---|---:|---|
| Apache Iceberg | 1.11.0 | table format / REST client libraries |
| Apache Spark | 4.1.3 | Iceberg 1.11.0 publishes `iceberg-spark-runtime-4.1_2.13` |
| Apache Polaris | 1.7.0 | Iceberg REST catalog |
| Trino | 483 | Iceberg REST catalog + native S3 filesystem |
| Garage | 2.3.0 | standalone S3-compatible object store; compatibility is verified by repository E2E tests |

## Compatibility policy

1. A component upgrade is not accepted merely because a newer release exists.
2. The dependency graph must be checked first (engine connector/runtime artifacts, protocol support and storage compatibility).
3. CI validates manifests on every change; scheduled standalone integration validates runtime behavior.
4. Production releases must additionally lock images by digest and record the tested matrix in release notes.

## Spark / Iceberg decision

Spark 4.2.0 is a current Spark stable release, but Iceberg 1.11.0 currently publishes official runtime artifacts for Spark 4.0 and 4.1. The reference stack therefore uses Spark 4.1.3 until an Iceberg release explicitly supports Spark 4.2.

This is intentional: **supported interoperability is preferred over version-number freshness**.
