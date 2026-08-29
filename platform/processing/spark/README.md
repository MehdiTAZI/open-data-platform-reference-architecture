# Spark Runtime

The reference runtime intentionally uses **Spark 4.1.3** with **Iceberg 1.11.0**.

Iceberg 1.11.0 publishes and maintains a Spark 4.1 / Scala 2.13 integration artifact. Spark 4.2 is newer, but is not currently listed in Iceberg's supported engine matrix; production reference stacks prefer a supported compatibility intersection over independently selecting the latest version of every component.

The standalone image extends the upstream Apache Spark image with:

- `iceberg-spark-runtime-4.1_2.13:1.11.0`;
- `iceberg-aws-bundle:1.11.0`.

Both artifacts are downloaded from Maven Central during image build and checked against their published SHA-512 values.

The runtime connects to Polaris using the Iceberg REST catalog and accesses standalone S3 storage directly with generated local credentials. Production deployments replace static S3 credentials with workload identity and/or credential vending where the selected object store supports it.
