import os

from pyspark.sql import SparkSession


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def build_spark() -> SparkSession:
    polaris_secret = required("POLARIS_CLIENT_SECRET")
    garage_access = required("GARAGE_ACCESS_KEY")
    garage_secret = required("GARAGE_SECRET_KEY")
    return (
        SparkSession.builder.appName("odp-cdc-orders")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", "http://polaris.odp-system.svc.cluster.local:8181/api/catalog")
        .config("spark.sql.catalog.polaris.warehouse", "odp")
        .config("spark.sql.catalog.polaris.credential", f"root:{polaris_secret}")
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.header.Polaris-Realm", "odp")
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.polaris.s3.endpoint", "http://garage.odp-data.svc.cluster.local:3900")
        .config("spark.sql.catalog.polaris.s3.path-style-access", "true")
        .config("spark.sql.catalog.polaris.s3.region", "garage")
        .config("spark.sql.catalog.polaris.s3.access-key-id", garage_access)
        .config("spark.sql.catalog.polaris.s3.secret-access-key", garage_secret)
        .getOrCreate()
    )
