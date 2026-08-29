from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.reference")
spark.sql("DROP TABLE IF EXISTS polaris.reference.engine_interop")
spark.sql("""
CREATE TABLE polaris.reference.engine_interop (
  id BIGINT,
  engine STRING,
  created_at TIMESTAMP
) USING iceberg
""")
spark.sql("""
INSERT INTO polaris.reference.engine_interop VALUES
  (1, 'spark', current_timestamp()),
  (2, 'trino-readable', current_timestamp())
""")

rows = spark.sql("SELECT id, engine FROM polaris.reference.engine_interop ORDER BY id").collect()
assert [(r.id, r.engine) for r in rows] == [(1, "spark"), (2, "trino-readable")]
print("Spark wrote and read Iceberg through Polaris successfully")
