import unittest
from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from orders_pipeline.context import PipelineRun
from orders_pipeline.layers import bronze, gold, silver


class LayerTransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("batch-orders-unit").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def source(self):
        return self.spark.createDataFrame(
            [
                (1, " C001 ", datetime(2026, 8, 25, 10, 0), " completed ", Decimal("10.50"), "ma"),
                (2, "C002", datetime(2026, 8, 25, 11, 0), "PENDING", Decimal("20.00"), "FR"),
            ],
            "order_id long, customer_id string, order_ts timestamp, status string, amount decimal(18,2), country string",
        )

    def test_bronze_preserves_source_and_adds_metadata(self):
        result = bronze.transform(self.source(), PipelineRun.create())
        self.assertEqual(result.count(), 2)
        self.assertTrue(set(bronze.REQUIRED_SOURCE_COLUMNS).issubset(result.columns))
        self.assertIn("_run_id", result.columns)
        self.assertIn("_record_hash", result.columns)

    def test_silver_normalizes_canonical_fields(self):
        result = silver.transform(bronze.transform(self.source(), PipelineRun.create())).orderBy("order_id")
        first = result.first()
        self.assertEqual(first.customer_id, "C001")
        self.assertEqual(first.status, "COMPLETED")
        self.assertEqual(first.country, "MA")

    def test_gold_reconciles_order_count(self):
        trusted = silver.transform(bronze.transform(self.source(), PipelineRun.create()))
        result = gold.transform(trusted)
        self.assertEqual(sum(row.order_count for row in result.collect()), 2)


if __name__ == "__main__":
    unittest.main()
