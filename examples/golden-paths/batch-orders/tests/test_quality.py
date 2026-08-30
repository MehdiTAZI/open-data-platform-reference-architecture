import unittest
from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from orders_pipeline.quality import classify


class QualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("batch-orders-quality").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_invalid_rows_are_quarantined_with_reasons(self):
        rows = [
            (1, "C001", datetime(2026, 8, 25, 10, 0), datetime(2026, 8, 25).date(), "COMPLETED", Decimal("10.00"), "MA"),
            (2, "C002", datetime(2026, 8, 25, 11, 0), datetime(2026, 8, 25).date(), "UNKNOWN", Decimal("20.00"), "FR"),
            (3, "C003", datetime(2026, 8, 25, 12, 0), datetime(2026, 8, 25).date(), "PENDING", Decimal("-1.00"), "ES"),
            (4, "C004", datetime(2026, 8, 25, 13, 0), datetime(2026, 8, 25).date(), "PENDING", Decimal("5.00"), "MOR"),
        ]
        df = self.spark.createDataFrame(
            rows,
            "order_id long, customer_id string, order_ts timestamp, order_date date, status string, amount decimal(18,2), country string",
        )
        result = classify(df)
        self.assertEqual(result.valid.count(), 1)
        self.assertEqual(result.invalid.count(), 3)
        reasons = {reason for row in result.invalid.select("_dq_errors").collect() for reason in row._dq_errors}
        self.assertIn("status_accepted_values", reasons)
        self.assertIn("amount_non_negative", reasons)
        self.assertIn("country_iso2_shape", reasons)

    def test_duplicate_business_keys_are_rejected(self):
        rows = [
            (10, "C001", datetime(2026, 8, 25, 10, 0), datetime(2026, 8, 25).date(), "COMPLETED", Decimal("10.00"), "MA"),
            (10, "C002", datetime(2026, 8, 25, 11, 0), datetime(2026, 8, 25).date(), "PENDING", Decimal("20.00"), "FR"),
        ]
        df = self.spark.createDataFrame(
            rows,
            "order_id long, customer_id string, order_ts timestamp, order_date date, status string, amount decimal(18,2), country string",
        )
        result = classify(df)
        self.assertEqual(result.valid.count(), 0)
        self.assertEqual(result.invalid.count(), 2)


if __name__ == "__main__":
    unittest.main()
