import unittest
from datetime import datetime, timedelta
from decimal import Decimal

from pyspark.sql import SparkSession

from orders_pipeline.contracts import load_contract
from orders_pipeline.quality import DataQualityFailure, evaluate

CONTRACT_PATH = "/opt/odp/contracts/batch-orders.yaml"
SCHEMA = "order_id long, customer_id string, order_ts timestamp, order_date date, status string, amount decimal(18,2), country string"


class QualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("batch-orders-quality")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.contract = load_contract(CONTRACT_PATH)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_invalid_rows_are_quarantined_with_contract_rule_names(self):
        now = datetime.utcnow()
        rows = [
            (1, "C001", now, now.date(), "COMPLETED", Decimal("10.00"), "MA"),
            (2, "C002", now, now.date(), "UNKNOWN", Decimal("20.00"), "FR"),
            (3, "C003", now, now.date(), "PENDING", Decimal("-1.00"), "ES"),
            (4, "C004", now, now.date(), "PENDING", Decimal("5.00"), "MOR"),
        ]
        result = evaluate(self.spark.createDataFrame(rows, SCHEMA), self.contract)
        self.assertEqual(result.valid.count(), 1)
        self.assertEqual(result.invalid.count(), 3)
        reasons = {
            reason
            for row in result.invalid.select("_dq_errors").collect()
            for reason in row._dq_errors
        }
        self.assertIn("status_accepted_values", reasons)
        self.assertIn("amount_non_negative", reasons)
        self.assertIn("country_iso2_shape", reasons)

        metrics = {metric.rule_name: metric for metric in result.metrics}
        self.assertEqual(metrics["status_accepted_values"].status, "QUARANTINE")
        self.assertEqual(metrics["status_accepted_values"].violation_count, 1)
        self.assertEqual(metrics["amount_non_negative"].violation_count, 1)

    def test_duplicate_business_keys_are_rejected(self):
        now = datetime.utcnow()
        rows = [
            (10, "C001", now, now.date(), "COMPLETED", Decimal("10.00"), "MA"),
            (10, "C002", now, now.date(), "PENDING", Decimal("20.00"), "FR"),
        ]
        result = evaluate(self.spark.createDataFrame(rows, SCHEMA), self.contract)
        self.assertEqual(result.valid.count(), 0)
        self.assertEqual(result.invalid.count(), 2)
        metric = next(metric for metric in result.metrics if metric.rule_name == "order_id_unique")
        self.assertEqual(metric.violation_count, 2)
        self.assertEqual(metric.status, "QUARANTINE")

    def test_warning_rule_does_not_quarantine_business_row(self):
        stale = datetime.utcnow() - timedelta(days=7)
        rows = [
            (20, "C020", stale, stale.date(), "COMPLETED", Decimal("30.00"), "FR"),
        ]
        result = evaluate(self.spark.createDataFrame(rows, SCHEMA), self.contract)
        self.assertEqual(result.valid.count(), 1)
        self.assertEqual(result.invalid.count(), 0)
        freshness = next(metric for metric in result.metrics if metric.rule_name == "order_freshness")
        self.assertEqual(freshness.status, "WARN")
        self.assertEqual(freshness.violation_count, 1)

    def test_fail_action_stops_empty_dataset(self):
        empty = self.spark.createDataFrame([], SCHEMA)
        with self.assertRaises(DataQualityFailure) as failure:
            evaluate(empty, self.contract)
        metrics = {metric.rule_name: metric for metric in failure.exception.metrics}
        self.assertEqual(metrics["source_non_empty"].status, "FAIL")


if __name__ == "__main__":
    unittest.main()
