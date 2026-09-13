import json
import unittest
from datetime import datetime

from pyspark.sql import SparkSession

from cdc_orders_pipeline.transforms import canonical_upserts, latest_change_per_order, parse_kafka_events


class CdcTransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("cdc-orders-tests").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def kafka_row(self, payload, offset):
        return (
            json.dumps(payload),
            "odp-commerce.source.orders",
            0,
            offset,
            datetime(2026, 8, 30, 10, 0, 0),
        )

    def test_create_event_parses_and_canonicalizes(self):
        payload = {
            "before": None,
            "after": {
                "order_id": 1007,
                "customer_id": " C007 ",
                "order_ts": 1788084000000,
                "status": "completed",
                "amount": "75.00",
                "country": "ma",
            },
            "source": {
                "version": "3.6.1.Final",
                "connector": "postgresql",
                "name": "odp-commerce",
                "ts_ms": 1788084000000,
                "db": "platform",
                "schema": "source",
                "table": "orders",
                "txId": 42,
                "lsn": 12345,
            },
            "op": "c",
            "ts_ms": 1788084001000,
        }
        source = self.spark.createDataFrame(
            [self.kafka_row(payload, 10)],
            "value string, topic string, partition int, offset long, timestamp timestamp",
        )
        event = parse_kafka_events(source).first()
        self.assertEqual(event.order_id, 1007)
        self.assertEqual(event._cdc_op, "c")
        self.assertEqual(event._source_lsn, 12345)
        self.assertTrue(event._event_id)

        canonical = canonical_upserts(parse_kafka_events(source), "run-1", "v1alpha2").first()
        self.assertEqual(canonical.customer_id, "C007")
        self.assertEqual(canonical.status, "COMPLETED")
        self.assertEqual(str(canonical.amount), "75.00")
        self.assertEqual(canonical.country, "MA")

    def test_delete_uses_before_primary_key(self):
        payload = {
            "before": {"order_id": 1003},
            "after": None,
            "source": {"lsn": 20000, "txId": 43, "ts_ms": 1788085000000},
            "op": "d",
            "ts_ms": 1788085001000,
        }
        source = self.spark.createDataFrame(
            [self.kafka_row(payload, 11)],
            "value string, topic string, partition int, offset long, timestamp timestamp",
        )
        event = parse_kafka_events(source).first()
        self.assertEqual(event.order_id, 1003)
        self.assertEqual(event._cdc_op, "d")

    def test_latest_lsn_wins_for_same_order(self):
        rows = [
            ("a", "topic", 0, 1, datetime(2026, 8, 30), "u", 10, 1, datetime(2026, 8, 30), 1, "C1", 1, "PENDING", "10.00", "MA"),
            ("b", "topic", 0, 2, datetime(2026, 8, 30), "u", 20, 2, datetime(2026, 8, 30), 1, "C1", 1, "COMPLETED", "20.00", "MA"),
        ]
        schema = "_event_id string, _kafka_topic string, _kafka_partition int, _kafka_offset long, _kafka_timestamp timestamp, _cdc_op string, _source_lsn long, _source_tx_id long, _source_ts timestamp, order_id long, customer_id string, _order_ts_ms long, status string, _amount_raw string, country string"
        latest = latest_change_per_order(self.spark.createDataFrame(rows, schema)).first()
        self.assertEqual(latest._source_lsn, 20)
        self.assertEqual(latest.status, "COMPLETED")


if __name__ == "__main__":
    unittest.main()
