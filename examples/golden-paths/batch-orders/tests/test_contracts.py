import tempfile
import unittest
from pathlib import Path

from orders_pipeline.contracts import load_contract

CONTRACT_PATH = "/opt/odp/contracts/batch-orders.yaml"


class ContractTests(unittest.TestCase):
    def test_reference_contract_loads_with_unique_rules(self):
        contract = load_contract(CONTRACT_PATH)
        self.assertEqual(contract.api_version, "odp/v1alpha2")
        self.assertEqual(contract.name, "batch-orders")
        self.assertEqual(len(contract.quality_rules), len({rule.name for rule in contract.quality_rules}))
        actions = {rule.action for rule in contract.quality_rules}
        self.assertEqual(actions, {"fail", "quarantine", "warn"})

    def test_contract_rejects_quality_rule_for_unknown_column(self):
        content = """
apiVersion: odp/v1alpha2
kind: DataContract
metadata:
  name: invalid
  owner: tests
spec:
  schema:
    - {name: order_id, type: long, nullable: false}
  quality:
    - name: missing_field_rule
      type: not_null
      column: does_not_exist
      severity: error
      action: quarantine
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "contract.yaml"
            path.write_text(content)
            with self.assertRaisesRegex(ValueError, "unknown contract columns"):
                load_contract(path)

    def test_warning_rules_cannot_quarantine(self):
        content = """
apiVersion: odp/v1alpha2
kind: DataContract
metadata:
  name: invalid
  owner: tests
spec:
  schema:
    - {name: order_id, type: long, nullable: false}
  quality:
    - name: invalid_warning
      type: not_null
      column: order_id
      severity: warning
      action: quarantine
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "contract.yaml"
            path.write_text(content)
            with self.assertRaisesRegex(ValueError, "must use action=warn"):
                load_contract(path)


if __name__ == "__main__":
    unittest.main()
