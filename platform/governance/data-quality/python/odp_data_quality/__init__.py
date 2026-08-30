"""Portable Data Contract primitives shared by processing adapters.

Spark-specific evaluation lives in :mod:`odp_data_quality.quality` and is imported
explicitly by Spark applications so contract parsing does not require PySpark.
"""

from .contracts import DataContract, QualityRule, assert_required_columns, load_contract

__all__ = [
    "DataContract",
    "QualityRule",
    "assert_required_columns",
    "load_contract",
]
