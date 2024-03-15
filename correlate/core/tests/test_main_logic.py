import unittest
from core.main_logic import create_index
from unittest import mock
import pandas as pd
from datasets.models import CorrelationMetric, AggregationPeriod

DATES = [
    "2020-01-01",
    "2020-02-01",
    "2020-03-01",
    "2020-04-01",
    "2020-05-01",
    "2020-06-01",
    "2020-07-01",
    "2020-08-01",
    "2020-09-01",
    "2020-10-01",
    "2020-11-01",
    "2020-12-01",
    "2021-01-01",
    "2021-02-01",
    "2021-03-01",
    "2021-04-01",
    "2021-05-01",
    "2021-06-01",
    "2021-07-01",
    "2021-08-01",
    "2021-09-01",
    "2021-10-01",
    "2021-11-01",
    "2021-12-01",
]

TEST_DATA = {
    "Date": DATES,
    "Value": list(range(1, 25)),
}

TEST_DATA_2 = {
    "Date": DATES,
    "Value": list(range(2, 49, 2)),
}


class TestCreateIndex(unittest.TestCase):
    def test_create_index_quarterly_raw_value(self):
        with mock.patch(
            "core.main_logic.get_all_dfs",
            return_value={
                "test": pd.DataFrame(TEST_DATA),
                "test2": pd.DataFrame(TEST_DATA_2),
            },
        ):
            index = create_index(
                {"test": 0.5, "test2": 0.5},
                CorrelationMetric.RAW_VALUE,
                AggregationPeriod.QUARTERLY,
                "December",
            )
            self.assertIsNotNone(index)
            assert index is not None  # required for type checker
            self.assertEqual(len(index), 8)
            self.assertAlmostEqual(index["Value"].iloc[0], 0.086956, 3)
            self.assertAlmostEqual(index["Value"].iloc[1], 0.217391, 3)

    def test_create_index_quarterly_yoy_growth(self):
        with mock.patch(
            "core.main_logic.get_all_dfs",
            return_value={
                "test": pd.DataFrame(TEST_DATA),
                "test2": pd.DataFrame(TEST_DATA_2),
            },
        ):
            index = create_index(
                {"test": 0.5, "test2": 0.5},
                CorrelationMetric.YOY_GROWTH,
                AggregationPeriod.QUARTERLY,
                "December",
            )

            self.assertIsNotNone(index)
            assert index is not None  # required for type checker
            self.assertEqual(len(index), 4)
            self.assertAlmostEqual(index["Value"].iloc[0], 6, 3)
            self.assertAlmostEqual(index["Value"].iloc[1], 2.4, 3)
            self.assertAlmostEqual(index["Value"].iloc[-1], 1.0909, 3)

    # Edge cases
    def test_create_index_quarterly_raw_value_empty(self):
        with mock.patch(
            "core.main_logic.get_all_dfs",
            return_value={},
        ):
            index = create_index(
                {}, CorrelationMetric.RAW_VALUE, AggregationPeriod.QUARTERLY, "December"
            )
            self.assertIsNone(index)

    def test_create_index_no_weights(self):
        with mock.patch(
            "core.main_logic.get_all_dfs",
            return_value={
                "test": pd.DataFrame(TEST_DATA),
                "test2": pd.DataFrame(TEST_DATA_2),
            },
        ):
            index = create_index(
                {}, CorrelationMetric.RAW_VALUE, AggregationPeriod.QUARTERLY, "December"
            )
            self.assertIsNone(index)
