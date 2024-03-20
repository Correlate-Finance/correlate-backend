import unittest
from core.main_logic import create_index, correlate_datasets
from unittest import mock
import pandas as pd
from datasets.models import CorrelationMetric, AggregationPeriod, CorrelateDataPoint

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


class TestCorrelateDatasets(unittest.TestCase):
    def test_correct_correlation_with_self(self):
        test_df = pd.DataFrame(TEST_DATA)

        result = correlate_datasets(test_df, test_df, "Test Dataset", lag_periods=0)

        assert result is not None  # required for type checker
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], CorrelateDataPoint)

        # Since it's the same dataset, the correlation should be 1.0
        self.assertAlmostEqual(result[0].pearson_value, 1.0)

        self.assertEqual(result[0].lag, 0)
        self.assertEqual(result[0].title, "Test Dataset")
        self.assertEqual(result[0].internal_name, "Test Dataset")
        self.assertEqual(result[0].input_data, list(range(1, 25)))
        self.assertEqual(result[0].dataset_data, list(range(1, 25)))

    def test_correct_correlation_with_lag(self):
        test_df = pd.DataFrame(TEST_DATA)
        test_df_2 = pd.DataFrame(TEST_DATA_2)

        result = correlate_datasets(test_df, test_df_2, "Test Dataset", lag_periods=3)

        assert result is not None
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], CorrelateDataPoint)
        self.assertEqual(len(result), 4)

        for i, point in enumerate(result):
            self.assertEqual(point.lag, i)
            self.assertEqual(point.title, "Test Dataset")
            self.assertEqual(point.internal_name, "Test Dataset")
            self.assertEqual(point.input_data, list(range(1, 25)))
            self.assertEqual(point.dataset_data, list(range(2, 49, 2)))
            self.assertAlmostEqual(point.pearson_value, 1.0, places=4)

    def test_insufficient_data(self):
        test_df = pd.DataFrame(TEST_DATA).iloc[:3]  # Taking only 3 rows
        df = pd.DataFrame(TEST_DATA_2).iloc[:3]

        result = correlate_datasets(test_df, df, "Test Dataset", lag_periods=0)

        self.assertIsNone(result)


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
