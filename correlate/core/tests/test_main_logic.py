import unittest
from core.main_logic import create_index
from unittest import mock
import pandas as pd

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
                {"test": 0.5, "test2": 0.5}, "RAW_VALUE", "Quarterly", "December"
            )
            self.assertIsNotNone(index)
            assert index is not None  # required for type checker
            self.assertEqual(len(index), 8)
            self.assertAlmostEqual(index["Value"].iloc[0], 0.086956, 3)
            self.assertAlmostEqual(index["Value"].iloc[1], 0.217391, 3)

    def test_create_index_monthly_raw_value(self):
        with mock.patch(
            "core.main_logic.get_all_dfs",
            return_value={
                "test": pd.DataFrame(TEST_DATA),
                "test2": pd.DataFrame(TEST_DATA_2),
            },
        ):
            index = create_index(
                {"test": 0.5, "test2": 0.5}, "RAW_VALUE", "Monthly", "December"
            )
            self.assertIsNotNone(index)
            assert index is not None  # required for type checker
            self.assertEqual(len(index), 24)
            self.assertAlmostEqual(index["Value"].iloc[0], 1 / 24, 3)
            self.assertAlmostEqual(index["Value"].iloc[1], 2 / 24, 3)

    def test_create_index_quarterly_yoy_growth(self):
        with mock.patch(
            "core.main_logic.get_all_dfs",
            return_value={
                "test": pd.DataFrame(TEST_DATA),
                "test2": pd.DataFrame(TEST_DATA_2),
            },
        ):
            index = create_index(
                {"test": 0.5, "test2": 0.5}, "YOY_GROWTH", "Quarterly", "December"
            )

            self.assertIsNotNone(index)
            assert index is not None  # required for type checker
            self.assertEqual(len(index), 4)
            self.assertAlmostEqual(index["Value"].iloc[0], 6, 3)
            self.assertAlmostEqual(index["Value"].iloc[1], 2.4, 3)
            self.assertAlmostEqual(index["Value"].iloc[-1], 1.0909, 3)

    def test_create_index_monthly_yoy_growth(self):
        with mock.patch(
            "core.main_logic.get_all_dfs",
            return_value={
                "test": pd.DataFrame(TEST_DATA),
                "test2": pd.DataFrame(TEST_DATA_2),
            },
        ):
            index = create_index(
                {"test": 0.5, "test2": 0.5}, "YOY_GROWTH", "Monthly", "December"
            )
            self.assertIsNotNone(index)
            assert index is not None  # required for type checker
            self.assertEqual(len(index), 12)
            self.assertAlmostEqual(index["Value"].iloc[0], 12, 3)
            self.assertAlmostEqual(index["Value"].iloc[1], 6, 3)

    def test_create_index_monthly_yoy_growth_three_datasets(self):
        with mock.patch(
            "core.main_logic.get_all_dfs",
            return_value={
                "test": pd.DataFrame(TEST_DATA),
                "test2": pd.DataFrame(TEST_DATA_2),
                "test3": pd.DataFrame(
                    {
                        "Date": DATES,
                        "Value": [1] * 12 + [5] * 12,
                    }
                ),
            },
        ):
            index = create_index(
                {"test": 0.2, "test2": 0.3, "test3": 0.5},
                "YOY_GROWTH",
                "Monthly",
                "December",
            )
            self.assertIsNotNone(index)
            assert index is not None  # required for type checker
            self.assertEqual(len(index), 12)
            self.assertAlmostEqual(index["Value"].iloc[0], 8, 3)
            self.assertAlmostEqual(index["Value"].iloc[1], 5, 3)

    # Edge cases
    def test_create_index_quarterly_raw_value_empty(self):
        with mock.patch(
            "core.main_logic.get_all_dfs",
            return_value={},
        ):
            index = create_index({}, "RAW_VALUE", "Quarterly", "December")
            self.assertIsNone(index)

    def test_create_index_no_weights(self):
        with mock.patch(
            "core.main_logic.get_all_dfs",
            return_value={
                "test": pd.DataFrame(TEST_DATA),
                "test2": pd.DataFrame(TEST_DATA_2),
            },
        ):
            index = create_index({}, "RAW_VALUE", "Monthly", "December")
            self.assertIsNone(index)
