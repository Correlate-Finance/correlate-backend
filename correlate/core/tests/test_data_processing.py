import unittest
import pandas as pd
import math

from core.data_processing import transform_data


class TestTransformData(unittest.TestCase):
    def setUp(self) -> None:
        self.test_data = {
            "Date": [
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
            ],
            "Value": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        }
        self.incomplete_data_start = {
            "Date": [
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
            ],
            "Value": [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        }
        self.incomplete_data_end = {
            "Date": [
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
            ],
            "Value": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
        }
        self.two_year_data = {
            "Date": [
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
            ],
            "Value": [1] * 12 + [2] * 12,
        }
        return super().setUp()

    def test_transform_empty(self):
        df = pd.DataFrame()
        result = transform_data(df, "Monthly")
        self.assertTrue(result.empty)

    def test_transform_monthly(self):
        df = pd.DataFrame(self.test_data)
        result = transform_data(df, "Monthly")
        self.assertEqual(result["Value"].iloc[0], 1)
        self.assertEqual(result["Value"].iloc[11], 12)

    def test_transform_monthly_yoy_growth(self):
        df = pd.DataFrame(self.two_year_data)
        result = transform_data(df, "Monthly", correlation_metric="YOY_GROWTH")
        self.assertTrue(math.isnan(result["Value"].iloc[0]))
        self.assertAlmostEqual(result["Value"].iloc[12], 1)

    def test_transform_quarterly_fiscal_end_month_throws_error(self):
        df = pd.DataFrame(self.test_data)
        with self.assertRaises(ValueError):
            transform_data(df, "Quarterly")

    def test_transform_quarterly(self):
        df = pd.DataFrame(self.test_data)
        result = transform_data(df, "Quarterly", fiscal_end_month="December")
        self.assertEqual(result["Date"].iloc[0], pd.Period("2020Q1", "Q-DEC"))
        self.assertEqual(result["Date"].iloc[3], pd.Period("2020Q4", "Q-DEC"))

    def test_transform_quarterly_less_than_three_months_of_data(self):
        df = pd.DataFrame(self.test_data).iloc[:2]
        result = transform_data(df, "Quarterly", fiscal_end_month="December")
        self.assertTrue(result.empty)

    def test_transform_quarterly_fiscal_month_January(self):
        df = pd.DataFrame(self.test_data)
        result = transform_data(df, "Quarterly", fiscal_end_month="January")
        self.assertEqual(result["Date"].iloc[0], pd.Period("2021Q1", "Q-JAN"))
        self.assertEqual(result["Date"].iloc[2], pd.Period("2021Q3", "Q-JAN"))

    def test_transform_quarterly_incomplete_start(self):
        df = pd.DataFrame(self.incomplete_data_start)
        result = transform_data(df, "Quarterly", fiscal_end_month="December")
        self.assertEqual(result["Date"].iloc[0], pd.Period("2020Q2", "Q-DEC"))
        self.assertEqual(result["Date"].iloc[2], pd.Period("2020Q4", "Q-DEC"))

    def test_transform_quarterly_incomplete_end(self):
        df = pd.DataFrame(self.incomplete_data_end)
        result = transform_data(df, "Quarterly", fiscal_end_month="December")
        self.assertEqual(result["Date"].iloc[0], pd.Period("2020Q1", "Q-DEC"))
        self.assertEqual(result["Date"].iloc[2], pd.Period("2020Q3", "Q-DEC"))

    def test_transform_quarterly_yoy_growth(self):
        df = pd.DataFrame(self.two_year_data)
        result = transform_data(
            df,
            "Quarterly",
            fiscal_end_month="December",
            correlation_metric="YOY_GROWTH",
        )

        self.assertTrue(math.isnan(result["Value"].iloc[0]))
        self.assertAlmostEqual(result["Value"].iloc[4], 1)

    def test_transform_annual(self):
        df = pd.DataFrame(self.test_data)
        result = transform_data(df, "Annually", fiscal_end_month="December")
        self.assertEqual(result["Date"].iloc[0], pd.to_datetime("2020-01-01"))

    def test_transform_annual_yoy_growth(self):
        df = pd.DataFrame(self.two_year_data)
        result = transform_data(
            df,
            "Annually",
            fiscal_end_month="December",
            correlation_metric="YOY_GROWTH",
        )
        self.assertTrue(math.isnan(result["Value"].iloc[0]))
        self.assertAlmostEqual(result["Value"].iloc[1], 1)

    def test_invalid_time_increment(self):
        df = pd.DataFrame(self.test_data)
        with self.assertRaises(ValueError):
            transform_data(df, "Invalid")
