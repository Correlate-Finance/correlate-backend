import unittest
import pandas as pd

from core.data_processing import parse_input_dataset, transform_data
from parameterized import parameterized
from datetime import datetime
from datasets.models import AggregationPeriod, CorrelationMetric

TEST_DATA = {
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
QUARTERLY_DATA = {
    "Date": [
        "2020Q1",
        "2020Q2",
        "2020Q3",
        "2020Q4",
        "2021Q1",
        "2021Q2",
        "2021Q3",
        "2021Q4",
    ],
    "Value": [1] * 4 + [2] * 4,
}


class TestTransformData(unittest.TestCase):
    def setUp(self) -> None:
        self.test_data = TEST_DATA
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
        self.quarterly_data = QUARTERLY_DATA
        return super().setUp()

    def test_transform_empty(self):
        df = pd.DataFrame()
        result = transform_data(df, AggregationPeriod.QUARTERLY)
        self.assertTrue(result.empty)

    def test_transform_quarterly_fiscal_end_month_throws_error(self):
        df = pd.DataFrame(self.test_data)
        with self.assertRaises(ValueError):
            transform_data(df, AggregationPeriod.QUARTERLY)

    @parameterized.expand([[TEST_DATA], [QUARTERLY_DATA]])
    def test_transform_quarterly(self, test_data):
        df = pd.DataFrame(test_data)
        result = transform_data(
            df, AggregationPeriod.QUARTERLY, fiscal_end_month="December"
        )
        self.assertEqual(result["Date"].iloc[0], pd.Period("2020Q1", "Q-DEC"))
        self.assertEqual(result["Date"].iloc[3], pd.Period("2020Q4", "Q-DEC"))

    def test_transform_quarterly_less_than_three_months_of_data(self):
        df = pd.DataFrame(self.test_data).iloc[:2]
        result = transform_data(
            df, AggregationPeriod.QUARTERLY, fiscal_end_month="December"
        )
        self.assertTrue(result.empty)

    def test_transform_quarterly_fiscal_month_January(self):
        df = pd.DataFrame(self.test_data)
        result = transform_data(
            df, AggregationPeriod.QUARTERLY, fiscal_end_month="January"
        )
        self.assertEqual(result["Date"].iloc[0], pd.Period("2021Q1", "Q-JAN"))
        self.assertEqual(result["Date"].iloc[2], pd.Period("2021Q3", "Q-JAN"))

    def test_transform_quarterly_incomplete_start(self):
        df = pd.DataFrame(self.incomplete_data_start)
        result = transform_data(
            df, AggregationPeriod.QUARTERLY, fiscal_end_month="December"
        )
        self.assertEqual(result["Date"].iloc[0], pd.Period("2020Q2", "Q-DEC"))
        self.assertEqual(result["Date"].iloc[2], pd.Period("2020Q4", "Q-DEC"))

    def test_transform_quarterly_incomplete_end(self):
        df = pd.DataFrame(self.incomplete_data_end)
        result = transform_data(
            df, AggregationPeriod.QUARTERLY, fiscal_end_month="December"
        )
        self.assertEqual(result["Date"].iloc[0], pd.Period("2020Q1", "Q-DEC"))
        self.assertEqual(result["Date"].iloc[2], pd.Period("2020Q3", "Q-DEC"))

    def test_transform_quarterly_yoy_growth(self):
        df = pd.DataFrame(self.two_year_data)
        result = transform_data(
            df,
            AggregationPeriod.QUARTERLY,
            fiscal_end_month="December",
            correlation_metric=CorrelationMetric.YOY_GROWTH,
        )

        self.assertAlmostEqual(result["Value"].iloc[0], 1)
        self.assertEqual(result["Value"].size, 4)

    @parameterized.expand([[TEST_DATA], [QUARTERLY_DATA]])
    def test_transform_annual(self, test_data):
        df = pd.DataFrame(test_data)
        result = transform_data(
            df, AggregationPeriod.ANNUALLY, fiscal_end_month="December"
        )
        self.assertEqual(result["Date"].iloc[0], pd.to_datetime("2020-01-01"))

    def test_transform_annual_yoy_growth(self):
        df = pd.DataFrame(self.two_year_data)
        result = transform_data(
            df,
            AggregationPeriod.ANNUALLY,
            fiscal_end_month="December",
            correlation_metric=CorrelationMetric.YOY_GROWTH,
        )
        self.assertAlmostEqual(result["Value"].iloc[0], 1)

    def test_transform_data_with_start_date(self):
        df = pd.DataFrame(self.test_data)
        result = transform_data(
            df,
            AggregationPeriod.QUARTERLY,
            start_date=datetime(2020, 6, 1),
            fiscal_end_month="December",
        )
        self.assertEqual(result["Value"].iloc[0], 24)
        self.assertEqual(result["Value"].iloc[-1], 33)
        self.assertEqual(result["Value"].size, 2)

    def test_transform_quarterly_with_start_date(self):
        df = pd.DataFrame(self.quarterly_data)
        result = transform_data(
            df,
            AggregationPeriod.QUARTERLY,
            fiscal_end_month="December",
            start_date=datetime(2020, 6, 1),
        )
        self.assertEqual(result["Date"].iloc[0], pd.Period("2020Q3", "Q-DEC"))
        self.assertEqual(result["Date"].iloc[-1], pd.Period("2021Q4", "Q-DEC"))
        self.assertEqual(result["Value"].size, 6)


class TestParseInputDataset(unittest.TestCase):
    def test_parse_input_dataset_with_floats(self):
        input_data = "Q1'14\t0.05\nQ2'14\t-0.02\nQ3'14\t0.04\nQ4'14\t0.02\nQ1'15\t0.01\nQ2'15\t-0.02\nQ3'15\t-0.04\nQ4'15\t-0.05\nQ1'16\t-0.05\nQ2'16\t0.01\nQ3'16\t0.00\nQ4'16\t0.00\nQ1'17\t0.06\nQ2'17\t0.04\nQ3'17\t0.07\nQ4'17\t0.07\nQ1'18\t0.05\nQ2'18\t0.10\nQ3'18\t0.12\nQ4'18\t0.08\nQ1'19\t0.06\nQ2'19\t0.03\nQ3'19\t0.02\nQ4'19\t-0.03\nQ1'20\t-0.05\nQ2'20\t-0.20\nQ3'20\t-0.17\nQ4'20\t-0.04\nQ1'21\t0.02\nQ2'21\t0.08\nQ3'21\t0.07\nQ4'21\t0.07\nQ1'22\t0.11\nQ2'22\t0.13\nQ3'22\t0.17\nQ4'22\t0.09\nQ1'23\t0.09\nQ2'23\t0.10\nQ3'23\t-0.01\nQ4'23\t0.03"
        result = parse_input_dataset(input_data)
        assert result

        self.assertEqual(result["Date"][0], "2014Q1")
        self.assertEqual(result["Value"][0], 0.05)
        self.assertEqual(result["Date"][-1], "2023Q4")
        self.assertEqual(result["Value"][-1], 0.03)

    def test_parse_input_dataset_with_integers(self):
        input_data = "Q1'14\t5\nQ2'14\t-2\nQ3'14\t4\nQ4'14\t2\nQ1'15\t1\nQ2'15\t-2\nQ3'15\t-4\nQ4'15\t-5\nQ1'16\t-5\nQ2'16\t1\nQ3'16\t0\nQ4'16\t0\nQ1'17\t6\nQ2'17\t4\nQ3'17\t7\nQ4'17\t7\nQ1'18\t5\nQ2'18\t10\nQ3'18\t12\nQ4'18\t8\nQ1'19\t6\nQ2'19\t3\nQ3'19\t2\nQ4'19\t-3\nQ1'20\t-5\nQ2'20\t-20\nQ3'20\t-17\nQ4'20\t-4\nQ1'21\t2\nQ2'21\t8\nQ3'21\t7\nQ4'21\t7\nQ1'22\t11\nQ2'22\t13\nQ3'22\t17\nQ4'22\t9\nQ1'23\t9\nQ2'23\t10\nQ3'23\t-1\nQ4'23\t3"
        result = parse_input_dataset(input_data)
        assert result

        self.assertEqual(result["Date"][0], "2014Q1")
        self.assertEqual(result["Value"][0], 5)
        self.assertEqual(result["Date"][-1], "2023Q4")
        self.assertEqual(result["Value"][-1], 3)

    def test_parse_input_dataset_with_integers_with_commas(self):
        input_data = "Q1'14\t5,000"
        result = parse_input_dataset(input_data)
        assert result

        self.assertEqual(result["Date"][0], "2014Q1")
        self.assertEqual(result["Value"][0], 5000)
