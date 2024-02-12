from core.data import test_data
import pandas as pd
from core.data_trends import (
    calculate_trailing_months,
    calculate_year_over_year_growth,
    calculate_yearly_stacks,
    calculate_average_monthly_growth,
)
import unittest


class TestDataTrends(unittest.TestCase):
    # TODO: Add a golden file to compare the results of the test
    def test_calculate_yearly_stacks(self):
        df = pd.DataFrame(test_data)
        df["Date"] = pd.to_datetime(df["Date"])

        df = calculate_trailing_months(df)
        df = calculate_year_over_year_growth(df)
        df = calculate_yearly_stacks(df, years=5)

        assert "Stack2Y" in df.columns
        assert "Stack3Y" in df.columns
        assert "Stack4Y" in df.columns
        assert "Stack5Y" in df.columns

    def test_calculate_average_monthly_growth(self):
        df = pd.DataFrame(test_data)
        df["Date"] = pd.to_datetime(df["Date"])

        df = calculate_trailing_months(df)
        df = calculate_year_over_year_growth(df)
        df = calculate_average_monthly_growth(df, years=5)

        assert "averageMoM" in df.columns
        assert "DeltaSeasonality" in df.columns
