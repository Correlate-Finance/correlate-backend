# correlation_service
# add dataframe task here

import pandas as pd
import re
import numpy as np
from datetime import datetime
from datasets.models import CorrelationMetric, AggregationPeriod
from pandas.core.dtypes.dtypes import DatetimeTZDtype
from datasets.lib.date import VALID_DATE_PATTERNS


def transform_data_base(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Convert 'Date' to datetime type if it's not already
    # Floor the date to the day so that there is no time component of the time
    df["Date"] = pd.to_datetime(df["Date"])
    # Convert 'Value' to float type if it's not already
    df["Value"] = df["Value"].astype(float)  # Convert to float
    return df


def transform_metric(
    df: pd.DataFrame,
    time_increment: AggregationPeriod,
    correlation_metric: CorrelationMetric = CorrelationMetric.RAW_VALUE,
) -> pd.DataFrame:
    if df.empty:
        return df

    # Always sort to make sure that dates are sorted in ascending order
    df = df.sort_values("Date")

    if time_increment == AggregationPeriod.QUARTERLY:
        if correlation_metric == CorrelationMetric.YOY_GROWTH:
            df["Value"] = df["Value"].pct_change(periods=4)

    elif time_increment == AggregationPeriod.ANNUALLY:
        if correlation_metric == CorrelationMetric.YOY_GROWTH:
            df["Value"] = df["Value"].pct_change(periods=1)

    else:
        raise ValueError(f"Invalid Aggregation Period: {time_increment}")

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(inplace=True)
    return df


def transform_quarterly(df, fiscal_end_month):
    fiscal_month_code = {
        "December": "Q-DEC",
        "January": "Q-JAN",
        "February": "Q-FEB",
        "March": "Q-MAR",
        "April": "Q-APR",
        "May": "Q-MAY",
        "June": "Q-JUN",
        "July": "Q-JUL",
        "August": "Q-AUG",
        "September": "Q-SEP",
        "October": "Q-OCT",
        "November": "Q-NOV",
    }

    df["Date"] = pd.to_datetime(df["Date"]).dt.to_period(
        fiscal_month_code[fiscal_end_month]
    )
    return df


def transform_data(
    df: pd.DataFrame,
    time_increment: AggregationPeriod,
    fiscal_end_month=None,
    correlation_metric: CorrelationMetric = CorrelationMetric.RAW_VALUE,
    start_date: datetime | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy(deep=True)
    # Short circuit if df is empty

    # if its not a datetime type, convert it
    if not isinstance(df["Date"].dtype, DatetimeTZDtype):
        transform_data_base(df)

    if start_date is not None:
        df = df[df["Date"] >= start_date]

    # Quarterly
    if time_increment == AggregationPeriod.QUARTERLY:
        if fiscal_end_month is None:
            raise ValueError(
                "fiscal_end_month is required for Quarterly time increment"
            )

        # Make sure we have at least 3 months of data, otherwise return an empty dataframe
        if len(df) < 3:
            return pd.DataFrame()

        # Map the fiscal_end_month to its appropriate code
        fiscal_month_code = {
            "December": "Q-DEC",
            "January": "Q-JAN",
            "February": "Q-FEB",
            "March": "Q-MAR",
            "April": "Q-APR",
            "May": "Q-MAY",
            "June": "Q-JUN",
            "July": "Q-JUL",
            "August": "Q-AUG",
            "September": "Q-SEP",
            "October": "Q-OCT",
            "November": "Q-NOV",
        }

        granularity = "monthly"
        if abs(df["Date"].iloc[0].month - df["Date"].iloc[1].month) != 1:
            granularity = AggregationPeriod.QUARTERLY

        df["Date"] = df["Date"].dt.to_period(fiscal_month_code[fiscal_end_month])

        if granularity == "monthly":
            # Make sure the start and end quarters are complete
            # Remove the start months until the first quarter is complete
            while (
                df["Date"].iloc[0].month != df["Date"].iloc[1].month
                or df["Date"].iloc[1].month != df["Date"].iloc[2].month
            ):
                df = df.iloc[1:]

            # Remove the end months until the last quarter is complete
            while (
                df["Date"].iloc[-1].month != df["Date"].iloc[-2].month
                or df["Date"].iloc[-2].month != df["Date"].iloc[-3].month
            ):
                df = df.iloc[:-1]

        # Group by Fiscal_Quarter and sum the values
        df = df.groupby("Date").sum().reset_index()
        if correlation_metric == CorrelationMetric.YOY_GROWTH:
            df["Value"] = df["Value"].pct_change(periods=4)

    # Annually
    elif time_increment == AggregationPeriod.ANNUALLY:
        # Sum only the 'Value' column (or other numeric columns) after grouping by year
        df = df.groupby(df["Date"].dt.year)["Value"].sum().reset_index()
        # Reconstruct the 'Date' column to represent the first day of each year
        df["Date"] = pd.to_datetime(df["Date"].astype(str) + "-1-1", errors="coerce")
        if correlation_metric == CorrelationMetric.YOY_GROWTH:
            df["Value"] = df["Value"].pct_change(periods=1)

    else:
        raise ValueError("Invalid time_increment")

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(inplace=True)
    return df


def compute_correlations(test_df, dfs):
    correlation_results = {}
    for title, df in dfs.items():
        # Perform Pearson correlation
        correlation_value = test_df["Value"].corr(df["Value"])
        correlation_results[title] = correlation_value

    # Sort by correlation in descending order
    sorted_correlations = sorted(
        correlation_results.items(), key=lambda x: x[1], reverse=True
    )
    return sorted_correlations


def clean_input(data: str) -> str:
    data = data.replace("$", "")
    data = data.replace("%", "")
    return data


def parse_input_dataset(data: str) -> dict[str, list[str | int]] | None:
    rows = data.split("\n")
    table = list(map(lambda row: row.split(), rows))

    num_rows = len(table)

    if num_rows == 2:
        # transpose data
        table = list(np.transpose(table))

    table = list(filter(lambda row: len(row) == 2, table))

    dates = [row[0] for row in table]
    values = [clean_input(row[1]) for row in table]

    if len(dates) <= 0:
        return None

    for pattern, functors in VALID_DATE_PATTERNS.items():
        for i in range(len(dates)):
            if match := re.match(pattern, dates[i]):
                dates[i] = functors["processor"](match)

    for i in range(len(values)):
        value = values[i]
        if isinstance(value, str):
            values[i] = float(value.replace(",", ""))

    return {"Date": dates, "Value": values}
