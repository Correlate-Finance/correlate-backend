# correlation_service
# add dataframe task here

import pandas as pd
import re


def transform_data(
    df: pd.DataFrame,
    time_increment,
    fiscal_end_month=None,
    correlation_metric="RAW_VALUE",
) -> pd.DataFrame:
    df = df.copy(deep=True)
    # Short circuit if df is empty
    if df.empty:
        return df

    # Convert 'Date' to datetime type if it's not already
    # Floor the date to the day so that there is no time component of the time
    df["Date"] = pd.to_datetime(df["Date"])
    # Convert 'Value' to float type if it's not already
    df["Value"] = df["Value"].astype(float)  # Convert to float

    # Monthly: Do nothing
    if time_increment == "Monthly":
        if correlation_metric == "YOY_GROWTH":
            df["Value"] = df["Value"].pct_change(periods=12)
        return df

    # Quarterly
    elif time_increment == "Quarterly":
        if fiscal_end_month is None:
            raise ValueError(
                "fiscal_end_month is required for Quarterly time increment"
            )
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

        df["Date"] = df["Date"].dt.to_period(fiscal_month_code[fiscal_end_month])

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
        if correlation_metric == "YOY_GROWTH":
            df["Value"] = df["Value"].pct_change(periods=4)
        return df

    # Annually
    elif time_increment == "Annually":
        # Sum only the 'Value' column (or other numeric columns) after grouping by year
        df = df.groupby(df["Date"].dt.year)["Value"].sum().reset_index()
        # Reconstruct the 'Date' column to represent the first day of each year
        df["Date"] = pd.to_datetime(df["Date"].astype(str) + "-1-1", errors="coerce")
        if correlation_metric == "YOY_GROWTH":
            df["Value"] = df["Value"].pct_change(periods=1)
        return df
    else:
        raise ValueError("Invalid time_increment")


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


def convert_to_four_digit_year(year: str, max_year: int = 2029):
    if int(year) < (max_year % 100):
        return "20" + year
    else:
        return "19" + year


def process_data(data):
    valid_date_patterns = {
        # 2014Q1 -> Accepted pattern, do nothing
        r"^(\d{4})Q([1-4])": lambda x: x,
        r"^Q([1-4])\'(\d{2})$": lambda match: convert_to_four_digit_year(match.group(2))
        + "Q"
        + match.group(1),
    }

    dates = data["Date"]

    if len(dates) <= 0:
        return data

    for pattern, processor in valid_date_patterns.items():
        for i in range(len(dates)):
            if match := re.match(pattern, dates[i]):
                dates[i] = processor(match)

    values: list[int | str] = data["Value"]
    for i in range(len(values)):
        if isinstance(values[i], str):
            values[i] = int(values[i].replace(",", ""))

    data["Date"] = dates
    data["Value"] = values

    return data
