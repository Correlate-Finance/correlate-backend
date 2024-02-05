# correlation_service
# add dataframe task here

import pandas as pd
import re


def transform_data(df, time_increment, fiscal_end_month=None):
    # Convert 'Date' to datetime type if it's not already
    # Floor the date to the day so that there is no time component of the time
    df = df.copy(deep=True)
    df["Date"] = pd.to_datetime(df["Date"])
    # Convert 'Value' to float type if it's not already
    df["Value"] = df["Value"].astype(float)  # Convert to float

    # Monthly: Do nothing
    if time_increment == "Monthly":
        return df

    # Quarterly
    elif time_increment == "Quarterly":
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
        # Group by Fiscal_Quarter and sum the values
        df = df.groupby("Date").sum().reset_index()
        return df

    # Annually
    elif time_increment == "Annually":
        # Sum only the 'Value' column (or other numeric columns) after grouping by year
        df = df.groupby(df["Date"].dt.year)["Value"].sum().reset_index()
        # Reconstruct the 'Date' column to represent the first day of each year
        df["Date"] = pd.to_datetime(df["Date"].astype(str) + "-1-1", errors="coerce")
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
