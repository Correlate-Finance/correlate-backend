import pandas as pd


def calculate_trailing_months(
    data: pd.DataFrame, windows: list[int] = [3, 6, 12]
) -> pd.DataFrame:
    """Calculate the trailing three months from the dataset."""
    for window in windows:
        data[f"T{window}M"] = data["Value"].rolling(window=window).sum()
    return data


def calculate_year_over_year_growth(
    data: pd.DataFrame, windows: list[int] = [3, 6, 12]
) -> pd.DataFrame:
    """Calculate the year over year growth from the dataset."""
    data["MoMGrowth"] = data["Value"].pct_change(periods=1)
    data["YoYGrowth"] = data["Value"].pct_change(periods=12)
    for window in windows:
        data[f"T{window}M_YoYGrowth"] = data[f"T{window}M"].pct_change(periods=12)
    return data


def calculate_yearly_stacks(data: pd.DataFrame, years: int = 5) -> pd.DataFrame:
    """Calculate the yearly stack from the dataset."""

    for stack_year in range(2, years + 1):
        compound_growth = 1
        for i in range(stack_year):
            compound_growth *= 1 + data["YoYGrowth"].shift(12 * i)
        data[f"Stack{stack_year}Y"] = compound_growth ** (1 / stack_year) - 1
    return data


def calculate_average_monthly_growth(
    data: pd.DataFrame, years: int | None = None
) -> pd.DataFrame:
    """Calculate the average monthly growth from the dataset."""
    total_data = len(data.index)
    if years is not None:
        index = min(total_data, years * 12)
    else:
        index = total_data

    df = (
        data[total_data - index :]
        .groupby(data["Date"].dt.month)["MoMGrowth"]
        .mean()
        .reset_index()
    )

    data["averageMoM"] = data["Date"].dt.month.map(lambda x: df["MoMGrowth"][x - 1])
    data["DeltaSeasonality"] = data["MoMGrowth"] - data["averageMoM"]

    return data
