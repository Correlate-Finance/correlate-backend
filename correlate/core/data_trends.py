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
    data["YoYGrowth"] = data["Value"].pct_change(periods=12)
    for window in windows:
        data[f"T{window}M_YoYGrowth"] = data[f"T{window}M"].pct_change(periods=12)
    return data


def calculate_yearly_stacks(data: pd.DataFrame) -> pd.DataFrame:
    """Calculate the yearly stack from the dataset."""
    data["Stack2Y"] = (
        ((1 + data["YoYGrowth"]) * (1 + data["YoYGrowth"].shift(12))) ** 0.5
    ) - 1
    data["Stack3Y"] = (
        (
            (1 + data["YoYGrowth"])
            * (1 + data["YoYGrowth"].shift(12))
            * (1 + data["YoYGrowth"].shift(24))
        )
        ** (1 / 3)
    ) - 1
    return data
