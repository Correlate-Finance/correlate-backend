# make this the api route file

from core.data_processing import transform_data
from core.mongo_operations import get_all_dfs, HIGH_LEVEL_TABLES
import pandas as pd
from core.data import TEST_DATA
import math

from datasets.models import CorrelateDataPoint
from scipy.stats.stats import pearsonr  # type: ignore
import numpy as np


def correlate_datasets(
    test_df: pd.DataFrame,
    df: pd.DataFrame,
    df_title: str,
    lag_periods: int = 0,
):
    """Correlate two datasets."""
    merged = pd.merge(df, test_df, on="Date")
    test_points = list(merged["Value_y"])
    dataset_points = list(merged["Value_x"])
    dates = list(merged["Date"].astype(str))

    if len(merged.index) < 4:
        return None

    results = []
    for lag in range(lag_periods + 1):
        try:
            correlation_value, p_value = pearsonr(
                test_points[lag:], dataset_points[: len(dataset_points) - lag]
            )  # type: ignore
        except Exception as e:
            print("Error in correlation", df_title, merged)
            raise e

        if math.isnan(correlation_value):
            continue

        results.append(
            CorrelateDataPoint(
                title=df_title,
                pearson_value=correlation_value,
                lag=lag,
                p_value=p_value,
                input_data=test_points,
                dataset_data=dataset_points,
                dates=dates,
            )
        )

    return results


def calculate_correlation(
    time_increment: str,
    fiscal_end_month: str,
    test_data: dict | pd.DataFrame | None = None,
    lag_periods: int = 0,
    high_level_only: bool = False,
    correlation_metric: str = "RAW_VALUE",
):
    if test_data is None:
        test_data = TEST_DATA

    # Create a DataFrame
    if isinstance(test_data, dict):
        test_df = pd.DataFrame(test_data)
    else:
        test_df = test_data

    dfs = get_all_dfs(selected_names=HIGH_LEVEL_TABLES if high_level_only else None)

    # Apply the transformation on test_data. make this a single helper method with the job below (sanitize and transform)
    test_df = transform_data(
        test_df, time_increment, fiscal_end_month, correlation_metric
    )

    transformed_dfs: dict[str, pd.DataFrame] = {}
    # Apply the transformation on every dataframe in dfs.
    for title, df in dfs.items():
        transformed_dfs[title] = transform_data(
            df, time_increment, fiscal_end_month, correlation_metric
        )

    dfs = transformed_dfs

    correlation_results: list[CorrelateDataPoint] = []

    for title, df in dfs.items():
        results = correlate_datasets(test_df, df, title, lag_periods)
        if results is not None:
            correlation_results.extend(results)

    # Sort by correlation in descending order
    sorted_correlations = sorted(
        correlation_results, key=lambda x: abs(x.pearson_value), reverse=True
    )

    return sorted_correlations


def create_index(
    dataset_weights: dict[str, float],
    correlation_metric: str,
    aggregation_period: str = "Quarterly",
    fiscal_end_month: str = "December",
) -> pd.DataFrame | None:
    if len(dataset_weights) == 0:
        return None
    dfs = get_all_dfs(selected_names=list(dataset_weights.keys()))

    if len(dfs) == 0:
        return None

    df_items = list(dfs.items())
    dfs_to_concat = []
    for title, df in df_items:
        transformed_df = transform_data(
            df, aggregation_period, fiscal_end_month, correlation_metric
        )

        if correlation_metric == "RAW_VALUE":
            transformed_df["Value"] = (
                transformed_df["Value"] / transformed_df["Value"].abs().max()
            )

        transformed_df["Value"] = transformed_df["Value"] * dataset_weights[title]
        dfs_to_concat.append(transformed_df)

    index = pd.concat(dfs_to_concat, join="inner").groupby("Date").sum()

    return index
