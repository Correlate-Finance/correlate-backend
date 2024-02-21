# make this the api route file

from core.data_processing import transform_data
from core.mongo_operations import get_all_dfs, HIGH_LEVEL_TABLES
import pandas as pd
from core.data import TEST_DATA
import math

from datasets.models import CorrelateDataPoint
from scipy.stats.stats import pearsonr


def calculate_correlation(
    time_increment: str,
    fiscal_end_month: str,
    test_data: dict | pd.DataFrame = None,
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
    with pd.option_context("mode.use_inf_as_na", True):
        test_df.dropna(inplace=True)

    transformed_dfs: dict[str, pd.DataFrame] = {}
    # Apply the transformation on every dataframe in dfs.
    for title, df in dfs.items():
        transformed_dfs[title] = transform_data(
            df, time_increment, fiscal_end_month, correlation_metric
        )
        with pd.option_context("mode.use_inf_as_na", True):
            transformed_dfs[title].dropna(inplace=True)

    dfs = transformed_dfs

    correlation_results: list[CorrelateDataPoint] = []

    for title, df in dfs.items():
        merged = pd.merge(df, test_df, on="Date")
        test_points = list(merged["Value_y"])
        dataset_points = list(merged["Value_x"])
        dates = list(merged["Date"].astype(str))

        if len(merged.index) < 4:
            continue

        for lag in range(lag_periods + 1):
            try:
                correlation_value, p_value = pearsonr(
                    test_points[lag:], dataset_points[: len(dataset_points) - lag]
                )
            except Exception as e:
                print("Error in correlation", title, merged)
                raise e

            if math.isnan(correlation_value):
                continue

            correlation_results.append(
                CorrelateDataPoint(
                    title=title,
                    pearson_value=correlation_value,
                    lag=lag,
                    p_value=p_value,
                    input_data=test_points,
                    dataset_data=dataset_points,
                    dates=dates,
                )
            )

    # Sort by correlation in descending order
    sorted_correlations = sorted(
        correlation_results, key=lambda x: abs(x.pearson_value), reverse=True
    )

    return sorted_correlations
