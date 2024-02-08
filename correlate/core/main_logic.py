# make this the api route file

from core.data_processing import transform_data
from core.mongo_operations import (
    get_all_dfs,
)
import pandas as pd
import time
from core.data import TEST_DATA
import math

from correlate.models import CorrelateDataPoint
from scipy.stats.stats import pearsonr


def calculate_correlation(
    time_increment: str,
    fiscal_end_month: str,
    test_data: dict | pd.DataFrame = None,
    selected_name=None,
    lag_periods: int = 0,
):
    if test_data is None:
        test_data = TEST_DATA

    # Create a DataFrame
    if isinstance(test_data, dict):
        test_df = pd.DataFrame(test_data)
    else:
        test_df = test_data

    dfs = get_all_dfs()

    # Apply the transformation on test_data. make this a single helper method with the job below (sanitize and transform)
    test_df = transform_data(test_df, time_increment, fiscal_end_month)

    transformed_dfs: dict[str, pd.DataFrame] = {}
    # Apply the transformation on every dataframe in dfs.
    for title, df in dfs.items():
        transformed_dfs[title] = transform_data(df, time_increment, fiscal_end_month)
    dfs = transformed_dfs

    correlation_results: list[CorrelateDataPoint] = []

    start_time = time.time()
    for title, df in dfs.items():
        # if title != "fast total personnel absolute":
        #     continue
        # Merge the data so that the dates are aligned
        merged = pd.merge(df, test_df, on="Date")
        test_points = list(merged["Value_y"])
        dataset_points = list(merged["Value_x"])
        dates = list(merged["Date"].astype(str))

        if len(merged.index) < 4:
            continue

        for lag in range(lag_periods + 1):
            correlation_value, p_value = pearsonr(
                test_points[lag:], dataset_points[: len(dataset_points) - lag]
            )
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

    print("Computation time", time.time() - start_time)
    return sorted_correlations
