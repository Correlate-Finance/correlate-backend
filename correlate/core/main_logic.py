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

    transformed_dfs = {}
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

        if len(merged.index) < 4:
            continue

        correlation_value = merged["Value_x"].corr(merged["Value_y"])
        if math.isnan(correlation_value):
            continue

        correlation_results.append(
            CorrelateDataPoint(title=title, pearson_value=correlation_value, lag=0)
        )

        if lag_periods > 0:
            for lag in range(lag_periods):
                print(merged["Value_x"][: -1 * (lag + 1)])
                print()
                correlation_value = merged["Value_y"][lag + 1 :].corr(
                    merged["Value_x"][: -1 * (lag + 1)]
                )
                if math.isnan(correlation_value):
                    continue

                correlation_results.append(
                    CorrelateDataPoint(
                        title=title, pearson_value=correlation_value, lag=lag + 1
                    )
                )

    # Sort by correlation in descending order
    sorted_correlations = sorted(
        correlation_results, key=lambda x: abs(x.pearson_value), reverse=True
    )

    print("Computation time", time.time() - start_time)
    return sorted_correlations
