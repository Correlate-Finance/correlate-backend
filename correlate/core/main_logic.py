# make this the api route file

from core.data_processing import transform_data, transform_data_base
from datasets.dataset_orm import get_all_dfs
import pandas as pd
from core.data import TEST_DATA
import math

from datasets.models import CorrelateDataPoint, AggregationPeriod, CorrelationMetric
import numpy as np
from frozendict import frozendict
from ddtrace import tracer


@tracer.wrap("main_logic.correlate_datasets")
def correlate_datasets(
    test_df: pd.DataFrame,
    df: pd.DataFrame,
    df_title: str,
    lag_periods: int = 0,
) -> list[CorrelateDataPoint] | None:
    """Correlate two datasets."""
    merged = pd.merge(df, test_df, on="Date")
    test_points = merged["Value_y"].values
    dataset_points = merged["Value_x"].values

    # Convert to list once so we don't do it in the loop
    test_points_list = list(test_points)
    dataset_points_list = list(dataset_points)

    dates = list(merged["Date"].astype(str))

    if len(merged.index) < 4:
        return None

    results = []
    n = len(dataset_points)
    for lag in range(lag_periods + 1):
        try:
            correlation_value = np.corrcoef(
                test_points[lag:], dataset_points[: n - lag]
            )[0, 1]

        except Exception as e:
            print("Error in correlation", df_title, merged)
            raise e

        if math.isnan(correlation_value):
            continue

        results.append(
            CorrelateDataPoint(
                title=df_title,
                internal_name=df_title,
                pearson_value=correlation_value,
                lag=lag,
                input_data=test_points_list,
                dataset_data=dataset_points_list,
                dates=dates,
            )
        )

    return results


@tracer.wrap("main_logic.calculate_correlation")
def calculate_correlation(
    time_increment: AggregationPeriod,
    fiscal_end_month: str,
    dfs: frozendict[str, pd.DataFrame] | dict[str, pd.DataFrame],
    test_data: dict | pd.DataFrame | None = None,
    lag_periods: int = 0,
    test_correlation_metric: CorrelationMetric = CorrelationMetric.RAW_VALUE,
    correlation_metric: CorrelationMetric = CorrelationMetric.RAW_VALUE,
) -> list[CorrelateDataPoint]:
    if test_data is None:
        test_data = TEST_DATA

    # Create a DataFrame
    if isinstance(test_data, dict):
        test_df = pd.DataFrame(test_data)
    else:
        test_df = test_data

    transform_data_base(test_df)
    test_df = transform_data(
        test_df, time_increment, fiscal_end_month, test_correlation_metric
    )

    # start_time = test_df["Date"].iloc[0]
    # timestamp: pd.Period = start_time.to_timestamp()
    # start_datetime = datetime(timestamp.year, timestamp.month, timestamp.day)

    transformed_dfs: dict[str, pd.DataFrame] = {}
    # Apply the transformation on every dataframe in dfs.
    for title, df in dfs.items():
        transformed_dfs[title] = transform_data(
            df,
            time_increment,
            fiscal_end_month,
            correlation_metric,
        )

    correlation_results: list[CorrelateDataPoint] = []

    for title, df in transformed_dfs.items():
        results = correlate_datasets(test_df, df, title, lag_periods)
        if results is not None:
            correlation_results.extend(results)

    # Sort by correlation in descending order
    sorted_correlations = sorted(
        correlation_results, key=lambda x: abs(x.pearson_value), reverse=True
    )

    return sorted_correlations


@tracer.wrap("main_logic.create_index")
def create_index(
    dataset_weights: dict[str, float],
    correlation_metric: CorrelationMetric,
    aggregation_period: AggregationPeriod = AggregationPeriod.QUARTERLY,
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

        if correlation_metric == CorrelationMetric.RAW_VALUE:
            transformed_df["Value"] = (
                transformed_df["Value"] / transformed_df["Value"].abs().max()
            )

        transformed_df["Value"] = transformed_df["Value"] * dataset_weights[title]
        dfs_to_concat.append(transformed_df)

    return pd.concat(dfs_to_concat, join="inner").groupby("Date").sum()
