"""
This script runs pairwise correlations against all datasets and calculates clusters as well as average correlation of a dataset against other datasets.
"""

from core.data_processing import transform_data, compute_correlations
from core.mongo_operations import (
    connect_to_mongo,
    fetch_category_names,
    fetch_data_table_ids,
    fetch_data_frames,
    get_all_dfs,
)
import pandas as pd
import time
from core.data import TEST_DATA
import math
from core.main_logic import calculate_correlation

from correlate.models import CorrelateDataPoint


def calculate_pairwise_correlation():
    dfs = get_all_dfs()

    average = {}
    top_correlations = {}

    for title, df in dfs.items():
        sorted_correlations = calculate_correlation("Quarterly", "December", df)
        correlations = [dp.pearson_value for dp in sorted_correlations]
        if len(correlations) == 0:
            print(title, "no correlations")
            continue
        average[title] = sum(correlations) / len(correlations)
        top_correlations[title] = [
            dp for dp in sorted_correlations if dp.pearson_value > 0.9
        ]
        print(f"Finished {title}. {len(average)/len(dfs) * 100:.2f} % completed")

    print(average)
    print(top_correlations)
