"""
This script runs pairwise correlations against all datasets and calculates clusters as well as average correlation of a dataset against other datasets.
"""

from datasets.dataset_orm import (
    get_all_dfs,
)
from core.main_logic import calculate_correlation
from datasets.models import AggregationPeriod


def calculate_pairwise_correlation():
    dfs = get_all_dfs()

    average = {}
    top_correlations = {}

    for title, df in dfs.items():
        sorted_correlations = calculate_correlation(
            AggregationPeriod.QUARTERLY, "December", dfs=dfs, test_data=df
        )
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
