"""
This script runs pairwise correlations against all datasets and calculates clusters as well as average correlation of a dataset against other datasets.
"""

from datasets.dataset_orm import (
    get_all_dfs,
)
from core.main_logic import calculate_correlation
from datasets.models import AggregationPeriod
import time


def calculate_pairwise_correlation():
    dfs = get_all_dfs()

    average = {}
    top_correlations = {}

    f = open("pairwise_correlation.csv", "w")

    for title, df in dfs.items():
        start_time = time.time()
        sorted_correlations = calculate_correlation(
            AggregationPeriod.QUARTERLY, "December", dfs=dfs, test_data=df
        )
        correlations = [dp.pearson_value for dp in sorted_correlations]
        if len(correlations) == 0:
            print(title, "no correlations")
            continue
        average[title] = sum(correlations) / len(correlations)
        top_correlation = [
            (dp.title, dp.pearson_value)
            for dp in sorted_correlations
            if dp.pearson_value > 0.97 and dp.title != title
        ]
        if top_correlation:
            top_correlations[title] = top_correlation
            f.write(f"{title},{",".join([str(t) for t in top_correlation])}\n")
            f.flush()
            print(f"{title},{",".join([str(t) for t in top_correlation])}\n")

        print(
            f"Finished {title}. {len(average)/len(dfs) * 100:.2f} % completed in {time.time() - start_time:.2f} seconds."
        )

    print(average)
    print(top_correlations)
