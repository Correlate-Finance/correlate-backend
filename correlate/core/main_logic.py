# make this the api route file

from core.data_processing import transform_data, compute_correlations
from core.mongo_operations import (
    connect_to_mongo,
    fetch_category_names,
    fetch_data_table_ids,
    fetch_data_frames,
)
import pandas as pd
import time
from core.data import TEST_DATA
import math


def calculate_correlation(
    time_increment, fiscal_end_month, test_data=None, selected_name=None
):
    if test_data is None:
        test_data = TEST_DATA

    # Create a DataFrame
    test_df = pd.DataFrame(test_data)

    mongo_uri = "mongodb+srv://cmd2:VXSkRSG3kbRLIoJd@cluster0.fgu6ofc.mongodb.net/?retryWrites=true&w=majority"
    database_name = "test"

    db = connect_to_mongo(mongo_uri, database_name)

    dataTable_ids = fetch_data_table_ids(db, selected_name)
    dfs = fetch_data_frames(db, dataTable_ids)
    db.client.close()

    # Apply the transformation on test_data. make this a single helper method with the job below (sanitize and transform)
    test_df = transform_data(test_df, time_increment, fiscal_end_month)

    transformed_dfs = {}
    # Apply the transformation on every dataframe in dfs.
    for title, df in dfs.items():
        transformed_dfs[title] = transform_data(df, time_increment, fiscal_end_month)
    dfs = transformed_dfs
    
    correlation_results = {}

    start_time = time.time()
    for title, df in dfs.items():
        # Merge the data so that the dates are aligned
        merged = pd.merge(df, test_df, on="Date")
        if merged.size < 4:
            continue
        correlation_value = merged["Value_x"].corr(merged["Value_y"])

        if math.isnan(correlation_value):
            continue

        correlation_results[title] = correlation_value
        # for lag in range(3):
        #     correlation_results[title + " lagged: " + str(lag + 1)] = merged["Value_x"][
        #         lag + 1 :
        #     ].corr(merged["Value_y"][: -1 * (lag + 1)])

    # Sort by correlation in descending order
    sorted_correlations = sorted(
        correlation_results.items(), key=lambda x: math.abs(x[1]), reverse=True
    )

    print("Computation time", time.time() - start_time)

    # Print the sorted results
    for title, corr_value in sorted_correlations:
        print(f"Table Name: {title}, Correlation: {corr_value:.2f}")

    return sorted_correlations