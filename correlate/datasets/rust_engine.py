import pandas as pd
import json
import requests
import urllib.parse
from datetime import datetime
from datasets.models import CorrelationParameters
from django.http import JsonResponse
from django.conf import settings


def run_correlations_rust(
    correlation_parameters: CorrelationParameters,
    test_df: pd.DataFrame,
    selected_datasets: list[str] | None = None,
) -> JsonResponse:
    test_df = test_df.rename(columns={"Date": "date", "Value": "value"})
    records = test_df.to_json(orient="records", default_handler=str)
    if records is None:
        return JsonResponse({"error": "Invalid data format"})
    body = {
        "manual_input_dataset": json.loads(records),
        "selected_datasets": selected_datasets or [],
    }

    start_year = correlation_parameters.start_year
    end_year = correlation_parameters.end_year

    if start_year is None or end_year is None:
        return JsonResponse({"error": "Invalid date format"})

    url = f"{settings.RUST_ENGINE_URL}/correlate_input?"

    fiscal_end_month = correlation_parameters.fiscal_year_end.value
    request_paramters = {
        "aggregation_period": correlation_parameters.aggregation_period.value,
        "fiscal_year_end": datetime.strptime(fiscal_end_month, "%B").month,
        "lag_periods": correlation_parameters.lag_periods,
        "correlation_metric": correlation_parameters.correlation_metric.value,
        "start_year": start_year,
        "end_year": end_year,
    }

    query_string = urllib.parse.urlencode(request_paramters)

    response = requests.post(url + query_string, data=json.dumps(body))
    json_response = response.json()
    # Add the id for the correlation parameters to the response
    json_response["correlation_parameters_id"] = correlation_parameters.id
    return JsonResponse(json_response)
