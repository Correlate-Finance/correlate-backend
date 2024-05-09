import pandas as pd
from core.data_processing import (
    transform_data,
    transform_metric,
    transform_quarterly,
)
from core.main_logic import correlate_datasets, create_index
from datasets.models import AggregationPeriod, CorrelationMetric
from datasets.models import CorrelateData
from datasets.models import Index
from django.http import HttpResponse, JsonResponse, HttpResponseBadRequest
from users.models import User
from adapters.discounting_cash_flows import fetch_stock_data, fetch_segment_data
from datasets.orm.correlation_parameters_orm import insert_automatic_correlation
import json
import requests
import urllib.parse
from datetime import datetime
from datasets.models import CorrelateDataPoint, CorrelationParameters, IndexDataset
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


def correlate_indexes(
    indexes: list[Index],
    aggregation_period: AggregationPeriod,
    correlation_metric: CorrelationMetric,
    fiscal_end_month: str,
    test_df: pd.DataFrame,
) -> HttpResponse:
    results: list[CorrelateDataPoint] = []
    if aggregation_period == AggregationPeriod.QUARTERLY:
        test_df = transform_quarterly(test_df, fiscal_end_month)
    for index in indexes:
        index_df = create_index(
            dataset_weights={
                index_dataset.dataset.internal_name: index_dataset.weight
                for index_dataset in IndexDataset.objects.filter(
                    index=index
                ).prefetch_related("dataset")
            },
            aggregation_period=aggregation_period,
            correlation_metric=correlation_metric,
            fiscal_end_month=fiscal_end_month,
        )
        if index_df is None:
            continue

        result = correlate_datasets(df=index_df, test_df=test_df, df_title=index.name)
        if result:
            results.extend(result)

    return JsonResponse(
        CorrelateData(
            data=results,
            aggregation_period=aggregation_period,
            correlation_metric=correlation_metric,
        ).model_dump()
    )


def generate_stock_correlations(
    stock: str,
    user: User | int,
    aggregation_period: AggregationPeriod,
    correlation_metric: CorrelationMetric,
    lag_periods: int,
    start_year: int,
    end_year: int,
    segment: str | None = None,
    selected_indexes: list[int] | None = None,
    selected_datasets: list[str] | None = None,
) -> HttpResponse:
    segment_data = None
    if segment is not None:
        segment_data = fetch_segment_data(
            stock,
            start_year,
            aggregation_period,
            end_year=end_year,
        )[segment]

    revenues, fiscal_end_month = fetch_stock_data(
        stock,
        start_year,
        aggregation_period,
        end_year=end_year,
    )
    if fiscal_end_month is None:
        return JsonResponse(
            CorrelateData(
                data=[],
                aggregation_period=aggregation_period,
                correlation_metric=correlation_metric,
            ).model_dump()
        )

    if segment_data is not None:
        test_data = {
            "Date": list(segment_data.keys()),
            "Value": list(segment_data.values()),
        }
        test_df = transform_data(
            pd.DataFrame(test_data),
            aggregation_period,
            correlation_metric=correlation_metric,
            fiscal_end_month=fiscal_end_month,
        )
    elif revenues is not None:
        test_data = {
            "Date": list(revenues.keys()),
            "Value": list(revenues.values()),
        }
        test_df = transform_metric(
            pd.DataFrame(test_data),
            aggregation_period,
            correlation_metric=correlation_metric,
        )
    else:
        return HttpResponseBadRequest("No data available")

    if selected_indexes:
        return correlate_indexes(
            indexes=list(Index.objects.filter(id__in=selected_indexes)),
            aggregation_period=aggregation_period,
            correlation_metric=correlation_metric,
            fiscal_end_month=fiscal_end_month,
            test_df=test_df,
        )
    else:
        correlation_parameters = insert_automatic_correlation(
            user=user,
            stock_ticker=stock,
            start_year=start_year,
            end_year=end_year,
            aggregation_period=aggregation_period,
            correlation_metric=correlation_metric,
            lag_periods=lag_periods,
            fiscal_year_end=fiscal_end_month,
            company_metric=segment,
        )

        return run_correlations_rust(
            correlation_parameters=correlation_parameters,
            test_df=test_df,
            selected_datasets=selected_datasets,
        )


def generate_manul_input_correlations():
    pass
