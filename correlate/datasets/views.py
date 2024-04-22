import json
from rest_framework.views import APIView
from django.http import (
    HttpResponse,
    JsonResponse,
    HttpResponseBadRequest,
    HttpResponseNotFound,
)
from rest_framework.request import Request
from rest_framework.response import Response
import urllib.parse
from rest_framework.permissions import IsAuthenticated
from core.main_logic import correlate_datasets, create_index
from core.data_trends import (
    calculate_average_monthly_growth,
    calculate_trailing_months,
    calculate_year_over_year_growth,
    calculate_yearly_stacks,
)
from datasets.lib import parse_year_from_date
from datasets.serializers import (
    CorrelateIndexRequestBody,
    DatasetMetadataSerializer,
    IndexSerializer,
)
from datasets.dataset_metadata_orm import (
    get_internal_name_from_external_name,
    get_metadata_from_external_name,
    get_metadata_from_name,
)
from datasets.models import CorrelateData, CorrelateDataPoint
import requests
import calendar
from datetime import datetime
from functools import cache
from django.db import transaction
import pandas as pd
from core.data_processing import (
    parse_input_dataset,
    transform_data,
    transform_metric,
    transform_quarterly,
)
from datasets.dataset_orm import get_dataset_filters, get_df
from datasets.models import (
    DatasetMetadata,
    AggregationPeriod,
    CorrelationMetric,
    CompanyMetric,
    Index,
    IndexDataset,
)
from collections import defaultdict
from django.conf import settings
from typing import List


@cache
def fetch_stock_data(
    stock: str,
    start_year: int,
    aggregation_period: AggregationPeriod = AggregationPeriod.ANNUALLY,
    end_year: int | None = None,
    company_metric: CompanyMetric = CompanyMetric.REVENUE,
) -> tuple[dict, str | None]:
    if aggregation_period == AggregationPeriod.ANNUALLY:
        url = f"https://discountingcashflows.com/api/income-statement/{stock}/"
        response = requests.get(url)

        response_json = response.json()
        report = response_json["report"]
        if len(report) == 0:
            return {}, None

        reporting_date_month = datetime.strptime(report[0]["date"], "%Y-%m-%d")
        fiscal_year_end = calendar.month_name[reporting_date_month.month]

        values = {}

        for i in range(len(report)):
            year = report[i]["calendarYear"]
            date = year + "-01-01"
            metric = report[i][company_metric]
            if int(year) < start_year:
                continue
            if end_year and int(year) > end_year:
                continue
            if metric == 0:
                continue

            values[date] = metric
        return values, fiscal_year_end
    elif aggregation_period == AggregationPeriod.QUARTERLY:
        url = f"https://discountingcashflows.com/api/income-statement/quarterly/{stock}/?key=e787734f-59d8-4809-8955-1502cb22ba36"
        response = requests.get(url)
        response_json = response.json()

        report = response_json["report"]
        if len(report) == 0:
            return {}, None

        period = report[0]["period"]
        reporting_date_month = datetime.strptime(report[0]["date"], "%Y-%m-%d")

        if period == "Q1":
            delta = 9
        elif period == "Q2":
            delta = 6
        elif period == "Q3":
            delta = 3
        else:
            delta = 0

        updated_month: int = reporting_date_month.month + delta
        if updated_month > 12:
            updated_month = ((updated_month - 1) % 12) + 1

        fiscal_year_end = calendar.month_name[updated_month]

        values = {}
        for i in range(len(report)):
            year: str = report[i]["calendarYear"]
            date: str = year + report[i]["period"]
            metric = report[i][company_metric]
            if int(year) < start_year:
                continue
            if end_year and int(year) > end_year:
                continue
            if metric == 0:
                continue
            values[date] = metric

        return values, fiscal_year_end
    else:
        raise ValueError("Invalid aggregation period")


@cache
def fetch_segment_data(
    stock: str,
    start_year: int,
    aggregation_period: AggregationPeriod = AggregationPeriod.ANNUALLY,
    end_year: int | None = None,
):
    if aggregation_period == AggregationPeriod.ANNUALLY:
        url = f"https://discountingcashflows.com/api/revenue-analysis/{stock}/product/?key=e787734f-59d8-4809-8955-1502cb22ba36"
        response = requests.get(url)
    else:
        url = f"https://discountingcashflows.com/api/revenue-analysis/{stock}/product/quarter/?key=e787734f-59d8-4809-8955-1502cb22ba36"
        response = requests.get(url)

    report = response.json()["report"]
    segments = defaultdict(dict)

    for categories in report:
        date = categories["date"]
        year = int(date.split("-")[0])
        if year < start_year or (end_year and year > end_year):
            continue
        for category in categories.keys():
            if category == "date":
                continue

            value = categories[category]
            if value == 0:
                continue

            segments[category][date] = value

    return segments


class RevenueView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        stock = request.GET.get("stock")
        start_year = int(request.GET.get("start_year", 2010))
        end_year = request.GET.get("end_year", None)
        if end_year is not None:
            end_year = int(end_year)

        aggregation_period = request.GET.get(
            "aggregation_period", AggregationPeriod.ANNUALLY
        )

        if stock is None or len(stock) < 1:
            return HttpResponseBadRequest("Pass a valid stock ticker")

        revenues, _ = fetch_stock_data(
            stock, start_year, aggregation_period, end_year=end_year
        )
        json_revenues = [
            {"date": date, "value": str(value)} for date, value in revenues.items()
        ]
        return JsonResponse(json_revenues, safe=False)


class CompanyDataView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        stock = request.GET.get("stock")
        try:
            company_metric = CompanyMetric[request.GET.get("company_metric")]
        except KeyError:
            return HttpResponseBadRequest("Invalid company metric")

        start_year = int(request.GET.get("start_year", 2010))
        end_year = request.GET.get("end_year", None)
        if end_year is not None:
            end_year = int(end_year)

        aggregation_period = request.GET.get(
            "aggregation_period", AggregationPeriod.ANNUALLY
        )

        if stock is None or len(stock) < 1:
            return HttpResponseBadRequest("Pass a valid stock ticker")

        revenues, _ = fetch_stock_data(
            stock,
            start_year,
            aggregation_period,
            end_year=end_year,
            company_metric=company_metric,
        )
        json_revenues = [
            {"date": date, "value": str(value)} for date, value in revenues.items()
        ]
        return JsonResponse(json_revenues, safe=False)


class SegmentDataView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        stock = request.GET.get("stock")
        start_year = int(request.GET.get("start_year", 2010))
        end_year = request.GET.get("end_year", None)
        if end_year is not None:
            end_year = int(end_year)

        aggregation_period = request.GET.get(
            "aggregation_period", AggregationPeriod.ANNUALLY
        )

        if stock is None or len(stock) < 1:
            return HttpResponseBadRequest("Pass a valid stock ticker")

        segments = fetch_segment_data(
            stock,
            start_year,
            aggregation_period,
            end_year=end_year,
        )

        json_segments = []
        for segment, values in segments.items():
            json_segments.append(
                {
                    "segment": segment,
                    "data": [
                        {"date": date, "value": str(value)}
                        for date, value in values.items()
                    ],
                }
            )

        return JsonResponse(json_segments, safe=False)


class DatasetView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        table = request.body.decode("utf-8")
        table = urllib.parse.unquote(table)
        metadata = get_metadata_from_external_name(table)
        if metadata is not None:
            table = metadata.internal_name

        df = get_df(table)
        if df is None:
            return HttpResponseBadRequest("Invalid data")

        df = df.copy()
        df["Date"] = pd.to_datetime(df["Date"])
        df.sort_values(by="Date", inplace=True)
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce")

        calculate_trailing_months(df)
        calculate_year_over_year_growth(df)
        calculate_yearly_stacks(df)
        calculate_average_monthly_growth(df, years=5)
        df["Date"] = df["Date"].dt.strftime("%m-%d-%Y")

        return JsonResponse(df.to_json(orient="records"), safe=False)


class DatasetMetadataView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        name = request.GET.get("name")
        metadata = get_metadata_from_name(name)
        if metadata is None:
            return HttpResponseNotFound("Metadata not found")

        return JsonResponse(
            {
                "series_id": metadata.internal_name,
                "title": metadata.external_name,
                "source": metadata.source,
                "description": metadata.description,
            },
            safe=False,
        )


class RawDatasetView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        table = request.body.decode("utf-8")
        table = urllib.parse.unquote(table)
        metadata = get_metadata_from_name(table)
        if metadata is None:
            return HttpResponseBadRequest("Invalid data")

        table = metadata.internal_name

        df = get_df(table)
        if df is None:
            return HttpResponseBadRequest("Invalid data")

        df = df.copy()
        df["Date"] = pd.to_datetime(df["Date"])
        df.sort_values(by="Date", inplace=True)
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
        df["Date"] = df["Date"].dt.strftime("%m-%d-%Y")

        return JsonResponse(
            {
                "dataset": df.to_json(orient="records"),
                "source": metadata.source,
                "description": metadata.description,
            },
            safe=False,
        )


class CorrelateView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        stock = request.GET.get("stock")
        start_year = int(request.GET.get("start_year", 2010))
        end_year = request.GET.get("end_year", None)
        if end_year is not None:
            end_year = int(end_year)

        aggregation_period = request.GET.get(
            "aggregation_period", AggregationPeriod.ANNUALLY
        )
        lag_periods = int(request.GET.get("lag_periods", 0))
        high_level_only = request.GET.get("high_level_only", "false") == "true"
        show_negatives = request.GET.get("show_negatives", "false") == "true"
        correlation_metric = request.GET.get(
            "correlation_metric", CorrelationMetric.RAW_VALUE
        )
        selected_datasets = request.GET.getlist("selected_datasets")
        selected_indexes = request.GET.getlist("selected_indexes")

        try:
            company_metric = CompanyMetric[request.GET.get("company_metric", "REVENUE")]
        except KeyError:
            return HttpResponseBadRequest("Invalid company metric")

        segment = request.GET.get("segment", None)
        if stock is None or len(stock) < 1:
            return HttpResponseBadRequest("Pass a valid stock ticker")

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
            company_metric=company_metric,
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

        if selected_indexes:
            return correlate_indexes(
                indexes=list(Index.objects.filter(id__in=selected_indexes)),
                aggregation_period=aggregation_period,
                correlation_metric=correlation_metric,
                fiscal_end_month=fiscal_end_month,
                test_df=test_df,
            )
        else:
            return run_correlations_rust(
                aggregation_period=aggregation_period,
                fiscal_end_month=fiscal_end_month,
                test_df=test_df,
                test_data=test_data,
                lag_periods=lag_periods,
                _high_level_only=high_level_only,
                _show_negatives=show_negatives,
                correlation_metric=correlation_metric,
                test_correlation_metric=correlation_metric,
                selected_datasets=selected_datasets,
            )


class CorrelateInputDataView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        body = request.body
        body = body.decode("utf-8")

        test_data = parse_input_dataset(body)
        if test_data is None:
            return HttpResponseBadRequest("Could not parse input data.")

        aggregation_period = request.GET.get(
            "aggregation_period", AggregationPeriod.QUARTERLY
        )
        fiscal_end_month = request.GET.get("fiscal_year_end", "December")
        lag_periods = int(request.GET.get("lag_periods", 0))
        high_level_only = request.GET.get("high_level_only", "false") == "true"
        show_negatives = request.GET.get("show_negatives", "false") == "true"
        correlation_metric = request.GET.get(
            "correlation_metric", CorrelationMetric.RAW_VALUE
        )
        selected_datasets = request.GET.getlist("selected_datasets")
        selected_indexes = request.GET.getlist("selected_indexes")

        dates: list[str] = test_data["Date"]  # type: ignore
        if len(dates) == 0:
            return HttpResponseBadRequest("Invalid data format")

        if "Q" in dates[0]:
            test_df = transform_metric(
                pd.DataFrame(test_data),
                aggregation_period,
                correlation_metric=correlation_metric,
            )
        else:
            test_df = transform_data(
                pd.DataFrame(test_data),
                aggregation_period,
                correlation_metric=correlation_metric,
                fiscal_end_month=fiscal_end_month,
            )

        if selected_indexes:
            return correlate_indexes(
                indexes=list(Index.objects.filter(id__in=selected_indexes)),
                aggregation_period=aggregation_period,
                correlation_metric=correlation_metric,
                fiscal_end_month=fiscal_end_month,
                test_df=test_df,
            )
        else:
            return run_correlations_rust(
                aggregation_period,
                fiscal_end_month,
                test_df=test_df,
                test_data=test_data,
                lag_periods=lag_periods,
                _high_level_only=high_level_only,
                _show_negatives=show_negatives,
                correlation_metric=correlation_metric,
                test_correlation_metric=CorrelationMetric.RAW_VALUE,
                selected_datasets=selected_datasets,
            )


class GetAllDatasetMetadata(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, _: Request) -> HttpResponse:
        metadata = DatasetMetadata.objects.filter(hidden=False)
        serialized = DatasetMetadataSerializer(metadata, many=True)
        return JsonResponse(
            serialized.data,
            safe=False,
        )


class CorrelateIndex(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        body = request.body
        body = body.decode("utf-8")
        aggregation_period = request.GET.get(
            "aggregation_period", AggregationPeriod.QUARTERLY
        )
        correlation_metric = request.GET.get(
            "correlation_metric", CorrelationMetric.RAW_VALUE
        )
        fiscal_end_month = request.GET.get("fiscal_year_end", "December")

        request_body = CorrelateIndexRequestBody(**json.loads(body))

        for i in range(len(request_body.index_datasets)):
            internal_name = get_internal_name_from_external_name(
                request_body.index_datasets[i]
            )
            request_body.index_datasets[i] = internal_name

        test_df = pd.DataFrame(
            {"Date": request_body.dates, "Value": request_body.input_data}
        )

        test_df = transform_data(
            test_df,
            aggregation_period,
            correlation_metric=correlation_metric,
            fiscal_end_month=fiscal_end_month,
        )

        index = create_index(
            dataset_weights={
                request_body.index_datasets[i]: request_body.index_percentages[i]
                for i in range(len(request_body.index_datasets))
            },
            aggregation_period=aggregation_period,
            correlation_metric=correlation_metric,
            fiscal_end_month=fiscal_end_month,
        )

        if index is None:
            return JsonResponse({"error": "No data available"})

        results = correlate_datasets(
            df=index, test_df=test_df, df_title=request_body.index_name
        )
        if results is None:
            return JsonResponse({"error": "No data available"})
        return JsonResponse(
            CorrelateData(
                data=results,
                aggregation_period=aggregation_period,
                correlation_metric=correlation_metric,
            ).model_dump()
        )


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
        print(len(indexes), index_df)
        if index_df is None:
            continue

        result = correlate_datasets(df=index_df, test_df=test_df, df_title=index.name)
        if result:
            results.extend(result)

    print(results)
    return JsonResponse(
        CorrelateData(
            data=results,
            aggregation_period=aggregation_period,
            correlation_metric=correlation_metric,
        ).model_dump()
    )


def run_correlations_rust(
    aggregation_period: AggregationPeriod,
    fiscal_end_month: str,
    test_df: pd.DataFrame,
    test_data: dict,
    lag_periods: int,
    _high_level_only: bool,
    _show_negatives: bool,
    correlation_metric: CorrelationMetric,
    selected_datasets: list[str] | None = None,
    test_correlation_metric: CorrelationMetric = CorrelationMetric.RAW_VALUE,
) -> JsonResponse:
    test_df = test_df.rename(columns={"Date": "date", "Value": "value"})
    records = test_df.to_json(orient="records", default_handler=str)
    body = {
        "manual_input_dataset": json.loads(records),
        "selected_datasets": selected_datasets or [],
    }

    start_year = parse_year_from_date(min(test_data["Date"]))
    end_year = parse_year_from_date(max(test_data["Date"]))

    if start_year is None or end_year is None:
        return JsonResponse({"error": "Invalid date format"})

    url = f"{settings.RUST_ENGINE_URL}/correlate_input?"
    request_paramters = {
        "aggregation_period": aggregation_period,
        "fiscal_year_end": datetime.strptime(fiscal_end_month, "%B").month,
        "lag_periods": lag_periods,
        "correlation_metric": correlation_metric,
        "start_year": start_year,
        "end_year": end_year,
    }

    query_string = urllib.parse.urlencode(request_paramters)

    response = requests.post(url + query_string, data=json.dumps(body))

    return JsonResponse(response.json())


class SaveIndexView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        user = request.user
        datasets: List[dict] = request.data.get("datasets", [])  # type: ignore
        name: str = request.data.get("index_name", "")  # type: ignore
        index_id: int = request.data.get("index_id", None)  # type: ignore

        if name is None or len(name) == 0:
            return Response({"message": "Index name cannot be empty"}, status=400)

        aggregation_period: str | None = request.data.get("aggregation_period", None)  # type: ignore
        correlation_metric: str | None = request.data.get("correlation_metric", None)  # type: ignore

        try:
            parsed_datasets = [
                (dataset["title"], dataset["percentage"]) for dataset in datasets
            ]
        except KeyError:
            return Response({"message": "Invalid dataset format"}, status=400)

        total_weight = sum([float(dataset[1]) for dataset in parsed_datasets])
        if total_weight > 1 or total_weight < 0.99:
            return Response({"message": "Total weight must be between 1"}, status=400)

        # Make sure all db actions happen atomically
        with transaction.atomic():
            if index_id:
                index = Index.objects.get(id=index_id)
                index.name = name
                index.aggregation_period = aggregation_period
                index.correlation_metric = correlation_metric
                index.save()
                IndexDataset.objects.filter(index=index).delete()
            else:
                index = Index.objects.create(
                    name=name,
                    user=user,
                    aggregation_period=aggregation_period,
                    correlation_metric=correlation_metric,
                )

            IndexDataset.objects.bulk_create(
                [
                    IndexDataset(
                        dataset=get_metadata_from_name(dataset[0]),
                        weight=dataset[1],
                        index=index,
                    )
                    for dataset in parsed_datasets
                ]
            )

        return Response({"message": "Index saved"})


class GetIndicesView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        user = request.user
        indices = Index.objects.filter(user=user)
        index_serializer = IndexSerializer(indices, many=True)
        return Response(index_serializer.data)


class GetDatasetFilters(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, _: Request) -> HttpResponse:
        filters = get_dataset_filters()
        return JsonResponse(filters)
