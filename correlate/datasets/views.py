import json
from rest_framework.views import APIView
from django.http import (
    HttpResponse,
    JsonResponse,
    HttpResponseBadRequest,
    HttpResponseNotFound,
)
from rest_framework.request import Request
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
from datasets.serializers import CorrelateIndexRequestBody
from datasets.dataset_metadata_orm import (
    get_internal_name_from_external_name,
    get_metadata_from_external_name,
    get_metadata_from_name,
)
from datasets.models import CorrelateData
import requests
import calendar
from datetime import datetime
from functools import cache
import pandas as pd
from core.data_processing import (
    parse_input_dataset,
    transform_data,
    transform_data_base,
)
from datasets.dataset_orm import get_df
from datasets.models import DatasetMetadata
from datasets.models import AggregationPeriod, CorrelationMetric, CompanyMetric
from ddtrace import tracer


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
        try:
            company_metric = CompanyMetric[request.GET.get("company_metric", "REVENUE")]
        except KeyError:
            return HttpResponseBadRequest("Invalid company metric")

        if stock is None or len(stock) < 1:
            return HttpResponseBadRequest("Pass a valid stock ticker")

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
                    aggregationPeriod=aggregation_period,
                    correlationMetric=correlation_metric,
                ).model_dump()
            )
        test_data = {"Date": list(revenues.keys()), "Value": list(revenues.values())}

        return run_correlations_rust(
            aggregation_period=aggregation_period,
            fiscal_end_month=fiscal_end_month,
            test_data=test_data,
            lag_periods=lag_periods,
            _high_level_only=high_level_only,
            _show_negatives=show_negatives,
            correlation_metric=correlation_metric,
            test_correlation_metric=correlation_metric,
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

        return run_correlations_rust(
            aggregation_period,
            fiscal_end_month,
            test_data=test_data,
            lag_periods=lag_periods,
            _high_level_only=high_level_only,
            _show_negatives=show_negatives,
            correlation_metric=correlation_metric,
            test_correlation_metric=CorrelationMetric.RAW_VALUE,
        )


class GetAllDatasetMetadata(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        metadata = DatasetMetadata.objects.all()
        return JsonResponse(
            [
                {"series_id": m.internal_name, "title": m.external_name}
                for m in metadata
            ],
            safe=False,
        )


class CorrelateIndex(APIView):
    # permission_classes = (IsAuthenticated,)

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
                aggregationPeriod=aggregation_period,
                correlationMetric=correlation_metric,
            ).model_dump()
        )


@tracer.wrap("run_correlations_rust")
def run_correlations_rust(
    aggregation_period: AggregationPeriod,
    fiscal_end_month: str,
    test_data: dict,
    lag_periods: int,
    _high_level_only: bool,
    _show_negatives: bool,
    correlation_metric: CorrelationMetric,
    test_correlation_metric: CorrelationMetric = CorrelationMetric.RAW_VALUE,
) -> JsonResponse:
    test_df = pd.DataFrame(test_data)
    transform_data_base(test_df)
    test_df = transform_data(
        test_df, aggregation_period, fiscal_end_month, test_correlation_metric
    )

    test_df = test_df.rename(columns={"Date": "date", "Value": "value"})
    records = test_df.to_json(orient="records", default_handler=str)

    start_year = parse_year_from_date(min(test_data["Date"]))
    end_year = parse_year_from_date(max(test_data["Date"]))

    if start_year is None or end_year is None:
        return JsonResponse({"error": "Invalid date format"})

    url = "https://api2.correlatefinance.com/correlate_input?"
    request_paramters = {
        "aggregation_period": aggregation_period,
        "fiscal_year_end": datetime.strptime(fiscal_end_month, "%B").month,
        "lag_periods": lag_periods,
        "correlation_metric": correlation_metric,
        "start_year": start_year,
        "end_year": end_year,
    }

    query_string = urllib.parse.urlencode(request_paramters)

    response = requests.post(url + query_string, data=records)

    return JsonResponse(response.json())
