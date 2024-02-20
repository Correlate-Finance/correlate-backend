from rest_framework.views import APIView
from django.http import (
    HttpResponse,
    JsonResponse,
    HttpResponseBadRequest,
    HttpRequest,
)
import urllib.parse

from rest_framework.permissions import IsAuthenticated

from core.main_logic import calculate_correlation
from core.data_trends import (
    calculate_average_monthly_growth,
    calculate_trailing_months,
    calculate_year_over_year_growth,
    calculate_yearly_stacks,
)
from core.mongo_operations import get_all_dfs, get_df
from core import mongo_operations
from datasets.dataset_metadata import (
    augment_with_external_title,
    get_metadata_from_external_name,
)
from datasets.models import CorrelateData
import requests
import calendar
from datetime import datetime

from functools import cache
import numpy
import pandas as pd

from core.data_processing import process_data


@cache
def fetch_stock_revenues(
    stock: str, start_year: int, aggregation_period: str = "Annually"
) -> tuple[dict, str]:
    if aggregation_period == "Annually":
        url = f"https://discountingcashflows.com/api/income-statement/{stock}/"
        response = requests.get(url)

        response_json = response.json()
        report = response_json["report"]
        if len(report) == 0:
            return {}, None

        reporting_date_month = datetime.strptime(report[0]["date"], "%Y-%m-%d")
        fiscal_year_end = calendar.month_name[reporting_date_month.month]

        revenues = {}

        for i in range(len(report)):
            year = report[i]["calendarYear"]
            date = year + "-01-01"
            if year < start_year:
                continue
            revenues[date] = report[i]["revenue"]
        return revenues, fiscal_year_end
    elif aggregation_period == "Quarterly":
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

        updated_month = reporting_date_month.month + delta
        if updated_month > 12:
            updated_month = ((updated_month - 1) % 12) + 1

        fiscal_year_end = calendar.month_name[updated_month]

        revenues = {}
        for i in range(len(report)):
            year = report[i]["calendarYear"]
            date = year + report[i]["period"]
            if year < start_year:
                continue
            revenues[date] = report[i]["revenue"]
        return revenues, fiscal_year_end


class DatasetView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: HttpRequest) -> HttpResponse:
        table = request.body.decode("utf-8")
        table = urllib.parse.unquote(table)
        metadata = get_metadata_from_external_name(table)
        if metadata is not None:
            table = metadata.internal_name

        if mongo_operations.CACHED:
            df: pd.DataFrame | None = get_all_dfs().get(table)
        else:
            df: pd.DataFrame | None = get_df(table)
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


class RevenueView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: HttpRequest) -> HttpResponse:
        stock = request.GET.get("stock")
        start_year = request.GET.get("startYear", 2010)
        aggregation_period = request.GET.get("aggregationPeriod", "Annually")

        if stock is None or len(stock) < 1:
            return HttpResponseBadRequest("Pass a valid stock ticker")

        revenues, _ = fetch_stock_revenues(stock, start_year, aggregation_period)
        json_revenues = [
            {"date": date, "value": str(value)} for date, value in revenues.items()
        ]
        return JsonResponse(json_revenues, safe=False)


class CorrelateView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: HttpRequest) -> HttpResponse:
        stock = request.GET.get("stock")
        start_year = request.GET.get("start_year", 2010)
        aggregation_period = request.GET.get("aggregation_period", "Annually")
        lag_periods = int(request.GET.get("lag_periods", 0))
        high_level_only = request.GET.get("high_level_only", "false") == "true"
        show_negatives = request.GET.get("show_negatives", "false") == "true"
        correlation_metric = request.GET.get("correlation_metric", "RAW_VALUE")

        if stock is None or len(stock) < 1:
            return HttpResponseBadRequest("Pass a valid stock ticker")

        revenues, fiscal_end_month = fetch_stock_revenues(
            stock, start_year, aggregation_period
        )
        test_data = {"Date": list(revenues.keys()), "Value": list(revenues.values())}

        return run_correlations(
            time_increment=aggregation_period,
            fiscal_end_month=fiscal_end_month,
            test_data=test_data,
            lag_periods=lag_periods,
            high_level_only=high_level_only,
            show_negatives=show_negatives,
            correlation_metric=correlation_metric,
        )


class CorrelateInputDataView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: HttpRequest) -> HttpResponse:
        body = request.body
        body = body.decode("utf-8")

        rows = body.split("\n")
        table = list(map(lambda row: row.split(), rows))

        rows = len(table)

        if rows == 2:
            # transpose data
            table = numpy.transpose(table)

        dates = [row[0] for row in table]
        values = [row[1] for row in table]

        test_data = process_data({"Date": dates, "Value": values})

        aggregation_period = request.GET.get("aggregation_period", "Quarterly")
        fiscal_end_month = request.GET.get("fiscal_year_end", "December")
        lag_periods = int(request.GET.get("lag_periods", 0))
        high_level_only = request.GET.get("high_level_only", "false") == "true"
        show_negatives = request.GET.get("show_negatives", "false") == "true"

        return run_correlations(
            aggregation_period,
            fiscal_end_month,
            test_data=test_data,
            lag_periods=lag_periods,
            high_level_only=high_level_only,
            show_negatives=show_negatives,
        )


def run_correlations(
    time_increment: str,
    fiscal_end_month: str,
    test_data: dict,
    lag_periods: int,
    high_level_only: bool,
    show_negatives: bool,
    correlation_metric: str,
) -> JsonResponse:
    sorted_correlations = calculate_correlation(
        time_increment,
        fiscal_end_month,
        test_data=test_data,
        lag_periods=lag_periods,
        high_level_only=high_level_only,
        correlation_metric=correlation_metric,
    )

    if not show_negatives:
        sorted_correlations = list(
            filter(lambda x: x.pearson_value > 0, sorted_correlations)
        )

    return JsonResponse(
        CorrelateData(
            data=augment_with_external_title(sorted_correlations[:100])
        ).model_dump()
    )
