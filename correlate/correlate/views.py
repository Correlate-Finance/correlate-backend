from rest_framework.views import APIView
from django.http import (
    HttpResponse,
    JsonResponse,
    HttpResponseBadRequest,
    HttpRequest,
    HttpResponseNotFound,
)
from rest_framework.permissions import IsAuthenticated

from core.main_logic import calculate_correlation
from core.mongo_operations import get_df
from correlate.models import CorrelateData
import requests
import calendar
from datetime import datetime

from functools import cache
import numpy

from core.data_processing import process_data, transform_data


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
        print("fiscal year end: ", fiscal_year_end)

        revenues = {}
        for i in range(len(report)):
            year = report[i]["calendarYear"]
            date = year + report[i]["period"]
            if year < start_year:
                continue
            revenues[date] = report[i]["revenue"]
        return revenues, fiscal_year_end


class RevenueView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: HttpRequest) -> HttpResponse:
        stock = request.GET.get("stock")
        start_year = request.GET.get("startYear", 2010)
        aggregation_period = request.GET.get("aggregationPeriod", "Annually")

        if stock is None or len(stock) < 2:
            return HttpResponseBadRequest("Pass a valid stock ticker")

        revenues, _ = fetch_stock_revenues(stock, start_year, aggregation_period)
        json_revenues = [
            {"date": date, "value": str(value)} for date, value in revenues.items()
        ]
        return JsonResponse(json_revenues, safe=False)


class DatasetView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: HttpRequest) -> HttpResponse:
        body = request.body
        table = body.decode("utf-8")

        time_increment = request.GET.get("time_increment", "Quarterly")
        fiscal_end_month = request.GET.get("fiscal_year_end", "December")

        print(table)
        if table is None or table == "":
            return HttpResponseBadRequest("Invalid table name")

        df = get_df(table)
        if df is None:
            return HttpResponseNotFound(f"No table with name {table} found")

        transformed_df = transform_data(df, time_increment, fiscal_end_month)
        transformed_df_json = {
            "date": list(transformed_df["Date"].to_string()),
            "value": list(transformed_df["Value"]),
        }
        print(transformed_df_json)

        return JsonResponse(transformed_df_json, safe=False)


class CorrelateView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: HttpRequest) -> HttpResponse:
        stock = request.GET.get("stock")
        start_year = request.GET.get("startYear", 2010)
        aggregation_period = request.GET.get("aggregationPeriod", "Annually")
        lag_periods = int(request.GET.get("lag_periods", 0))

        if stock is None or len(stock) < 2:
            return HttpResponseBadRequest("Pass a valid stock ticker")

        revenues, fiscal_end_month = fetch_stock_revenues(
            stock, start_year, aggregation_period
        )
        test_data = {"Date": list(revenues.keys()), "Value": list(revenues.values())}

        print("test_data", test_data)

        sorted_correlations = calculate_correlation(
            aggregation_period,
            fiscal_end_month,
            test_data=test_data,
            lag_periods=lag_periods,
        )

        return JsonResponse(CorrelateData(data=sorted_correlations[:100]).model_dump())


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

        time_increment = request.GET.get("time_increment", "Quarterly")
        fiscal_end_month = request.GET.get("fiscal_year_end", "December")
        lag_periods = int(request.GET.get("lag_periods", 0))

        sorted_correlations = calculate_correlation(
            time_increment,
            fiscal_end_month,
            test_data=test_data,
            lag_periods=lag_periods,
        )
        return JsonResponse(CorrelateData(data=sorted_correlations[:100]).model_dump())
