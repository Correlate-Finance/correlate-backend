from django.http import JsonResponse, HttpResponseBadRequest, HttpRequest
from django.views.decorators.csrf import csrf_exempt

from core.main_logic import calculate_correlation
import json
from correlate.models import CorrelateData
import requests
import calendar
from datetime import datetime

from functools import cache
import numpy

from core.data_processing import process_data



# Create your views here.
def index(request: HttpRequest):
    time_increment = request.GET.get("time_increment", "Quarterly")
    fiscal_end_month = request.GET.get("fiscal_end_month", "December")

    sorted_correlations = calculate_correlation(time_increment, fiscal_end_month)
    return JsonResponse(CorrelateData(data=sorted_correlations).model_dump())


@cache
def fetch_stock_revenues(
    stock: str, start_year: int, aggregation_period: str = "Annually"
):
    fiscal_year_end = None
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

        fiscal_year_end = calendar.month_name[(reporting_date_month.month + delta) % 12]
        print(fiscal_year_end)

        revenues = {}
        for i in range(len(report)):
            year = report[i]["calendarYear"]
            date = year + report[i]["period"]
            if year < start_year:
                continue
            revenues[date] = report[i]["revenue"]
        return revenues, fiscal_year_end


def revenue(request: HttpRequest):
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


def correlate(request: HttpRequest):
    stock = request.GET.get("stock")
    start_year = request.GET.get("startYear", 2010)
    aggregation_period = request.GET.get("aggregationPeriod", "Annually")

    if stock is None or len(stock) < 2:
        return HttpResponseBadRequest("Pass a valid stock ticker")

    revenues, fiscal_end_month = fetch_stock_revenues(
        stock, start_year, aggregation_period
    )
    test_data = {"Date": list(revenues.keys()), "Value": list(revenues.values())}

    print("test_data", test_data)

    sorted_correlations = calculate_correlation(
        aggregation_period, fiscal_end_month, test_data=test_data
    )

    return JsonResponse(CorrelateData(data=sorted_correlations[:100]).model_dump())


@csrf_exempt
def correlateInputData(request: HttpRequest):

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

    sorted_correlations = calculate_correlation(
        time_increment, fiscal_end_month, test_data=test_data
    )
    return JsonResponse(CorrelateData(data=sorted_correlations[:100]).model_dump())
