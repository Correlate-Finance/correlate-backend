from django.http import JsonResponse, HttpResponseBadRequest, HttpRequest
from django.views.decorators.csrf import csrf_exempt

from core.main_logic import calculate_correlation
import json
from pydantic import BaseModel
import requests

from functools import cache
import numpy 


class CorrelateDataPoint(BaseModel):
    title: str
    pearson_value: float


class CorrelateData(BaseModel):
    data: list[CorrelateDataPoint]


# Create your views here.
def index(request:HttpRequest):
    time_increment = request.GET.get("time_increment", "Quarterly")
    fiscal_end_month = request.GET.get("fiscal_end_month", "December")

    sorted_correlations = calculate_correlation(time_increment, fiscal_end_month)

    correlation_points = []
    for title, corr_value in sorted_correlations:
        correlation_points.append(
            CorrelateDataPoint(title=title, pearson_value=corr_value)
        )

    return JsonResponse(CorrelateData(data=correlation_points).model_dump())

@cache
def fetch_stock_revenues(stock: str, start_year: int):
    url = f"https://discountingcashflows.com/api/income-statement/{stock}/"
    response = requests.get(url)

    response_json = response.json()

    revenues = {}
    report = response_json["report"]
    for i in range(len(report)):
        year = report[i]["calendarYear"]
        if year < start_year:
            continue
        revenues[year] = report[i]["revenue"]
    return revenues


def fetch_historic_data(request: HttpRequest):
    stock = request.GET.get("stock")
    start_year = request.GET.get("startYear", 2010)

    if stock is None or len(stock) < 2:
        return HttpResponseBadRequest("Pass a valid stock ticker")

    revenues = fetch_stock_revenues(stock, start_year)
    return JsonResponse(revenues)


def correlate(request: HttpRequest):
    stock = request.GET.get("stock")
    start_year = request.GET.get("startYear", 2010)

    if stock is None or len(stock) < 2:
        return HttpResponseBadRequest("Pass a valid stock ticker")

    revenues = fetch_stock_revenues(stock, start_year)
    test_data = {"Date": list(map(lambda x: x + "-01-01", revenues.keys())), "Value": list(revenues.values())}

    print("test_data", test_data)

    time_increment = request.GET.get("time_increment", "Annually")
    fiscal_end_month = request.GET.get("fiscal_end_month", "December")

    sorted_correlations = calculate_correlation(time_increment, fiscal_end_month, test_data=test_data)

    correlation_points = []
    for title, corr_value in sorted_correlations[:100]:
        if corr_value < 0.8:
            break
        correlation_points.append(
            CorrelateDataPoint(title=title, pearson_value=corr_value)
        )

    return JsonResponse(CorrelateData(data=correlation_points).model_dump())

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

    test_data = {"Date": dates, "Value": values}

    print("test_data", test_data)

    time_increment = request.GET.get("time_increment", "Annually")
    fiscal_end_month = request.GET.get("fiscal_end_month", "December")

    sorted_correlations = calculate_correlation(time_increment, fiscal_end_month, test_data=test_data)

    correlation_points = []
    for title, corr_value in sorted_correlations[:100]:
        if corr_value < 0.8:
            break

        correlation_points.append(
            CorrelateDataPoint(title=title, pearson_value=corr_value)
        )
    print(correlation_points)
    return JsonResponse(CorrelateData(data=correlation_points).model_dump())
