from django.conf import settings
from datetime import datetime
from datasets.models import AggregationPeriod, CompanyMetric
import requests
import calendar
from functools import cache
from collections import defaultdict

# TODO: Add VCR test to record the response of this API

API_KEY = settings.DCF_API_KEY
BASE_URL = "https://discountingcashflows.com/api"


@cache
def fetch_stock_data(
    stock: str,
    start_year: int,
    aggregation_period: AggregationPeriod = AggregationPeriod.ANNUALLY,
    end_year: int | None = None,
    company_metric: CompanyMetric = CompanyMetric.REVENUE,
) -> tuple[dict, str | None]:
    if API_KEY is None or API_KEY == "":
        raise ValueError("DCF_API_KEY is not set in settings.py")

    if aggregation_period == AggregationPeriod.ANNUALLY:
        url = f"{BASE_URL}/income-statement/{stock}/"
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
        url = f"{BASE_URL}/income-statement/quarterly/{stock}/?key={API_KEY}"
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
    if API_KEY is None or API_KEY == "":
        raise ValueError("DCF_API_KEY is not set in settings.py")

    if aggregation_period == AggregationPeriod.ANNUALLY:
        url = f"{BASE_URL}/revenue-analysis/{stock}/product/?key={API_KEY}"
        response = requests.get(url)
    else:
        url = f"{BASE_URL}/revenue-analysis/{stock}/product/quarter/?key={API_KEY}"
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


def fetch_company_description(stock: str):
    url = f"{BASE_URL}/profile/{stock}/"
    response = requests.get(url)
    return response.json()["report"][0]["description"]
