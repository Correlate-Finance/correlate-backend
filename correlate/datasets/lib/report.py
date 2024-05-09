from adapters.openai import OpenAIAdapter
from adapters.discounting_cash_flows import fetch_company_description
from datasets.models import CorrelateDataPoint, CorrelationParameters
from datasets.orm.report_orm import create_report
from users.models import User
from datasets.models import AggregationPeriod, CorrelationMetric, Report
import json
from datasets.lib.correlations import generate_stock_correlations


def generate_stock_report(
    top_correlations: list[CorrelateDataPoint],
    stock: str,
    user: User | int,
    correlation_parameters: CorrelationParameters | int,
) -> Report | None:
    company_description = fetch_company_description(stock)

    return generate_report(
        top_correlations,
        stock.upper(),
        company_description,
        user,
        correlation_parameters,
    )


def generate_report(
    top_correlations: list[CorrelateDataPoint],
    name: str,
    company_description: str,
    user: User | int,
    correlation_parameters: CorrelationParameters | int,
) -> Report | None:
    correlations_text = "\n".join(
        [
            f"id: {correlation.internal_name} name: {correlation.title} {correlation.pearson_value}"
            for correlation in top_correlations
        ]
    )

    content = f"Company: {company_description}\n Correlations: {correlations_text}"
    openai_adapter = OpenAIAdapter()
    response = openai_adapter.generate_report(content)

    if response is None:
        return None

    selected_datasets = [d["series_id"] for d in response]
    selected_correlations = {
        correlation.internal_name: correlation
        for correlation in top_correlations
        if correlation.internal_name in selected_datasets
    }
    return create_report(
        name=name.upper(),
        user=user,
        parameters=correlation_parameters,
        llm_response=response,
        report_data=[
            selected_correlations[series_id] for series_id in selected_datasets
        ],
        description=company_description,
    )


def generate_automatic_report(stock: str, user_id: int) -> Report | None:
    # Default parameters
    start_year = 2014
    end_year = 2025
    aggregation_period = AggregationPeriod.QUARTERLY
    lag_periods = 0
    correlation_metric = CorrelationMetric.YOY_GROWTH

    response = generate_stock_correlations(
        stock=stock,
        user=user_id,
        aggregation_period=aggregation_period,
        correlation_metric=correlation_metric,
        lag_periods=lag_periods,
        start_year=start_year,
        end_year=end_year,
    )
    if response.status_code != 200:
        return None

    response_json = json.loads(response.content)
    # fetch the top correlations for the stock ticker using the rust engine
    top_correlations = [CorrelateDataPoint(**c) for c in response_json["data"][:100]]
    correlation_parameters_id = int(response_json["correlation_parameters_id"])
    return generate_stock_report(
        top_correlations, stock, user_id, correlation_parameters_id
    )
