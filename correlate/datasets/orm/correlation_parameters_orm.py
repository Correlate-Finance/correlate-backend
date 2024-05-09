from datasets.lib.date import parse_year_from_date
from datasets.models import (
    CorrelationParameters,
    AggregationPeriod,
    CorrelationMetric,
    Month,
)
from users.models import User


def insert_automatic_correlation(
    user: User | int,
    stock_ticker: str,
    start_year: int,
    end_year: int,
    aggregation_period: AggregationPeriod,
    correlation_metric: CorrelationMetric,
    lag_periods: int,
    fiscal_year_end: Month,
    company_metric: str | None = None,
) -> CorrelationParameters:
    if isinstance(user, int):
        user_id = user
    else:
        user_id = user.id

    return CorrelationParameters.objects.create(
        user_id=user_id,
        ticker=stock_ticker,
        start_year=start_year,
        end_year=end_year,
        lag_periods=lag_periods,
        fiscal_year_end=fiscal_year_end,
        company_metric=company_metric,
        aggregation_period=aggregation_period,
        correlation_metric=correlation_metric,
    )


def insert_manual_correlation(
    user: User,
    input_data: dict[str, list[str | int]],
    aggregation_period: AggregationPeriod,
    correlation_metric: CorrelationMetric,
    lag_periods: int,
    fiscal_year_end: Month,
) -> CorrelationParameters:
    start_year = parse_year_from_date(min(input_data["Date"]))  # type: ignore
    end_year = parse_year_from_date(max(input_data["Date"]))  # type: ignore

    return CorrelationParameters.objects.create(
        user=user,
        input_data=input_data,
        start_year=start_year,
        end_year=end_year,
        lag_periods=lag_periods,
        fiscal_year_end=fiscal_year_end,
        aggregation_period=aggregation_period,
        correlation_metric=correlation_metric,
    )
