from datasets.models import CorrelationParameters, AggregationPeriod, CorrelationMetric
from users.models import User
import pandas as pd


def insert_automatic_correlation(
    user: User,
    stock_ticker: str,
    start_year: int,
    end_year: int,
    aggregation_period: AggregationPeriod,
    correlation_metric: CorrelationMetric,
    lag_periods: int,
    fiscal_year_end: str,
    company_metric: str | None,
) -> CorrelationParameters:
    return CorrelationParameters.objects.create(
        user=user,
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
    dates: pd.Series,
    aggregation_period: AggregationPeriod,
    correlation_metric: CorrelationMetric,
    lag_periods: int,
    fiscal_year_end: str,
) -> CorrelationParameters:
    start_year: int = min(dates).year  # type: ignore
    end_year: int = max(dates).year  # type: ignore

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
