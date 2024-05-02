from datasets.models import Correlation, AggregationPeriod, CorrelationMetric
from users.models import User


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
):
    Correlation.objects.create(
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
    aggregation_period: AggregationPeriod,
    correlation_metric: CorrelationMetric,
    lag_periods: int,
    fiscal_year_end: str,
):
    start_year: int = min(input_data["Date"])  # type: ignore
    end_year: int = max(input_data["Date"])  # type: ignore

    Correlation.objects.create(
        user=user,
        input_data=input_data,
        start_year=start_year,
        end_year=end_year,
        lag_periods=lag_periods,
        fiscal_year_end=fiscal_year_end,
        aggregation_period=aggregation_period,
        correlation_metric=correlation_metric,
    )
