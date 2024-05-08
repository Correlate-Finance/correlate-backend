import json
import urllib.parse
from datetime import datetime
from typing import List

import pandas as pd
import requests
from adapters.discounting_cash_flows import (
    fetch_company_description,
    fetch_segment_data,
    fetch_stock_data,
)
from adapters.openai import OpenAIAdapter
from core.data_processing import (
    parse_input_dataset,
    transform_data,
    transform_metric,
    transform_quarterly,
)
from core.data_trends import (
    calculate_average_monthly_growth,
    calculate_trailing_months,
    calculate_year_over_year_growth,
    calculate_yearly_stacks,
)
from core.main_logic import correlate_datasets, create_index
from django.conf import settings
from django.db import transaction
from django.http import (
    HttpResponse,
    HttpResponseBadRequest,
    HttpResponseNotFound,
    JsonResponse,
)
from rest_framework.permissions import IsAuthenticated
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from datasets.orm.report_orm import create_report
from datasets.lib import parse_year_from_date
from datasets.models import (
    AggregationPeriod,
    CompanyMetric,
    CorrelateData,
    CorrelateDataPoint,
    CorrelationMetric,
    DatasetMetadata,
    Index,
    IndexDataset,
    CorrelationParameters,
    Report,
)
from datasets.orm.correlation_parameters_orm import (
    insert_automatic_correlation,
    insert_manual_correlation,
)
from datasets.orm.dataset_metadata_orm import (
    get_internal_name_from_external_name,
    get_metadata_from_external_name,
    get_metadata_from_name,
)
from datasets.orm.dataset_orm import get_dataset_filters, get_df
from datasets.serializers import (
    CorrelateIndexRequestBody,
    DatasetMetadataSerializer,
    IndexSerializer,
    ReportSerializer,
)


class RevenueView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        stock = request.GET.get("stock")
        start_year = int(request.GET.get("start_year", 2010))
        end_year = request.GET.get("end_year", None)
        if end_year is not None:
            end_year = int(end_year)

        aggregation_period = AggregationPeriod[
            request.GET.get("aggregation_period", AggregationPeriod.QUARTERLY.value).upper()
        ]

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

        aggregation_period = AggregationPeriod[
            request.GET.get("aggregation_period", AggregationPeriod.QUARTERLY.value).upper()
        ]

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


class SegmentDataView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        stock = request.GET.get("stock")
        start_year = int(request.GET.get("start_year", 2010))
        end_year = request.GET.get("end_year", None)
        if end_year is not None:
            end_year = int(end_year)

        aggregation_period = AggregationPeriod[
            request.GET.get("aggregation_period", AggregationPeriod.QUARTERLY.value).upper()
        ]

        if stock is None or len(stock) < 1:
            return HttpResponseBadRequest("Pass a valid stock ticker")

        segments = fetch_segment_data(
            stock,
            start_year,
            aggregation_period,
            end_year=end_year,
        )

        json_segments = []
        for segment, values in segments.items():
            json_segments.append(
                {
                    "segment": segment,
                    "data": [
                        {"date": date, "value": str(value)}
                        for date, value in values.items()
                    ],
                }
            )

        return JsonResponse(json_segments, safe=False)


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


class CorrelateView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        user = request.user
        stock = request.GET.get("stock")
        start_year = int(request.GET.get("start_year", 2010))
        end_year = request.GET.get("end_year", None)
        if end_year is not None:
            end_year = int(end_year)

        aggregation_period = AggregationPeriod[
            request.GET.get("aggregation_period", AggregationPeriod.QUARTERLY.value).upper()
        ]
        lag_periods = int(request.GET.get("lag_periods", 0))
        correlation_metric = CorrelationMetric[
            request.GET.get("correlation_metric", CorrelationMetric.RAW_VALUE.value).upper()
        ]
        selected_datasets = request.GET.getlist("selected_datasets")
        selected_indexes = request.GET.getlist("selected_indexes")

        try:
            company_metric = CompanyMetric[request.GET.get("company_metric", "REVENUE")]
        except KeyError:
            return HttpResponseBadRequest("Invalid company metric")

        segment: str | None = request.GET.get("segment", None)
        if stock is None or len(stock) < 1:
            return HttpResponseBadRequest("Pass a valid stock ticker")

        segment_data = None
        if segment is not None:
            segment_data = fetch_segment_data(
                stock,
                start_year,
                aggregation_period,
                end_year=end_year,
            )[segment]

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
                    aggregation_period=aggregation_period,
                    correlation_metric=correlation_metric,
                ).model_dump()
            )

        if segment_data is not None:
            test_data = {
                "Date": list(segment_data.keys()),
                "Value": list(segment_data.values()),
            }
            test_df = transform_data(
                pd.DataFrame(test_data),
                aggregation_period,
                correlation_metric=correlation_metric,
                fiscal_end_month=fiscal_end_month,
            )
        elif revenues is not None:
            test_data = {
                "Date": list(revenues.keys()),
                "Value": list(revenues.values()),
            }
            test_df = transform_metric(
                pd.DataFrame(test_data),
                aggregation_period,
                correlation_metric=correlation_metric,
            )

        if selected_indexes:
            return correlate_indexes(
                indexes=list(Index.objects.filter(id__in=selected_indexes)),
                aggregation_period=aggregation_period,
                correlation_metric=correlation_metric,
                fiscal_end_month=fiscal_end_month,
                test_df=test_df,
            )
        else:
            correlation_parameters = insert_automatic_correlation(
                user=user,
                stock_ticker=stock,
                start_year=start_year,
                end_year=end_year,
                aggregation_period=aggregation_period,
                correlation_metric=correlation_metric,
                lag_periods=lag_periods,
                fiscal_year_end=fiscal_end_month,
                company_metric=segment,
            )

            return run_correlations_rust(
                correlation_parameters=correlation_parameters,
                aggregation_period=aggregation_period,
                fiscal_end_month=fiscal_end_month,
                test_df=test_df,
                test_data=test_data,
                lag_periods=lag_periods,
                correlation_metric=correlation_metric,
                selected_datasets=selected_datasets,
            )


class CorrelateInputDataView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        user = request.user
        body = request.body
        body = body.decode("utf-8")

        test_data = parse_input_dataset(body)
        if test_data is None:
            return HttpResponseBadRequest("Could not parse input data.")
        aggregation_period = AggregationPeriod[
            request.GET.get("aggregation_period", AggregationPeriod.QUARTERLY.value).upper()
        ]
        fiscal_end_month = request.GET.get("fiscal_year_end", "December")
        lag_periods = int(request.GET.get("lag_periods", 0))
        correlation_metric = CorrelationMetric[
            request.GET.get("correlation_metric", CorrelationMetric.RAW_VALUE.value).upper()
        ]
        selected_datasets = request.GET.getlist("selected_datasets")
        selected_indexes = request.GET.getlist("selected_indexes")

        dates: list[str] = test_data["Date"]  # type: ignore
        if len(dates) == 0:
            return HttpResponseBadRequest("Invalid data format")

        if "Q" in dates[0]:
            test_df = transform_metric(
                pd.DataFrame(test_data),
                aggregation_period,
                correlation_metric=correlation_metric,
            )
        else:
            test_df = transform_data(
                pd.DataFrame(test_data),
                aggregation_period,
                correlation_metric=correlation_metric,
                fiscal_end_month=fiscal_end_month,
            )

        if selected_indexes:
            return correlate_indexes(
                indexes=list(Index.objects.filter(id__in=selected_indexes)),
                aggregation_period=aggregation_period,
                correlation_metric=correlation_metric,
                fiscal_end_month=fiscal_end_month,
                test_df=test_df,
            )
        else:
            correlation_parameters = insert_manual_correlation(
                user=user,
                input_data=test_data,
                aggregation_period=aggregation_period,
                correlation_metric=correlation_metric,
                lag_periods=lag_periods,
                fiscal_year_end=fiscal_end_month,
            )
            return run_correlations_rust(
                correlation_parameters=correlation_parameters,
                aggregation_period=aggregation_period,
                fiscal_end_month=fiscal_end_month,
                test_df=test_df,
                test_data=test_data,
                lag_periods=lag_periods,
                correlation_metric=correlation_metric,
                selected_datasets=selected_datasets,
            )


class GetAllDatasetMetadata(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, _: Request) -> HttpResponse:
        metadata = DatasetMetadata.objects.filter(hidden=False)
        serialized = DatasetMetadataSerializer(metadata, many=True)
        return JsonResponse(
            serialized.data,
            safe=False,
        )


class CorrelateIndex(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        body = request.body
        body = body.decode("utf-8")
        aggregation_period = AggregationPeriod[
            request.GET.get("aggregation_period", AggregationPeriod.QUARTERLY.value).upper()
        ]
        correlation_metric = CorrelationMetric[
            request.GET.get("correlation_metric", CorrelationMetric.RAW_VALUE.value).upper()
        ]
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
                aggregation_period=aggregation_period,
                correlation_metric=correlation_metric,
            ).model_dump()
        )


def correlate_indexes(
    indexes: list[Index],
    aggregation_period: AggregationPeriod,
    correlation_metric: CorrelationMetric,
    fiscal_end_month: str,
    test_df: pd.DataFrame,
) -> HttpResponse:
    results: list[CorrelateDataPoint] = []
    if aggregation_period == AggregationPeriod.QUARTERLY:
        test_df = transform_quarterly(test_df, fiscal_end_month)
    for index in indexes:
        index_df = create_index(
            dataset_weights={
                index_dataset.dataset.internal_name: index_dataset.weight
                for index_dataset in IndexDataset.objects.filter(
                    index=index
                ).prefetch_related("dataset")
            },
            aggregation_period=aggregation_period,
            correlation_metric=correlation_metric,
            fiscal_end_month=fiscal_end_month,
        )
        if index_df is None:
            continue

        result = correlate_datasets(df=index_df, test_df=test_df, df_title=index.name)
        if result:
            results.extend(result)

    return JsonResponse(
        CorrelateData(
            data=results,
            aggregation_period=aggregation_period,
            correlation_metric=correlation_metric,
        ).model_dump()
    )


def run_correlations_rust(
    correlation_parameters: CorrelationParameters,
    aggregation_period: AggregationPeriod,
    fiscal_end_month: str,
    test_df: pd.DataFrame,
    test_data: dict,
    lag_periods: int,
    correlation_metric: CorrelationMetric,
    selected_datasets: list[str] | None = None,
) -> JsonResponse:
    test_df = test_df.rename(columns={"Date": "date", "Value": "value"})
    records = test_df.to_json(orient="records", default_handler=str)
    body = {
        "manual_input_dataset": json.loads(records),
        "selected_datasets": selected_datasets or [],
    }

    start_year = parse_year_from_date(min(test_data["Date"]))
    end_year = parse_year_from_date(max(test_data["Date"]))

    if start_year is None or end_year is None:
        return JsonResponse({"error": "Invalid date format"})

    url = f"{settings.RUST_ENGINE_URL}/correlate_input?"
    request_paramters = {
        "aggregation_period": aggregation_period.value,
        "fiscal_year_end": datetime.strptime(fiscal_end_month, "%B").month,
        "lag_periods": lag_periods,
        "correlation_metric": correlation_metric.value,
        "start_year": start_year,
        "end_year": end_year,
    }

    query_string = urllib.parse.urlencode(request_paramters)

    response = requests.post(url + query_string, data=json.dumps(body))
    json_response = response.json()
    # Add the id for the correlation parameters to the response
    json_response["correlation_parameters_id"] = correlation_parameters.id
    return JsonResponse(json_response)


class SaveIndexView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        user = request.user
        datasets: List[dict] = request.data.get("datasets", [])  # type: ignore
        name: str = request.data.get("index_name", "")  # type: ignore
        index_id: int = request.data.get("index_id", None)  # type: ignore

        if name is None or len(name) == 0:
            return Response({"message": "Index name cannot be empty"}, status=400)

        aggregation_period: str | None = request.data.get("aggregation_period", None)  # type: ignore
        correlation_metric: str | None = request.data.get("correlation_metric", None)  # type: ignore

        try:
            parsed_datasets = [
                (dataset["title"], dataset["percentage"]) for dataset in datasets
            ]
        except KeyError:
            return Response({"message": "Invalid dataset format"}, status=400)

        total_weight = sum([float(dataset[1]) for dataset in parsed_datasets])
        if total_weight > 1 or total_weight < 0.99:
            return Response({"message": "Total weight must be between 1"}, status=400)

        # Make sure all db actions happen atomically
        with transaction.atomic():
            if index_id:
                index = Index.objects.get(id=index_id)
                index.name = name
                index.aggregation_period = aggregation_period
                index.correlation_metric = correlation_metric
                index.save()
                IndexDataset.objects.filter(index=index).delete()
            else:
                index = Index.objects.create(
                    name=name,
                    user=user,
                    aggregation_period=aggregation_period,
                    correlation_metric=correlation_metric,
                )

            IndexDataset.objects.bulk_create(
                [
                    IndexDataset(
                        dataset=get_metadata_from_name(dataset[0]),
                        weight=dataset[1],
                        index=index,
                    )
                    for dataset in parsed_datasets
                ]
            )

        return Response({"message": "Index saved"})


class GetIndicesView(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        user = request.user
        indices = Index.objects.filter(user=user)
        index_serializer = IndexSerializer(indices, many=True)
        return Response(index_serializer.data)


class GetDatasetFilters(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, _: Request) -> HttpResponse:
        filters = get_dataset_filters()
        return JsonResponse(filters)


class GenerateReport(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        user = request.user
        body = request.body
        body = body.decode("utf-8")
        top_correlations = [
            CorrelateDataPoint(**c) for c in json.loads(body)["top_correlations"]
        ]

        correlations_text = "\n".join(
            [
                f"ID: {correlation.internal_name} Name: {correlation.title} {correlation.pearson_value}"
                for correlation in top_correlations
            ]
        )

        stock = request.GET.get("stock")

        if stock is None:
            # This is the manual input case in which both the name and
            # description of the report should be present.
            company_description = request.GET.get("company_description")
            name = request.GET.get("name")
            if company_description is None or name is None:
                return JsonResponse(
                    {"error": "Company description and name are required"}
                )
        else:
            company_description = fetch_company_description(stock)

        correlation_parameters_id = request.GET.get("correlation_parameters_id")
        content = f"Company: {company_description}\n Correlations: {correlations_text}"
        openai_adapter = OpenAIAdapter()
        response = openai_adapter.generate_report(content)

        if response is None:
            return JsonResponse({"error": "Invalid response from OpenAI"})

        selected_datasets = [d["series_id"] for d in response]
        selected_correlations = {
            correlation.internal_name: correlation
            for correlation in top_correlations
            if correlation.internal_name in selected_datasets
        }
        report = create_report(
            name=stock.upper() if stock is not None else name,
            user=user,
            parameters=correlation_parameters_id,
            llm_response=response,
            report_data=[
                selected_correlations[series_id] for series_id in selected_datasets
            ],
            description=company_description,
        )
        return JsonResponse({"report_id": report.id})


class GenerateAutomaticReport(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        user = request.user
        body = request.body
        body = body.decode("utf-8")
        stocks = request.GET.getlist("stocks")
        stock = stocks[0] if stocks else None
        start_year = 2014
        end_year = 2025
        aggregation_period = AggregationPeriod.QUARTERLY
        lag_periods = 0
        correlation_metric = CorrelationMetric.YOY_GROWTH

        company_metric = CompanyMetric["REVENUE"]

        if stock is None or len(stock) < 1:
            return HttpResponseBadRequest("Pass a valid stock ticker")

        revenues, fiscal_end_month = fetch_stock_data(
            stock,
            start_year,
            aggregation_period,
            end_year=end_year,
            company_metric=company_metric,
        )

        if revenues is None or fiscal_end_month is None:
            return JsonResponse(
                CorrelateData(
                    data=[],
                    aggregation_period=aggregation_period,
                    correlation_metric=correlation_metric,
                ).model_dump()
            )

        test_data = {
            "Date": list(revenues.keys()),
            "Value": list(revenues.values()),
        }
        test_df = transform_metric(
            pd.DataFrame(test_data),
            aggregation_period,
            correlation_metric=correlation_metric,
        )

        correlation_parameters = insert_automatic_correlation(
            user=user,
            stock_ticker=stock,
            start_year=start_year,
            end_year=end_year,
            aggregation_period=aggregation_period,
            correlation_metric=correlation_metric,
            lag_periods=lag_periods,
            fiscal_year_end=fiscal_end_month,
        )

        correlations = run_correlations_rust(
            correlation_parameters=correlation_parameters,
            aggregation_period=aggregation_period,
            fiscal_end_month=fiscal_end_month,
            test_df=test_df,
            test_data=test_data,
            lag_periods=lag_periods,
            correlation_metric=correlation_metric,
        )

        # fetch the top correlations for the stock ticker using the rust engine
        top_correlations = [
            CorrelateDataPoint(**c)
            for c in json.loads(correlations.content)["data"][:100]
        ]

        correlations_text = "\n".join(
            [
                f"id: {correlation.internal_name} name: {correlation.title} {correlation.pearson_value}"
                for correlation in top_correlations
            ]
        )

        if stock is None:
            # this is the manual input case in which both the name and
            # description of the report should be present.
            company_description = request.get.get("company_description")
            name = request.get.get("name")
            if company_description is None or name is None:
                return JsonResponse(
                    {"error": "company description and name are required"}
                )
        else:
            company_description = fetch_company_description(stock)

        correlation_parameters_id = correlation_parameters.id
        content = f"Company: {company_description}\n Correlations: {correlations_text}"
        openai_adapter = OpenAIAdapter()
        response = openai_adapter.generate_report(content)

        if response is None:
            return JsonResponse({"error": "Invalid response from OpenAI"})

        selected_datasets = [d["series_id"] for d in response]
        selected_correlations = {
            correlation.internal_name: correlation
            for correlation in top_correlations
            if correlation.internal_name in selected_datasets
        }
        report = create_report(
            name=stock.upper() if stock is not None else name,
            user=user,
            parameters=correlation_parameters_id,
            llm_response=response,
            report_data=[
                selected_correlations[series_id] for series_id in selected_datasets
            ],
            description=company_description,
        )
        return JsonResponse({"report_id": report.id})


class GetReport(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        user = request.user
        report_id = request.GET.get("report_id")
        report = Report.objects.filter(user=user, id=report_id).first()
        if report is None:
            return JsonResponse({"error": "Report not found"})

        return JsonResponse(
            ReportSerializer(report).data,
        )


class GetAllReports(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request: Request) -> HttpResponse:
        user = request.user
        reports = Report.objects.filter(user=user)
        report_serializer = ReportSerializer(reports, many=True)
        return Response(report_serializer.data)
