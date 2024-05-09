import json
import urllib.parse
from users.models import User
from typing import List
import pandas as pd
from datasets.lib.report import generate_report, generate_stock_report
from datasets.lib.correlations import (
    correlate_indexes,
    generate_stock_correlations,
    run_correlations_rust,
)
from adapters.discounting_cash_flows import (
    fetch_segment_data,
    fetch_stock_data,
)
from core.data_processing import (
    parse_input_dataset,
    transform_data,
    transform_metric,
)
from core.data_trends import (
    calculate_average_monthly_growth,
    calculate_trailing_months,
    calculate_year_over_year_growth,
    calculate_yearly_stacks,
)
from core.main_logic import correlate_datasets, create_index
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

from datasets.tasks import add, generate_automatic_report_task
from datasets.models import (
    AggregationPeriod,
    CompanyMetric,
    CorrelateData,
    CorrelateDataPoint,
    CorrelationMetric,
    DatasetMetadata,
    Index,
    IndexDataset,
    Report,
)
from datasets.orm.correlation_parameters_orm import (
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
            request.GET.get(
                "aggregation_period", AggregationPeriod.QUARTERLY.value
            ).upper()
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
            request.GET.get(
                "aggregation_period", AggregationPeriod.QUARTERLY.value
            ).upper()
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
            request.GET.get(
                "aggregation_period", AggregationPeriod.QUARTERLY.value
            ).upper()
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
            request.GET.get(
                "aggregation_period", AggregationPeriod.QUARTERLY.value
            ).upper()
        ]
        lag_periods = int(request.GET.get("lag_periods", 0))
        correlation_metric = CorrelationMetric[
            request.GET.get(
                "correlation_metric", CorrelationMetric.RAW_VALUE.value
            ).upper()
        ]
        selected_datasets = request.GET.getlist("selected_datasets")
        selected_indexes = request.GET.getlist("selected_indexes")

        segment: str | None = request.GET.get("segment", None)
        if stock is None or len(stock) < 1:
            return HttpResponseBadRequest("Pass a valid stock ticker")

        return generate_stock_correlations(
            stock=stock,
            user=user,
            segment=segment,
            aggregation_period=aggregation_period,
            correlation_metric=correlation_metric,
            lag_periods=lag_periods,
            start_year=start_year,
            end_year=end_year,
            selected_indexes=selected_indexes,
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
            request.GET.get(
                "aggregation_period", AggregationPeriod.QUARTERLY.value
            ).upper()
        ]
        fiscal_end_month = request.GET.get("fiscal_year_end", "December")
        lag_periods = int(request.GET.get("lag_periods", 0))
        correlation_metric = CorrelationMetric[
            request.GET.get(
                "correlation_metric", CorrelationMetric.RAW_VALUE.value
            ).upper()
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
                test_df=test_df,
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
            request.GET.get(
                "aggregation_period", AggregationPeriod.QUARTERLY.value
            ).upper()
        ]
        correlation_metric = CorrelationMetric[
            request.GET.get(
                "correlation_metric", CorrelationMetric.RAW_VALUE.value
            ).upper()
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

        correlation_parameters_id = request.GET.get("correlation_parameters_id")
        stock: str | None = request.GET.get("stock")
        name: str | None = request.GET.get("name")
        company_description: str | None = None

        if stock is not None:
            report = generate_stock_report(
                top_correlations, stock, user, correlation_parameters_id
            )
        elif name is not None and company_description is not None:
            report = generate_report(
                top_correlations,
                name,
                company_description,
                user,
                correlation_parameters_id,
            )
        else:
            return JsonResponse({"error": "Stock or name and description are required"})

        if report is None:
            return JsonResponse({"error": "Invalid response from OpenAI"})
        return JsonResponse({"report_id": report.id})


class GenerateAutomaticReport(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request: Request) -> HttpResponse:
        user: User = request.user
        body = request.body
        body = body.decode("utf-8")
        stocks = request.GET.getlist("stocks")
        stock = stocks[0] if stocks else None

        for stock in stocks:
            if stock is None or len(stock) < 1:
                return HttpResponseBadRequest("Pass a valid stock ticker")

            generate_automatic_report_task.delay(stock, user.id)

        return JsonResponse({"success": "Queued reports"})


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


class AsyncGet(APIView):
    permission_classes = ()
    authentication_classes = ()

    def get(self, _: Request) -> HttpResponse:
        result = add.delay(4, 4)
        return JsonResponse({"message": f"Task ID: {result.id}"})
