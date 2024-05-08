from pydantic import BaseModel
from rest_framework import serializers
from .models import CorrelationParameters, DatasetMetadata, Index, IndexDataset, Report


class CorrelateIndexRequestBody(BaseModel):
    index_name: str
    dates: list[str]
    input_data: list[float]
    index_percentages: list[float]
    index_datasets: list[str]


class IndexDatasetSerializer(serializers.ModelSerializer):
    dataset = serializers.SerializerMethodField()

    def get_dataset(self, obj: IndexDataset):
        return DatasetMetadataSerializer(obj.dataset).data

    class Meta:
        model = IndexDataset
        fields = ["dataset", "weight"]


class IndexSerializer(serializers.ModelSerializer):
    index_datasets = serializers.SerializerMethodField()

    def get_index_datasets(self, obj: Index):
        return IndexDatasetSerializer(
            IndexDataset.objects.filter(index=obj).all(), many=True
        ).data

    class Meta:
        model = Index
        fields = [
            "id",
            "name",
            "user",
            "aggregation_period",
            "correlation_metric",
            "created_at",
            "index_datasets",
        ]

    def create(self, validated_data):
        index_datasets_data = validated_data.pop("index_datasets")
        index = Index.objects.create(**validated_data)
        for index_dataset_data in index_datasets_data:
            IndexDataset.objects.create(index=index, **index_dataset_data)
        return index


class DatasetMetadataSerializer(serializers.ModelSerializer):
    class Meta:
        model = DatasetMetadata
        fields = [
            "internal_name",
            "external_name",
            "description",
            "source",
            "release",
            "url",
            "categories",
            "popularity",
            "units",
        ]


class CorrelationParametersSerializer(serializers.ModelSerializer):
    class Meta:
        model = CorrelationParameters
        fields = [
            "user",
            "created_at",
            "updated_at",
            "correlation_metric",
            "aggregation_period",
            "start_year",
            "end_year",
            "lag_periods",
            "fiscal_year_end",
            "ticker",
            "company_metric",
            "input_data",
        ]


class ReportSerializer(serializers.ModelSerializer):
    parameters = serializers.SerializerMethodField()

    def get_parameters(self, obj: Report):
        return CorrelationParametersSerializer(obj.parameters).data

    class Meta:
        model = Report
        fields = [
            "id",
            "user",
            "parameters",
            "llm_response",
            "report_data",
            "created_at",
            "description",
            "name",
        ]
