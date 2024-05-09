from django.urls import path
from . import views

urlpatterns = [
    path("revenue", views.RevenueView.as_view(), name="revenue"),
    path("company_data", views.CompanyDataView.as_view(), name="company_data"),
    path("segment_data", views.SegmentDataView.as_view(), name="segment_data"),
    path("correlate", views.CorrelateView.as_view(), name="correlate"),
    path("correlate/", views.CorrelateView.as_view(), name="correlate"),
    path(
        "correlate-input-data",
        views.CorrelateInputDataView.as_view(),
        name="correlate-input-data",
    ),
    path(
        "correlate-input-data/",
        views.CorrelateInputDataView.as_view(),
        name="correlate-input-data",
    ),
    path("raw-dataset", views.RawDatasetView.as_view(), name="rawdataset"),
    path("raw-dataset/", views.RawDatasetView.as_view(), name="rawdataset"),
    path("dataset", views.DatasetView.as_view(), name="dataset"),
    path("dataset/", views.DatasetView.as_view(), name="dataset"),
    path(
        "dataset-metadata/", views.DatasetMetadataView.as_view(), name="datasetMetadata"
    ),
    path("correlate-index", views.CorrelateIndex.as_view(), name="correlateindex"),
    path("correlate-index/", views.CorrelateIndex.as_view(), name="correlateindex"),
    path(
        "get-all-dataset-metadata",
        views.GetAllDatasetMetadata.as_view(),
        name="get_all_dataset_metadata",
    ),
    path("save-index", views.SaveIndexView.as_view(), name="save-index"),
    path("save-index/", views.SaveIndexView.as_view(), name="save-index"),
    path("get-indices", views.GetIndicesView.as_view(), name="get-indices"),
    path("get-indices/", views.GetIndicesView.as_view(), name="get-indices"),
    path(
        "get-dataset-filters",
        views.GetDatasetFilters.as_view(),
        name="get-dataset-filters",
    ),
    path(
        "generate-report",
        views.GenerateReport.as_view(),
        name="generate-report",
    ),
    path(
        "generate-report/",
        views.GenerateReport.as_view(),
        name="generate-report",
    ),
    path(
        "get-report/",
        views.GetReport.as_view(),
        name="get-report",
    ),
    path(
        "get-all-reports/",
        views.GetAllReports.as_view(),
        name="get-all-reports",
    ),
    path(
        "generate-automatic-report/",
        views.GenerateAutomaticReport.as_view(),
        name="generate-automatic-report",
    ),
    path(
        "async",
        views.AsyncGet.as_view(),
        name="async",
    ),
]
