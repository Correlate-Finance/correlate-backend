from django.urls import path
from . import views

urlpatterns = [
    path("revenue", views.RevenueView.as_view(), name="revenue"),
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
]
