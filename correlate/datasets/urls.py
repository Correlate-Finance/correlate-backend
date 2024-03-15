from django.urls import path
from . import views

urlpatterns = [
    path("revenue", views.RevenueView.as_view(), name="revenue"),
    path("correlate", views.CorrelateView.as_view(), name="correlate"),
    path("correlate/", views.CorrelateView.as_view(), name="correlate"),
    path(
        "correlateInputData",
        views.CorrelateInputDataView.as_view(),
        name="correlateInputData",
    ),
    path(
        "correlateInputData/",
        views.CorrelateInputDataView.as_view(),
        name="correlateInputData",
    ),
    path("rawdataset", views.RawDatasetView.as_view(), name="rawdataset"),
    path("rawdataset/", views.RawDatasetView.as_view(), name="rawdataset"),
    path("dataset", views.DatasetView.as_view(), name="dataset"),
    path("dataset/", views.DatasetView.as_view(), name="dataset"),
    path("correlateindex", views.CorrelateIndex.as_view(), name="correlateindex"),
    path("correlateindex/", views.CorrelateIndex.as_view(), name="correlateindex"),
    path(
        "get_all_dataset_metadata",
        views.GetAllDatasetMetadata.as_view(),
        name="get_all_dataset_metadata",
    ),
]
