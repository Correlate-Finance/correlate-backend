from django.urls import path
from . import views

urlpatterns = [
    path("revenue", views.RevenueView.as_view(), name="revenue"),
    path("correlate", views.CorrelateView.as_view(), name="correlate"),
    path(
        "correlateInputData",
        views.CorrelateInputDataView.as_view(),
        name="correlateInputData",
    ),
    path("dataset", views.DatasetView.as_view(), name="dataset"),
    path("dataset/", views.DatasetView.as_view(), name="dataset"),
]
