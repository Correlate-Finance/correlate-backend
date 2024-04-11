# Create your tests here.
from unittest.mock import patch
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from users.models import User
from django.http import JsonResponse
from rest_framework.authtoken.models import Token
import pytest
from datasets.models import AggregationPeriod, CorrelationMetric


class CorrelateViewTests(APITestCase):
    def setUp(self):
        # Create a user for authentication
        self.user = User.objects.create(email="testuser", password="testpassword")
        self.token, _ = Token.objects.get_or_create(user=self.user)
        self.url = reverse("correlate")

    def test_access_without_authentication(self):
        # Ensure that unauthenticated requests are denied
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_access_with_authentication(self):
        # Ensure that authenticated requests are allowed
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        response = self.client.get(self.url)
        self.assertNotEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_invalid_stock_parameter(self):
        # Test the view with an invalid stock parameter
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        response = self.client.get(self.url, {"stock": ""})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    @patch(
        "datasets.views.fetch_stock_data",
        return_value=({"2020-01-01": 1}, "December"),
    )
    @patch(
        "datasets.views.run_correlations_rust",
        return_value=JsonResponse({"test": "test"}),
    )
    def test_valid_request(self, mock_fetch_stock_data, mock_run_correlations):
        # Test the view with valid parameters
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        params = {
            "stock": "AAPL",
            "start_year": 2020,
            "aggregation_period": AggregationPeriod.ANNUALLY,
            "lag_periods": "3",
            "high_level_only": "true",
            "show_negatives": "false",
            "correlation_metric": CorrelationMetric.RAW_VALUE,
        }

        response = self.client.get(self.url, params)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Add more tests here to cover different scenarios and parameter combinations


class CorrelateViewGoldenTests(APITestCase):
    def setUp(self):
        # Create a user for authentication
        self.user = User.objects.create(email="testuser", password="testpassword")
        self.token, _ = Token.objects.get_or_create(user=self.user)
        self.url = reverse("correlate")
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore

    @pytest.mark.vcr
    def test_correlation(self):
        # Test the view with valid parameters
        lags = 3
        params = {
            "stock": "AAPL",
            "start_year": 2020,
            "aggregation_period": AggregationPeriod.QUARTERLY,
            "lag_periods": str(lags),
            "high_level_only": "false",
            "show_negatives": "false",
            "correlation_metric": CorrelationMetric.RAW_VALUE,
        }

        response = self.client.get(self.url, params)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response = response.json()

        self.assertAlmostEqual(
            response["data"][0]["pearson_value"], 0.8944271909999159, places=5
        )
        self.assertAlmostEqual(
            response["data"][1]["pearson_value"], 0.8660254037844387, places=5
        )
        self.assertAlmostEqual(
            response["data"][2]["pearson_value"], 0.8660254037844386, places=5
        )
        self.assertAlmostEqual(
            response["data"][3]["pearson_value"], 0.8280786712108251, places=5
        )
        sort_by_lags = sorted(response["data"], key=lambda x: x["lag"])

        for i, data in enumerate(sort_by_lags):
            self.assertEqual(data["lag"], i)
            self.assertEqual(len(data["input_data"]), 6)
            self.assertEqual(len(data["dataset_data"]), 6)
