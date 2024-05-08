from unittest.mock import patch
from urllib.parse import urlencode
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from users.models import User
from django.http import JsonResponse
from rest_framework.authtoken.models import Token
import pytest
from datasets.models import AggregationPeriod, CorrelationParameters, CorrelationMetric


class CorrelateManualInputViewTests(APITestCase):
    def setUp(self):
        # Create a user for authentication
        self.user = User.objects.create(email="testuser", password="testpassword")
        self.token, _ = Token.objects.get_or_create(user=self.user)
        self.url = reverse("correlate-input-data")

    def test_access_without_authentication(self):
        # Ensure that unauthenticated requests are denied
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_access_with_authentication(self):
        # Ensure that authenticated requests are allowed
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        response = self.client.get(self.url)
        self.assertNotEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    @patch(
        "datasets.views.run_correlations_rust",
        return_value=JsonResponse({"test": "test"}),
    )
    def test_valid_request(self, _mock_run_correlations):
        # Test the view with valid parameters
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        params: dict[str, str] = {
            "fiscal_year_end": "December",
            "aggregation_period": AggregationPeriod.ANNUALLY.value,
            "lag_periods": "3",
            "correlation_metric": CorrelationMetric.RAW_VALUE.value,
        }

        body = """Q1'11	4,401
                    Q2'11	4,730
                    Q3'11	4,625
                    Q4'11	4,910
                    Q1'12	4,348
                    Q2'12	4,510
                    Q3'12	4,494
                    Q4'12	4,791
                    """

        response = self.client.post(
            f"{self.url}?{urlencode(params)}", data=body, content_type="text/plain"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    @patch(
        "datasets.views.run_correlations_rust",
        return_value=JsonResponse({"test": "test"}),
    )
    def test_valid_request_creates_correlation(self, _mock_run_correlations):
        # Test the view with valid parameters
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        params: dict[str, str] = {
            "fiscal_year_end": "December",
            "aggregation_period": AggregationPeriod.ANNUALLY.value,
            "lag_periods": "3",
            "correlation_metric": CorrelationMetric.RAW_VALUE.value,
        }

        body = """Q1'11	4,401
                    Q2'11	4,730
                    Q3'11	4,625
                    Q4'11	4,910
                    Q1'12	4,348
                    Q2'12	4,510
                    Q3'12	4,494
                    Q4'12	4,791
                    """

        response = self.client.post(
            f"{self.url}?{urlencode(params)}", data=body, content_type="text/plain"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        correlation_parameters = CorrelationParameters.objects.get()
        self.assertEqual(correlation_parameters.start_year, 2011)
        self.assertEqual(correlation_parameters.end_year, 2012)
        self.assertEqual(
            correlation_parameters.aggregation_period, AggregationPeriod.ANNUALLY
        )
        self.assertEqual(correlation_parameters.lag_periods, 3)
        self.assertEqual(
            correlation_parameters.correlation_metric, CorrelationMetric.RAW_VALUE
        )
        self.assertEqual(correlation_parameters.fiscal_year_end, "December")
        self.assertIsNone(correlation_parameters.ticker)
        self.assertIsNone(correlation_parameters.company_metric)
        self.assertIsNotNone(correlation_parameters.input_data)
