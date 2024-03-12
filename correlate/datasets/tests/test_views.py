# Create your tests here.
from unittest.mock import patch
import pandas as pd
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from users.models import User
from django.http import JsonResponse
from datasets import mongo_operations
import json
from rest_framework.authtoken.models import Token


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
        "datasets.views.fetch_stock_revenues",
        return_value=({"2020-01-01": [1]}, "December"),
    )
    @patch(
        "datasets.views.run_correlations", return_value=JsonResponse({"test": "test"})
    )
    def test_valid_request(self, mock_fetch_stock_revenues, mock_run_correlations):
        # Test the view with valid parameters
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        params = {
            "stock": "AAPL",
            "start_year": 2020,
            "aggregation_period": "Annually",
            "lag_periods": "3",
            "high_level_only": "true",
            "show_negatives": "false",
            "correlation_metric": "RAW_VALUE",
        }

        response = self.client.get(self.url, params)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Add more tests here to cover different scenarios and parameter combinations


class TestRawDatasetView(APITestCase):
    def setUp(self):
        self.user = User.objects.create(email="testuser", password="testpassword")
        self.token, _ = Token.objects.get_or_create(user=self.user)
        self.url = reverse("rawdataset")

    def test_post_cached_df_exists(self):
        # Mock the dataframe to be returned by get_all_dfs
        mock_df = pd.DataFrame(
            {"Date": ["2021-01-01", "2021-01-02"], "Value": ["10", "20"]}
        )
        mongo_operations.CACHED_DFS = {"table_name": mock_df}

        # Create a fake request
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        response = self.client.post(self.url, "table_name", content_type="html/text")

        # Check if the response is correct
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            json.loads(response.content),
            '[{"Date":"01-01-2021","Value":10},{"Date":"01-02-2021","Value":20}]',
        )
        # Todo: This is really bad we shouldnt be updating state this way
        mongo_operations.CACHED_DFS = {}

    @patch(
        "datasets.dataset_metadata_orm.get_metadata_from_external_name",
        return_value=None,
    )
    @patch("datasets.mongo_operations.get_df", return_value=None)
    def test_post_df_does_not_exist(
        self, mock_get_df, mock_get_metadata_from_external_name
    ):
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        response = self.client.post(self.url, "table_name", content_type="html/text")

        # Check if the response is a bad request
        self.assertEqual(response.status_code, 400)
