# Create your tests here.
from unittest.mock import patch
import pandas as pd
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from django.contrib.auth.models import User


class CorrelateViewTests(APITestCase):
    def setUp(self):
        # Create a user for authentication
        self.user = User.objects.create_user(
            username="testuser", password="testpassword"
        )
        self.url = reverse("/correlate")

    def test_access_without_authentication(self):
        # Ensure that unauthenticated requests are denied
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_access_with_authentication(self):
        # Ensure that authenticated requests are allowed
        self.client.force_login(user=self.user)
        response = self.client.get(self.url)
        self.assertNotEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_invalid_stock_parameter(self):
        # Test the view with an invalid stock parameter
        self.client.force_login(user=self.user)
        response = self.client.get(self.url, {"stock": ""})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_valid_request(self):
        # Test the view with valid parameters
        self.client.force_login(user=self.user)
        params = {
            "stock": "AAPL",
            "start_year": 2020,
            "aggregation_period": "Annually",
            "lag_periods": "0,1,2",
            "high_level_only": "true",
            "show_negatives": "false",
            "correlation_metric": "RAW_VALUE",
        }
        response = self.client.get(self.url, params)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Add more tests here to cover different scenarios and parameter combinations


class TestRawDatasetView(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            username="testuser", password="testpassword"
        )
        self.url = reverse("/rawdataset")

    @patch("core.mongo_operations.get_all_dfs")
    def test_post_cached_df_exists(self, mock_get_all_dfs):
        # Mock the dataframe to be returned by get_all_dfs
        mock_df = pd.DataFrame(
            {"Date": ["2021-01-01", "2021-01-02"], "Value": ["10", "20"]}
        )
        mock_get_all_dfs.return_value = {"table_name": mock_df}

        # Create a fake request
        response = self.client.post(self.url, "table_name")

        # Check if the response is correct
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"your_expected_json_output")

    @patch("datasets.dataset_metadata.get_metadata_from_external_name")
    @patch("core.mongo_operations.get_df")
    def test_post_df_does_not_exist(
        self, mock_get_df, mock_get_metadata_from_external_name
    ):
        # Mock the metadata to return None indicating the table doesn't exist
        mock_get_metadata_from_external_name.return_value = None
        # Mock get_df to return None indicating the dataframe doesn't exist
        mock_get_df.return_value = None

        response = self.client.post(self.url, "table_name")

        # Check if the response is a bad request
        self.assertEqual(response.status_code, 400)
