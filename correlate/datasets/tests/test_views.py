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
