from django.test import TestCase
from rest_framework.test import APIClient
from rest_framework import status
from users.models import User
from datasets.models import Index, IndexDataset, DatasetMetadata
from django.urls import reverse
from rest_framework.authtoken.models import Token


class SaveIndexViewTests(TestCase):
    def setUp(self):
        # Create a user and obtain a token
        self.user = User.objects.create(email="testuser", password="12345")
        self.token, _ = Token.objects.get_or_create(user=self.user)
        self.client = APIClient()
        self.dataset = DatasetMetadata.objects.create(
            internal_name="Dataset 1", description="Sample dataset"
        )
        self.dataset2 = DatasetMetadata.objects.create(
            internal_name="Dataset 2", description="Sample dataset"
        )

    def test_save_index_success(self):
        data = {
            "index_name": "Test Index",
            "aggregation_period": "Quarterly",
            "correlation_metric": "RAW_VALUE",
            "datasets": [
                {"title": "Dataset 1", "percentage": "0.5"},
                {"title": "Dataset 2", "percentage": "0.5"},
            ],
        }

        # Set the token in the request headers
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        response = self.client.post(reverse("save-index"), data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(Index.objects.all().count(), 1)
        self.assertEqual(IndexDataset.objects.all().count(), 2)
        self.assertEqual(
            list(IndexDataset.objects.all().values_list("weight", flat=True)),
            [0.5, 0.5],
        )
        self.assertEqual(response.data, {"message": "Index saved"})  # type: ignore

    def test_save_index_unauthenticated(self):
        # No token provided, simulating unauthenticated state
        data = {
            "name": "Test Index",
            "aggregation_period": "Quarterly",
            "correlation_metric": "RAW_VALUE",
            "datasets": [
                {"title": "Dataset 1", "weight": 0.5},
                {"title": "Dataset 2", "weight": 0.5},
            ],
        }

        response = self.client.post(reverse("save-index"), data, format="json")

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
