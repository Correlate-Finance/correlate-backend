from users.models import User
from django.urls import reverse
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase
from datasets.models import DatasetMetadata
import json


class TestGetDatasetMetadata(APITestCase):
    def setUp(self) -> None:
        self.user = User.objects.create(email="testuser", password="testpassword")
        self.token, _ = Token.objects.get_or_create(user=self.user)
        self.url = reverse("datasetMetadata")
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        DatasetMetadata.objects.create(
            internal_name="table_name",
            external_name="Title",
            source="Source",
            description="Description",
        )

    def test_get_dataset_metadata_internal_name(self) -> None:
        response = self.client.get(self.url, {"name": "table_name"})
        json_data = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            json_data,
            {
                "series_id": "table_name",
                "title": "Title",
                "source": "Source",
                "description": "Description",
            },
        )

    def test_get_dataset_metadata_external_name(self) -> None:
        response = self.client.get(self.url, {"name": "Title"})
        json_data = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            json_data,
            {
                "series_id": "table_name",
                "title": "Title",
                "source": "Source",
                "description": "Description",
            },
        )

    def test_get_dataset_metadata_doesnt_exist(self) -> None:
        response = self.client.get(self.url, {"name": "non_existent"})
        self.assertEqual(response.status_code, 404)
