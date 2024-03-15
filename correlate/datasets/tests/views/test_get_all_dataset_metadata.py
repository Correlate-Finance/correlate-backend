from users.models import User
from django.urls import reverse
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase
from datasets.models import DatasetMetadata
import json


class TestGetAllDatasetMetadata(APITestCase):
    def setUp(self) -> None:
        self.user = User.objects.create(email="testuser", password="testpassword")
        self.token, _ = Token.objects.get_or_create(user=self.user)
        self.url = reverse("get_all_dataset_metadata")

    def test_get_all_dataset_metadata(self) -> None:
        DatasetMetadata.objects.create(
            internal_name="table_name",
            external_name="Title",
            source="Source",
            description="Description",
        )

        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        response = self.client.get(self.url)
        json_data = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            json_data,
            [{"series_id": "table_name", "title": "Title"}],
        )
