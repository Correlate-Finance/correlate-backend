from users.models import User
from django.urls import reverse
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase
from datasets.models import DatasetMetadata
import json


class TestGetAllDatasetMetadata(APITestCase):
    maxDiff = None

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
            release="Release",
            url="URL",
            units="Units",
            popularity=5,
        )

        DatasetMetadata.objects.create(
            internal_name="table_name_2",
            external_name="Title 2",
            source="Source",
            description="Description",
            release="Release",
            url="URL",
            units="Units",
            popularity=10,
            categories=["a", "b"],
        )

        # Should not be displayed
        DatasetMetadata.objects.create(
            internal_name="table_name3",
            external_name="Title 3",
            source="Source",
            description="Description",
            hidden=True,
        )

        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        response = self.client.get(self.url)
        json_data = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            json_data,
            [
                {
                    "internal_name": "table_name",
                    "external_name": "Title",
                    "source": "Source",
                    "description": "Description",
                    "release": "Release",
                    "url": "URL",
                    "units": "Units",
                    "popularity": 5,
                    "categories": None,
                },
                {
                    "internal_name": "table_name_2",
                    "external_name": "Title 2",
                    "source": "Source",
                    "description": "Description",
                    "release": "Release",
                    "url": "URL",
                    "units": "Units",
                    "popularity": 10,
                    "categories": ["a", "b"],
                },
            ],
        )
