# Create your tests here.
from unittest.mock import patch
import pandas as pd
from django.urls import reverse
from rest_framework.test import APITestCase
from users.models import User
from datasets.orm import dataset_orm
import json
from rest_framework.authtoken.models import Token
from datasets.models import DatasetMetadata


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
        dataset_orm.CACHED_DFS = {"table_name": mock_df}
        DatasetMetadata.objects.create(
            internal_name="table_name",
            external_name="Title",
            source="Source",
            description="Description",
        )

        # Create a fake request
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        response = self.client.post(self.url, "table_name", content_type="html/text")
        json_data = json.loads(response.content)

        # Check if the response is correct
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            json.loads(json_data["dataset"]),
            [{"Date": "01-01-2021", "Value": 10}, {"Date": "01-02-2021", "Value": 20}],
        )
        self.assertEqual(
            json_data["source"],
            "Source",
        )
        self.assertEqual(
            json_data["description"],
            "Description",
        )
        # Todo: This is really bad we shouldnt be updating state this way
        dataset_orm.CACHED_DFS = {}

    @patch(
        "datasets.orm.dataset_metadata_orm.get_metadata_from_external_name",
        return_value=None,
    )
    @patch("datasets.orm.dataset_orm.get_df", return_value=None)
    def test_post_df_does_not_exist(
        self, mock_get_df, mock_get_metadata_from_external_name
    ):
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token}")  # type: ignore
        response = self.client.post(self.url, "table_name", content_type="html/text")

        # Check if the response is a bad request
        self.assertEqual(response.status_code, 400)
