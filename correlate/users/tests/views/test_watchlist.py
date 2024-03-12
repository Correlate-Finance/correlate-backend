from django.test import TestCase
from rest_framework.test import APIClient
from rest_framework import status
from unittest.mock import patch
from users.models import WatchList, User
from datasets.models import DatasetMetadata
from django.urls import reverse


class WatchListViewTests(TestCase):
    def setUp(self):
        # Create a user
        self.user = User.objects.create(email="testuser", password="12345")
        self.client = APIClient()
        self.client.force_login(user=self.user)

        # Create dataset metadata instance for testing
        self.dataset = DatasetMetadata.objects.create(
            internal_name="Dataset 1", description="Sample dataset"
        )

    @patch("users.views.dataset_metadata_orm.get_metadata_from_external_name")
    def test_add_watchlist_success(self, mock_get_metadata):
        mock_get_metadata.return_value = self.dataset

        response = self.client.post(reverse("add-watchlist"), {"dataset": "Dataset 1"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(
            WatchList.objects.filter(user=self.user, dataset=self.dataset).exists()
        )

    @patch("users.views.dataset_metadata_orm.get_metadata_from_external_name")
    def test_add_watchlist_dataset_not_found(self, mock_get_metadata):
        mock_get_metadata.return_value = None

        response = self.client.post(
            reverse("add-watchlist"), {"dataset": "Nonexistent Dataset"}
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    @patch("users.views.dataset_metadata_orm.get_metadata_from_external_name")
    def test_delete_watchlist_success(self, mock_get_metadata):
        mock_get_metadata.return_value = self.dataset
        WatchList.objects.create(user=self.user, dataset=self.dataset)

        response = self.client.post(
            reverse("delete-watchlist"), {"dataset": "Dataset 1"}
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(
            WatchList.objects.filter(user=self.user, dataset=self.dataset).exists()
        )

    @patch("users.views.dataset_metadata_orm.get_metadata_from_external_name")
    def test_delete_watchlist_not_found(self, mock_get_metadata):
        mock_get_metadata.return_value = self.dataset

        response = self.client.post(
            reverse("delete-watchlist"), {"dataset": "Dataset 1"}
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
