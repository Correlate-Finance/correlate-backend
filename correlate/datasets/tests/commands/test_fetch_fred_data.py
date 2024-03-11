from django.test import TestCase, TransactionTestCase
from unittest.mock import patch
from datasets.management.commands.fetch_fred_data import (
    fetch_fred_data,
    fetch_fred_title,
)
from django.core.management import call_command
from datasets.models import DatasetMetadata, Dataset
from datetime import datetime, timezone


class FetchFredTest(TestCase):
    @patch("requests.get")
    def test_fetch_fred_data_success(self, mock_get):
        # Mock the API response
        mock_get.return_value.json.return_value = {
            "observations": [
                {"date": "2020-01-01", "value": "100.0"},
                {"date": "2020-01-02", "value": "200.0"},
            ]
        }
        data = fetch_fred_data("test_series_id")
        self.assertEqual(len(data), 2)  # Check if two records are returned

    @patch("requests.get")
    def test_fetch_fred_title_success(self, mock_get):
        # Mock the API response
        mock_get.return_value.json.return_value = {"seriess": [{"title": "Test Title"}]}
        title = fetch_fred_title("test_series_id")
        self.assertEqual(title, "Test Title")


class CommandTest(TransactionTestCase):
    @patch("datasets.management.commands.fetch_fred_data.fetch_fred_data")
    @patch("datasets.management.commands.fetch_fred_data.fetch_fred_title")
    def test_command(self, mock_fetch_title, mock_fetch_data):
        # Setup mock responses
        mock_fetch_data.return_value = [("2020-01-01", 100.0)]
        mock_fetch_title.return_value = "Test Title"

        # Call the command
        call_command("fetch_fred_data", "test_series_id")

        # Check if the functions were called
        mock_fetch_data.assert_called_once_with("test_series_id")
        mock_fetch_title.assert_called_once_with("test_series_id")

        dataset_metadata = DatasetMetadata.objects.get(internal_name="test_series_id")
        dataset = Dataset.objects.get(metadata=dataset_metadata)

        self.assertEqual(dataset.value, 100.0)
        self.assertEqual(dataset_metadata.external_name, "Test Title")
        self.assertEqual(dataset_metadata.source, "FRED")
        self.assertEqual(dataset_metadata.internal_name, "test_series_id")
        self.assertEqual(Dataset.objects.count(), 1)
        self.assertEqual(
            dataset.date,
            datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc),
        )
