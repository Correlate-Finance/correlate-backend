from django.test import TestCase, TransactionTestCase
from unittest.mock import patch
from adapters.fred import (
    fetch_fred_data,
    fetch_fred_metadata,
)
from django.core.management import call_command
from datasets.models import DatasetMetadata, Dataset
from datetime import datetime, timezone
from unittest.mock import MagicMock, ANY


class FetchFredTest(TestCase):
    @patch("requests.get")
    def test_fetch_fred_data_success(self, mock_get: MagicMock):
        # Mock the API response
        mock_get.return_value.json.return_value = {
            "observations": [
                {"date": "2020-01-01", "value": "100.0"},
                {"date": "2020-01-02", "value": "200.0"},
            ]
        }
        data = fetch_fred_data("test_series_id", None)
        self.assertEqual(len(data), 2)  # Check if two records are returned

    @patch("requests.get")
    def test_fetch_fred_title_success(self, mock_get: MagicMock):
        # Mock the API response
        mock_get.return_value.json.return_value = {"seriess": [{"title": "Test Title"}]}
        metadata = fetch_fred_metadata("test_series_id")
        assert metadata
        title = metadata["title"]
        self.assertEqual(title, "Test Title")

    @patch("requests.get")
    def test_fetch_fred_data_with_periods(self, mock_get: MagicMock):
        # Mock the API response
        mock_get.return_value.json.return_value = {
            "observations": [
                {"date": "2020-01-01", "value": "."},
                {"date": "2020-01-02", "value": "100.0"},
                {"date": "2020-01-03", "value": "200.0"},
            ]
        }
        data = fetch_fred_data("test_series_id", None)
        self.assertEqual(len(data), 2)  # Check if two records are returned


class CommandTest(TransactionTestCase):
    @patch("datasets.management.commands.fetch_fred_data.fetch_fred_data")
    @patch("datasets.management.commands.fetch_fred_data.fetch_fred_metadata")
    def test_command(self, mock_fetch_metadata: MagicMock, mock_fetch_data: MagicMock):
        # Setup mock responses
        mock_fetch_data.return_value = [(datetime(year=2020, month=1, day=1), 100.0)]
        mock_fetch_metadata.return_value = {
            "title": "Test Title",
            "notes": "Description",
        }

        # Call the command
        call_command("fetch_fred_data", series_id="test_series_id")

        # Check if the functions were called
        mock_fetch_data.assert_called_once_with("test_series_id", ANY)
        mock_fetch_metadata.assert_called_once_with("test_series_id")

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
