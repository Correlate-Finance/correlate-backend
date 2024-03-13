from django.test import TestCase
from unittest.mock import patch
from django.core.management import call_command
from datasets.models import DatasetMetadata, Dataset
from datasets.management.commands.fetch_eia_data import (
    fetch_eia_data,
    fetch_records_from_eia_data,
    fetch_all_eia_series,
)

TEST_ALL_FACETS_RESPONSE = {
    "response": {
        "totalFacets": 1,
        "facets": [
            {
                "id": "ARICBUS",
                "name": "Asphalt and Road Oil Consumed by the Industrial Sector in Trillion Btu",
            },
        ],
    },
    "request": {
        "command": "\/v2\/total-energy\/facet\/msn\/",  # type: ignore
        "params": {"api_key": "3zjKYxV86AqtJWSRoAECir1wQFscVu6lxXnRVKG8"},
    },
    "apiVersion": "2.1.6",
    "ExcelAddInVersion": "2.1.0",
}

TEST_SERIES_RESPONSE = {
    "response": {
        "total": "611",
        "dateFormat": "YYYY-MM",
        "frequency": "monthly",
        "data": [
            {
                "period": "2023-11",
                "msn": "ARICBUS",
                "seriesDescription": "Asphalt and Road Oil Consumed by the Industrial Sector in Trillion Btu",
                "value": "65.872",
                "unit": "Trillion Btu",
            },
            {
                "period": "2023-10",
                "msn": "ARICBUS",
                "seriesDescription": "Asphalt and Road Oil Consumed by the Industrial Sector in Trillion Btu",
                "value": "92.763",
                "unit": "Trillion Btu",
            },
            {
                "period": "2023-09",
                "msn": "ARICBUS",
                "seriesDescription": "Asphalt and Road Oil Consumed by the Industrial Sector in Trillion Btu",
                "value": "94.797",
                "unit": "Trillion Btu",
            },
        ],
        "description": "These data represent the most recent comprehensive energy statistics integrated across all energy sources.  The data includes total energy production, consumption, stocks, and trade; energy prices; overviews of petroleum, natural gas, coal, electricity, nuclear energy, renewable energy, and carbon dioxide emissions; and data unit conversions values.  Source: https://www.eia.gov/totalenergy/data/monthly/pdf/mer_a_doc.pdf  Report:  MER (https://www.eia.gov/totalenergy/data/monthly/)",
    },
    "request": {
        "command": "/v2/total-energy/data/",
        "params": {
            "frequency": "monthly",
            "data": ["value"],
            "facets": {"msn": ["ARICBUS"]},
            "sort": [{"column": "period", "direction": "desc"}],
            "offset": "0",
            "length": "5000",
            "api_key": "p5iCMjXrWnA4682opLaVGL570vdMShweBO3EBsum",
        },
    },
    "apiVersion": "2.1.6",
    "ExcelAddInVersion": "2.1.0",
}


class FetchEIADataTests(TestCase):
    @patch("datasets.management.commands.fetch_eia_data.requests.get")
    def test_fetch_eia_data(self, mock_get):
        # Mock the API response
        mock_get.return_value.json.side_effect = [
            TEST_SERIES_RESPONSE,
        ]

        # Call the function
        data = fetch_eia_data("TEST_SERIES_ID")

        # Assertions to verify correct parsing
        self.assertIn("data", data)
        self.assertIn("description", data)


class FetchAllEIASeriesTests(TestCase):
    @patch("datasets.management.commands.fetch_eia_data.requests.get")
    def test_fetch_all_eia_series(self, mock_get):
        # Mock the API response
        mock_get.return_value.json.return_value = TEST_ALL_FACETS_RESPONSE

        # Call the function
        series = fetch_all_eia_series()

        # Assertions
        self.assertEqual(series, ["ARICBUS"])


class CommandExecutionTests(TestCase):
    @patch("datasets.management.commands.fetch_eia_data.requests.get")
    def test_command_execution(self, mock_get):
        # Setup mock response
        mock_get.return_value.json.side_effect = [
            TEST_ALL_FACETS_RESPONSE,
            TEST_SERIES_RESPONSE,
        ]

        # Call the command
        call_command("fetch_eia_data", n=1)

        # Assertions to verify database interactions
        self.assertTrue(
            DatasetMetadata.objects.filter(
                internal_name="ARICBUS",
                description=TEST_SERIES_RESPONSE["response"]["description"],
                external_name=TEST_SERIES_RESPONSE["response"]["data"][0][
                    "seriesDescription"
                ],
                source="EIA",
            ).exists()
        )
        self.assertEqual(
            Dataset.objects.filter(metadata__internal_name="ARICBUS").count(),
            len(TEST_SERIES_RESPONSE["response"]["data"]),
        )

    @patch("datasets.management.commands.fetch_eia_data.requests.get")
    def test_command_execution_no_data(self, mock_get):
        # Setup mock response with no data
        mock_get.return_value.json.side_effect = [
            TEST_ALL_FACETS_RESPONSE,
            {"response": {"data": [], "description": "No data"}},
        ]

        # Execute the command
        call_command("fetch_eia_data")

        # Assert that no new records are added to the database
        self.assertFalse(
            DatasetMetadata.objects.filter(internal_name="TEST_SERIES_ID").exists()
        )


class ParseRecordsTests(TestCase):
    @patch("datasets.management.commands.fetch_eia_data.requests.get")
    def test_fetch_records_from_eia_data(self, mock_get):
        # Mock the API response
        mock_get.return_value.json.return_value = TEST_SERIES_RESPONSE

        # Call the function
        data = fetch_eia_data("TEST_SERIES_ID")

        # Call the function
        records = fetch_records_from_eia_data(data["data"], "TEST_SERIES_ID")

        # Assertions
        self.assertEqual(len(records), len(data["data"]))
