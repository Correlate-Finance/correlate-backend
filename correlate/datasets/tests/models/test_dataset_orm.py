from django.test import TransactionTestCase
from datasets.models import Dataset, DatasetMetadata
from datasets.dataset_orm import (
    add_dataset,
    parse_excel_file_for_datasets,
    get_all_postgres_dfs,
)
from datetime import datetime

from django.test import TestCase
from io import BytesIO
import openpyxl
import pytz
from django.core.files.uploadedfile import SimpleUploadedFile
import pandas as pd
from pathlib import Path


class AddDatasetTest(TransactionTestCase):
    def setUp(self):
        self.metadata = DatasetMetadata.objects.create(internal_name="Test Metadata")

    def test_add_new_dataset(self):
        records = [(datetime(2020, 1, 1), 100.0), (datetime(2020, 1, 2), 200.0)]
        total_new = add_dataset(records, self.metadata)
        self.assertEqual(total_new, 2)
        self.assertEqual(Dataset.objects.count(), 2)

    def test_add_duplicate_dataset(self):
        records = [(datetime(2020, 1, 1), 100.0)]
        add_dataset(records, self.metadata)  # First addition

        # Attempt to add the same record again
        total_new = add_dataset(records, self.metadata)
        self.assertEqual(total_new, 0)  # No new record should be added
        self.assertEqual(
            Dataset.objects.count(), 1
        )  # Still only one record in the database

    def test_mixed_new_and_duplicate_datasets(self):
        Dataset.objects.create(
            metadata=self.metadata, date=datetime(2020, 1, 1), value=100.0
        )

        records = [
            (datetime(2020, 1, 1), 100.0),  # Duplicate
            (datetime(2020, 1, 2), 200.0),  # New
            (datetime(2020, 1, 3), 300.0),  # New
        ]
        total_new = add_dataset(records, self.metadata)
        self.assertEqual(total_new, 2)  # Only two new records should be added
        self.assertEqual(
            Dataset.objects.count(), 3
        )  # Total three records in the database


class ParseExcelFileForDatasetsTest(TestCase):
    def setUp(self):
        # Create a mock Excel file
        self.workbook = openpyxl.Workbook()
        sheet = self.workbook.active
        assert sheet

        sheet.title = "Test Sheet"
        sheet.append(["Title", "Test Data Title"])  # type:ignore
        sheet.append(["Source", "Test Data Source"])  # type:ignore
        sheet.append(["Description", "Test Data Description"])  # type:ignore
        sheet.append(["Date", "Value"])  # type:ignore
        sheet.append(["2020-01-01", 123.45])  # type:ignore
        sheet.append(["2020-01-02", 678.90])  # type:ignore

        self.excel_file = BytesIO()
        self.workbook.save(self.excel_file)
        self.excel_file.seek(0)  # Rewind the BytesIO object to the beginning

    def test_parse_excel_file_for_datasets(self):
        # Call the function with the mock Excel file
        results = parse_excel_file_for_datasets(
            SimpleUploadedFile("file", self.excel_file.read())
        )

        # Assertions
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], "Test Sheet")
        self.assertTrue(results[0][1])  # created should be True
        self.assertEqual(results[0][2], 2)  # total_new should be 2

        # Verify that add_dataset was called with the correct arguments
        expected_dataset = [
            (datetime(2020, 1, 1, tzinfo=pytz.utc), 123.45),
            (datetime(2020, 1, 2, tzinfo=pytz.utc), 678.90),
        ]

        self.assertEqual(DatasetMetadata.objects.count(), 1)
        metadata = DatasetMetadata.objects.first()
        assert metadata
        self.assertEqual(metadata.internal_name, "Test Sheet")
        self.assertEqual(metadata.external_name, "Test Data Title")
        self.assertEqual(metadata.source, "Test Data Source")
        self.assertEqual(metadata.description, "Test Data Description")

        self.assertEqual(Dataset.objects.count(), 2)
        dataset = Dataset.objects.all()
        self.assertEqual((dataset[0].date, dataset[0].value), expected_dataset[0])
        self.assertEqual((dataset[1].date, dataset[1].value), expected_dataset[1])

    def test_parse_excel_file_for_datasets_with_datetime_dates(self):
        f = open(Path(__file__).parent / "../data/fast.xlsx", "rb")
        results = parse_excel_file_for_datasets(SimpleUploadedFile("file", f.read()))
        f.close()

        # Assertions
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], "Fastenal Daily Sales")
        self.assertTrue(results[0][1])  # created should be True
        self.assertEqual(results[0][2], 62)  # total_new should be 2

        expected_dataset = [
            (datetime(2019, 1, 31, tzinfo=pytz.utc), 20314.13636),
            (datetime(2019, 2, 28, tzinfo=pytz.utc), 20593.85),
        ]

        dataset = Dataset.objects.all()

        self.assertEqual(dataset[0].date, expected_dataset[0][0])
        self.assertAlmostEqual(dataset[0].value, expected_dataset[0][1], places=4)

        self.assertEqual(dataset[1].date, expected_dataset[1][0])
        self.assertAlmostEqual(dataset[1].value, expected_dataset[1][1], places=4)


class GetAllDatasetDfsTest(TransactionTestCase):
    def setUp(self):
        # Setup test data
        metadata1 = DatasetMetadata.objects.create(internal_name="Test Data 1")
        metadata2 = DatasetMetadata.objects.create(internal_name="Test Data 2")

        Dataset.objects.create(metadata=metadata1, date=datetime.now(), value=100)
        Dataset.objects.create(metadata=metadata1, date=datetime.now(), value=200)
        Dataset.objects.create(metadata=metadata2, date=datetime.now(), value=300)

    def test_get_all_postgres_dfs(self):
        # Call the function
        dfs = get_all_postgres_dfs()

        # Verify the DataFrames
        self.assertEqual(len(dfs), 2)
        self.assertIn("Test Data 1", dfs)
        self.assertIn("Test Data 2", dfs)

        # Further checks can be done on the content of the DataFrames
        df1 = dfs["Test Data 1"]
        self.assertIsInstance(df1, pd.DataFrame)
        self.assertEqual(list(df1.columns), ["Date", "Value"])
        self.assertEqual(len(df1), 2)  # Two rows for 'Test Data 1'

        df2 = dfs["Test Data 2"]
        self.assertIsInstance(df2, pd.DataFrame)
        self.assertEqual(len(df2), 1)  # One row for 'Test Data 2'
