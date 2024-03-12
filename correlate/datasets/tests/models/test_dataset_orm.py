from django.test import TransactionTestCase
from datasets.models import Dataset, DatasetMetadata
from datasets.dataset_orm import add_dataset, parse_excel_file_for_datasets
from datetime import datetime

from django.test import TestCase
from io import BytesIO
import openpyxl
import pytz
from django.core.files.uploadedfile import SimpleUploadedFile


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
