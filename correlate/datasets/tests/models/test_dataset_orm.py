from django.test import TransactionTestCase
from datasets.models import Dataset, DatasetMetadata
from datasets.dataset_orm import add_dataset
from datetime import datetime


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
