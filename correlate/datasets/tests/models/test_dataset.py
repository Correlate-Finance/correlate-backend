from django.test import TransactionTestCase
from datasets.models import Dataset, DatasetMetadata
from datetime import datetime
from django.core.exceptions import ValidationError


class DatasetModelTest(TransactionTestCase):
    def setUp(self):
        # Set up non-modified objects used by all test methods
        self.metadata = DatasetMetadata.objects.create(internal_name="Test Metadata")
        Dataset.objects.create(
            metadata=self.metadata, date=datetime.utcnow(), value=123.45
        )
        self.date = datetime.utcnow()

    def test_metadata_label(self):
        dataset = Dataset.objects.get(metadata=self.metadata)
        field_label = dataset._meta.get_field("metadata").verbose_name
        self.assertEqual(field_label, "metadata")

    def test_date_label(self):
        dataset = Dataset.objects.get(metadata=self.metadata)
        field_label = dataset._meta.get_field("date").verbose_name
        self.assertEqual(field_label, "date")

    def test_value_label(self):
        dataset = Dataset.objects.get(metadata=self.metadata)
        field_label = dataset._meta.get_field("value").verbose_name
        self.assertEqual(field_label, "value")

    def test_created_at_label(self):
        dataset = Dataset.objects.get(metadata=self.metadata)
        field_label = dataset._meta.get_field("created_at").verbose_name
        self.assertEqual(field_label, "created at")

    def test_dataset_creation(self):
        dataset = Dataset.objects.get(metadata=self.metadata)
        self.assertTrue(isinstance(dataset, Dataset))

    def test_dataset_str(self):
        dataset = Dataset.objects.get(metadata=self.metadata)
        expected_object_name = f"{dataset.metadata.name}"
        self.assertEqual(expected_object_name, str(dataset))

    def test_dataset_uniqueness(self):
        # Creating a dataset instance
        Dataset.objects.create(date=self.date, metadata=self.metadata, value=123.45)

        # Attempting to create a second dataset instance with the same date and metadata
        duplicate_dataset = Dataset(
            date=self.date, metadata=self.metadata, value=678.90
        )

        # Expecting a ValidationError due to the unique_together constraint
        with self.assertRaises(ValidationError):
            duplicate_dataset.full_clean()  # This method is used to manually invoke validation
            duplicate_dataset.save()
