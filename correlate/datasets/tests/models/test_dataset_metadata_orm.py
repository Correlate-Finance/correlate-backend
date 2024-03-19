from django.test import TestCase
from datasets.models import DatasetMetadata, CorrelateDataPoint
from datasets.dataset_metadata_orm import augment_with_metadata


class AugmentWithMetadataTests(TestCase):
    def setUp(self):
        # Set up test data
        DatasetMetadata.objects.create(
            internal_name="test1",
            external_name="Test 1",
            source="Source 1",
            description="Description 1",
        )
        DatasetMetadata.objects.create(
            internal_name="test2",
            external_name="Test 2",
            source="Source 2",
            description="Description 2",
        )

    def test_augment_with_metadata(self):
        # Create test CorrelateDataPoints
        data_points = [
            CorrelateDataPoint(
                title="test1",
                pearson_value=0.5,
                p_value=0.05,
                dates=[],
                input_data=[],
                dataset_data=[],
            ),
            CorrelateDataPoint(
                title="test2",
                pearson_value=0.6,
                p_value=0.06,
                dates=[],
                input_data=[],
                dataset_data=[],
            ),
            # ... more test data points ...
        ]

        # Call the function to be tested
        augmented_data_points = augment_with_metadata(data_points)

        # Assert the outcomes
        self.assertEqual(augmented_data_points[0].title, "Test 1")
        self.assertEqual(augmented_data_points[0].source, "Source 1")
        self.assertEqual(augmented_data_points[0].description, "Description 1")

    def test_no_matching_metadata(self):
        data_points = [
            CorrelateDataPoint(
                title="nonexistent",
                pearson_value=0.5,
                p_value=0.05,
                dates=[],
                input_data=[],
                dataset_data=[],
            ),
        ]
        augmented_data_points = augment_with_metadata(data_points)
        self.assertEqual(len(augmented_data_points), len(data_points))
        self.assertIsNone(augmented_data_points[0].source)
        self.assertIsNone(augmented_data_points[0].description)
        self.assertEqual(data_points[0].title, augmented_data_points[0].title)

    def test_empty_datasets_list(self):
        augmented_data_points = augment_with_metadata([])
        self.assertEqual(len(augmented_data_points), 0)
