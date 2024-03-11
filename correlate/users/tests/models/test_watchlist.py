from django.test import TestCase
from users.models import User, WatchList
from datasets.models import DatasetMetadata


class WatchlistModelTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        # Set up non-modified objects used by all test methods
        cls.user = User.objects.create(email="testuser", password="12345")
        cls.dataset_metadata = DatasetMetadata.objects.create(
            internal_name="Dataset 1", description="Sample dataset"
        )

    def test_watchlist_creation(self):
        # Test creating and saving a new WatchList instance
        watchlist = WatchList.objects.create(
            user=self.user, dataset=self.dataset_metadata
        )
        self.assertIsInstance(watchlist, WatchList)

    def test_watchlist_fields(self):
        # Test the fields of the WatchList instance
        watchlist = WatchList.objects.create(
            user=self.user, dataset=self.dataset_metadata
        )
        self.assertEqual(watchlist.user.email, "testuser")
        self.assertEqual(watchlist.dataset.name, "Dataset 1")
