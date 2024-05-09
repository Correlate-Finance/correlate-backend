from unittest import TestCase
from datasets.lib.date import get_date_from_days_since_1900
from datetime import datetime


class TestGetDateFromDaysSince1900(TestCase):
    def test_get_date_from_days_since_1900(self):
        # Test for the very first day (should return 1899-12-31 because of Excel bug)
        self.assertEqual(get_date_from_days_since_1900(1), datetime(1899, 12, 31))

        # Test for a non-leap year (should return 1900-01-30)
        self.assertEqual(get_date_from_days_since_1900(31), datetime(1900, 1, 30))

        # Test for the Excel leap year bug (should return 1900-02-27)
        self.assertEqual(get_date_from_days_since_1900(59), datetime(1900, 2, 27))

        # Test for the day after the Excel leap year (should return 1900-03-01)
        self.assertEqual(get_date_from_days_since_1900(61), datetime(1900, 3, 1))

        # Test for a regular date (should return 2017-09-23 for 43001 days since 1900)
        self.assertEqual(get_date_from_days_since_1900(43001), datetime(2017, 9, 23))
