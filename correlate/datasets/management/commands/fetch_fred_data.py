from django.core.management.base import BaseCommand
from django.conf import settings
from datasets.models import DatasetMetadata
from datasets.dataset_orm import add_dataset
import requests
from datetime import datetime
import pytz


class Command(BaseCommand):
    help = "Fetches the latest data from FRED and updates the database."

    def add_arguments(self, parser):
        parser.add_argument("series_id", type=str, help="The series id to fetch")

    def handle(self, *args, **options):
        series_id = options["series_id"]
        print(series_id)
        self.stdout.write(f"Fetching data for series {series_id}")

        # Fetch the data from FRED
        records = fetch_fred_data(series_id)
        title = fetch_fred_title(series_id)
        dataset_metadata, _ = DatasetMetadata.objects.get_or_create(
            internal_name=series_id,
            defaults={"external_name": title, "source": "FRED"},
        )
        total_new = add_dataset(records, dataset_metadata)
        self.stdout.write(f"Added {total_new} new records to the database")


def fetch_fred_data(series_id):
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations?series_id="
    API_KEY = settings.FRED_API_KEY
    url = f"{BASE_URL}{series_id}&api_key={API_KEY}&file_type=json"

    # Fetch the data from FRED
    response = requests.get(url)
    data = response.json()
    observations = data["observations"]
    records = [
        (
            datetime.strptime(obs["date"], "%Y-%m-%d").replace(tzinfo=pytz.utc),
            obs["value"],
        )
        for obs in observations
    ]
    return records


def fetch_fred_title(series_id):
    BASE_URL = "https://api.stlouisfed.org/fred/series?series_id="
    API_KEY = settings.FRED_API_KEY
    url = f"{BASE_URL}{series_id}&api_key={API_KEY}&file_type=json"

    response = requests.get(url)
    data = response.json()
    return data["seriess"][0]["title"]
