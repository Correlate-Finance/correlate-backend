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
        parser.add_argument("--series_id", type=str, help="The series id to fetch")
        parser.add_argument("--tag", type=str, nargs="+", help="The tags to fetch")
        parser.add_argument(
            "--skip_existing",
            action="store_true",
            help="Skip existing data",
            default=False,
        )
        parser.add_argument(
            "--set_hidden",
            action="store_true",
            help="Mark ingested data as hidden",
            default=False,
        )
        parser.add_argument(
            "--sub_source",
            type=str,
            help="Set the sub source",
            default=None,
        )

    def handle(self, *args, **options):
        series_id = options["series_id"]
        tags = options["tag"]
        skip_existing = options["skip_existing"]
        set_hidden = options["set_hidden"]
        sub_source = options["sub_source"]

        series = []
        metadata = {}
        if series_id:
            series = [series_id]
        elif tags:
            series_response = fetch_fred_series(tags)
            for s in series_response:
                metadata[s["id"]] = {
                    "title": s["title"],
                    "description": s.get("notes"),
                    "popularity": s.get("popularity"),
                    "group_popularity": s.get("group_popularity"),
                }
                series.append(s["id"])

        for series_id in series:
            if (
                skip_existing
                and DatasetMetadata.objects.filter(internal_name=series_id).exists()
            ):
                self.stdout.write(f"Skipping series {series_id}")
                continue

            self.stdout.write(f"Fetching data for series {series_id}")

            # Fetch the data from FRED
            records = fetch_fred_data(series_id, self.stdout)

            if len(metadata) == 0:
                series_metadata = fetch_fred_metadata(series_id)
                dataset_metadata, _ = DatasetMetadata.objects.get_or_create(
                    internal_name=series_id,
                    defaults={
                        "external_name": series_metadata["title"],
                        "source": "FRED",
                        "description": series_metadata["notes"],
                    },
                )
                total_new = add_dataset(records, dataset_metadata)
                self.stdout.write(f"Added {total_new} new records to the database")
            else:
                defaults = {
                    "external_name": metadata[series_id]["title"],
                    "source": "FRED",
                    "description": metadata[series_id]["description"],
                    "popularity": metadata[series_id]["popularity"],
                    "group_popularity": metadata[series_id]["group_popularity"],
                    "hidden": set_hidden,
                }
                if sub_source:
                    defaults["sub_source"] = sub_source
                dataset_metadata, _ = DatasetMetadata.objects.get_or_create(
                    internal_name=series_id,
                    defaults=defaults,
                )
                total_new = add_dataset(records, dataset_metadata)
                self.stdout.write(f"Added {total_new} new records to the database")


def fetch_fred_data(series_id, stdout=None) -> list[tuple[datetime, float]]:
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations?series_id="
    API_KEY = settings.FRED_API_KEY
    url = f"{BASE_URL}{series_id}&api_key={API_KEY}&file_type=json"

    # Fetch the data from FRED
    response = requests.get(url)
    data = response.json()
    observations = data["observations"]
    records = []
    for observation in observations:
        try:
            date = datetime.strptime(observation["date"], "%Y-%m-%d").replace(
                tzinfo=pytz.utc
            )
            value = float(observation["value"])

            # skip dates before 2000
            if date < datetime(2000, 1, 1, tzinfo=pytz.utc):
                continue
            records.append((date, value))
        except ValueError:
            if stdout:
                stdout.write(
                    f"Unable to parse datapoint {observation['value']} for FRED dataset {series_id}"
                )

    return records


def fetch_fred_metadata(series_id):
    BASE_URL = "https://api.stlouisfed.org/fred/series?series_id="
    API_KEY = settings.FRED_API_KEY
    url = f"{BASE_URL}{series_id}&api_key={API_KEY}&file_type=json"

    response = requests.get(url)
    data = response.json()
    return data["seriess"][0]


def fetch_fred_series(tags: list[str]):
    BASE_URL = "https://api.stlouisfed.org/fred/tags/series?tag_names="
    API_KEY = "b1161c8cd782fd6e42684d6578b08d83"
    tag_string = ""
    for i, tag in enumerate(tags):
        if i != 0:
            tag_string += ";"
        tag_string += tag

    url = f"{BASE_URL}{tag_string}&api_key={API_KEY}&file_type=json"

    response = requests.get(url)
    data = response.json()
    return data["seriess"]
