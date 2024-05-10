from datetime import datetime
from datasets.management.commands.denylisted.manual import MANUAL_DENYLIST
from datasets.management.commands.denylisted.pairwise_clusters import PAIRWISE_CLUSTERS
import pytz
from datasets.models import DatasetMetadata
import requests
from django.conf import settings
from datasets.orm.dataset_orm import add_dataset_bulk

DENYLIST = MANUAL_DENYLIST + PAIRWISE_CLUSTERS


def validate_series(
    series_id: str, records: list[tuple[datetime, float]], metadata: dict | None = None
):
    # We want at least 2 years of data
    if len(records) < 24:
        return False

    if series_id in DENYLIST:
        return False

    # If name has discontinued, skip
    if metadata:
        if title := metadata.get("title"):
            if "DISCONTINUED" in title:
                return False

    # Data should have most recent data in 2023
    most_recent_datapoint = max(records)
    if most_recent_datapoint[0] < datetime(
        year=2022, month=12, day=31, tzinfo=pytz.utc
    ):
        return False

    return True


def add_fred_series(series_id: str, metadata: dict | None = None):
    records = fetch_fred_data(series_id)

    if metadata is None:
        series_metadata = fetch_fred_metadata(series_id)
        if series_metadata is None:
            print(f"No data found for series {series_id}")
            return

        if not validate_series(series_id, records, metadata):
            # Delete the metadata if series is invalid
            DatasetMetadata.objects.filter(internal_name=series_id).delete()
            print("Skipping series", series_id)
            return

        dataset_metadata, _ = DatasetMetadata.objects.get_or_create(
            internal_name=series_id,
            defaults={
                "external_name": series_metadata["title"],
                "source": "FRED",
                "description": series_metadata.get("notes"),
                "popularity": series_metadata.get("popularity"),
                "group_popularity": series_metadata.get("group_popularity"),
            },
        )
        new = add_dataset_bulk(records, dataset_metadata)
        print(f"added {new} records for {series_id}")
    else:
        defaults = {
            "external_name": metadata[series_id]["title"],
            "source": "FRED",
            "description": metadata[series_id]["description"],
            "popularity": metadata[series_id]["popularity"],
            "group_popularity": metadata[series_id]["group_popularity"],
            "hidden": metadata["set_hidden"],
        }
        if metadata.get("sub_source"):
            defaults["sub_source"] = metadata["sub_source"]
        dataset_metadata, _ = DatasetMetadata.objects.get_or_create(
            internal_name=series_id,
            defaults=defaults,
        )
        add_dataset_bulk(records, dataset_metadata)

    return dataset_metadata


def fetch_fred_data(series_id, stdout=None) -> list[tuple[datetime, float]]:
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations?series_id="
    API_KEY = settings.FRED_API_KEY
    url = f"{BASE_URL}{series_id}&api_key={API_KEY}&file_type=json"

    # Fetch the data from FRED
    response = requests.get(url)
    data = response.json()

    error = data.get("error_code")
    while error is not None:
        import time

        time.sleep(10)
        response = requests.get(url)
        data = response.json()
        error = data.get("error_code")

    observations = data.get("observations")
    if observations is None:
        print(f"No data found for series {series_id}")
        return []

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


def fetch_fred_metadata(series_id: str):
    BASE_URL = "https://api.stlouisfed.org/fred/series?series_id="
    API_KEY = settings.FRED_API_KEY
    url = f"{BASE_URL}{series_id}&api_key={API_KEY}&file_type=json"

    response = requests.get(url)
    data = response.json()
    error = data.get("error_code")
    while error is not None:
        import time

        time.sleep(10)
        response = requests.get(url)
        data = response.json()
        error = data.get("error_code")

    metadata = data.get("seriess")
    if metadata is None:
        return None
    return metadata[0]


def fetch_fred_series(tags: list[str]):
    BASE_URL = "https://api.stlouisfed.org/fred/tags/series?tag_names="
    API_KEY = settings.FRED_API_KEY
    tag_string = ""
    for i, tag in enumerate(tags):
        if i != 0:
            tag_string += ";"
        tag_string += tag

    series = []
    while True:
        url = f"{BASE_URL}{tag_string}&api_key={API_KEY}&file_type=json&offset={len(series)}"

        response = requests.get(url)
        data = response.json()
        if len(data["seriess"]) == 0:
            break
        series.extend(data["seriess"])

    return series
