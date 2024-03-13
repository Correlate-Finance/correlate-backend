from django.core.management.base import BaseCommand, CommandParser
from django.conf import settings
from datasets.models import DatasetMetadata
from datasets.dataset_orm import add_dataset
import requests
from datetime import datetime
import pytz


class Command(BaseCommand):
    help = "Fetches the latest data from EIA and updates the database."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "n",
            type=int,
            help="The max number of series to fetch",
            nargs="*",
            default=1000,
        )
        return super().add_arguments(parser)

    def handle(self, *args, **options):
        all_series = fetch_all_eia_series()
        for series_id in all_series[: options["n"]]:
            fetch_and_store_eia_series(series_id, self.stdout)


def fetch_all_eia_series() -> list[str]:
    total_energy_datasets = requests.get(
        f"https://api.eia.gov/v2/total-energy/facet/msn?api_key={settings.EIA_API_KEY}"
    ).json()
    facets = total_energy_datasets["response"]["facets"]
    return [facet["id"] for facet in facets]


def fetch_and_store_eia_series(series_id: str, stdout):
    stdout.write(f"Fetching data for series {series_id}")
    # Fetch the data from EIA
    data = fetch_eia_data(series_id)

    if len(data["data"]) == 0:
        stdout.write(f"No data found for series {series_id}")
        return

    records = fetch_records_from_eia_data(data["data"], series_id, stdout)

    dataset_metadata, _ = DatasetMetadata.objects.get_or_create(
        internal_name=series_id,
        defaults={
            "external_name": data["data"][0]["seriesDescription"],
            "description": data["description"],
            "source": "EIA",
        },
    )

    total_new = add_dataset(records, dataset_metadata)
    stdout.write(f"Added {total_new} new records to the database")


def fetch_eia_data(series_id) -> dict[str, list[dict]]:
    BASE_URL = "https://api.eia.gov/v2/total-energy/data/?frequency=monthly&data[0]=value&facets[msn][]={series_id}&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000&api_key={api_key}"
    url = BASE_URL.format(series_id=series_id, api_key=settings.EIA_API_KEY)

    # Fetch the data from EIA
    response = requests.get(url)
    data = response.json()
    observations = data["response"]
    return observations


def fetch_records_from_eia_data(
    observations, series_id, stdout=None
) -> list[tuple[datetime, float]]:
    records = []
    for observation in observations:
        try:
            date = datetime.strptime(observation["period"], "%Y-%m").replace(
                tzinfo=pytz.utc
            )
            value = float(observation["value"])
            records.append((date, value))
        except ValueError:
            if stdout:
                stdout.write(
                    f"Unable to parse datapoint {observation['value']} for EIA dataset {series_id}"
                )

    return records
