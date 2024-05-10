import requests
from datetime import datetime
from datasets.models import DatasetMetadata
from datasets.orm.dataset_orm import add_dataset_bulk
from django.conf import settings
import pytz


BLOCKED_SERIES = [
    "BOIMPUS",  # Data only had 7 data points, most recent data was not available
    "HPC9PUS",  # Data had 11 data points, most data was not available and all values were same
    "HPI9PUS",  # Data had 11 data points, most data was not available and all values were 0
    "NUC9PUS",  # Data had 11 data points, most data was not available and all values were 0
    "NUI9PUS",  # Data had 11 data points, most data was not available and all values were 0
    "HPC9SUS",  # Data had 24 data points and most recent data was not available
    "BMACBUS",  # Data is duplicate of "REACBUS"
    # Data does not have data newer than 2022
    "RLUCUUS",
    "ESOTUUS",
    "CLRCPUS",
    "CLRCBUS",
    "CLRCEUS",
    "COQIPUS",
    "D2RCOUS",
    "D2RCAUS",
    "NGWPUUS",
    "HPC9SUS",
    "COFMUAQ",
    "COFMUVE",
    "COFMUUK",
    "COFMUNI",
    "COIMUAQ",
    "PAIMPVI",
    "GEC9PUS",
    "RBTCUUS",
    "COFMUSA",
    "D2WHUUS",
    "RFTCUUS",
    "ROWHUUS",
    "MGTCUUS",
    "JKWHUUS",
    "D2TCUUS",
    "PRWHUUS",
    "OGWSPUS",
    "DSWHUUS",
    "ROTCUUS",
    "DSTCUUS",
    "MGWHUUS",
    "KSTCUUS",
    "AVWHUUS",
    "AVTCUUS",
    "JKTCUUS",
    "RFWHUUS",
    "RBWHUUS",
    "KSWHUUS",
    "PRTCUUS",
    "COFMUCL",
    # Data has too many 0 points, as per analysis from Ryan. Check sheet output_data
    "PCCCEUS",
    "BTI9SUS",
    "NGIMPMX",
    "CLACPUS",
    "CLACBUS",
    "CLACEUS",
    "LNIMPUA",
    "LNIMPAU",
    "LNIMPOM",
    "LNIMPNO",
    "LNIMPYE",
    "LNEXPFR",
    "LNEXPTU",
    "LNEXPUK",
    "LNIMPQA",
    "LNEXPCE",
    "LNEXPBZ",
    "LNEXPCH",
    "LNIMPEY",
    "LNIMPNI",
    "LNEXPSP",
    "LNIMPAG",
    "LNIMPOT",
    "LNEXPIH",
    "LNEXPSK",
    "LNEXPOT",
    "PAIMPEN",
    "PAIMPAO",
    "PCCCBUS",
    "PCCCPUS",
    "GEC9SUS",
    "LNEXPJA",
    "LNIMPTD",
]


def fetch_all_eia_series() -> list[str]:
    total_energy_datasets = requests.get(
        f"https://api.eia.gov/v2/total-energy/facet/msn?api_key={settings.EIA_API_KEY}"
    ).json()
    facets = total_energy_datasets["response"]["facets"]
    return [facet["id"] for facet in facets]


def fetch_and_store_eia_series(series_id: str, stdout=None):
    print(f"Fetching data for series {series_id}")
    # Fetch the data from EIA
    data = fetch_eia_data(series_id)

    if len(data["data"]) == 0:
        print(f"No data found for series {series_id}")
        return

    records = fetch_records_from_eia_data(data["data"], series_id)

    dataset_metadata, _ = DatasetMetadata.objects.get_or_create(
        internal_name=series_id,
        defaults={
            "external_name": data["data"][0]["seriesDescription"],
            "description": data["description"],
            "source": "EIA",
        },
    )

    total_new = add_dataset_bulk(records, dataset_metadata)
    print(f"Added {total_new} new records to the database")


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
