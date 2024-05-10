# prompt: Generate querystring from urlparams

import urllib.parse
import urllib.request
from collections import defaultdict
import itertools
import requests
from datasets.models import DatasetMetadata
from dateutil import parser
from datasets.orm.dataset_orm import add_dataset_bulk


BASE_URL = "http://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"


BASE_URL_PARAMS = {
    "lang": "EN",
    "freq": "M",
    "sinceTimePeriod": "2024",
}


def parse_and_store_observations(
    response,
    suffix="",
):
    data = response["data"]
    url = response["url"]
    series_id = data["extension"]["id"] + suffix
    name = data["label"]

    values = data["value"]
    if len(values) == 0:
        print("No values found for", name)
        return

    index = data["dimension"]["time"]["category"]["index"]

    defaults = {
        "external_name": name,
        "source": "ESTAT",
        "description": name + "\n" + url + "\n" + str(response["params"]),
        "hidden": True,
        "url": url,
    }

    data_points = []
    for k, v in index.items():
        if str(v) not in values:
            continue

        date = parser.parse(k).replace(day=1)
        data_points.append((date, float(values[str(v)])))

    dataset_metadata = DatasetMetadata.objects.create(
        internal_name=series_id, **defaults
    )
    add_dataset_bulk(data_points, dataset_metadata)


def get_eurostat_data(series_id, params, suffix=""):
    querystring = urllib.parse.urlencode(params)
    URL = BASE_URL + series_id + "?" + querystring
    response = requests.get(URL)
    data = response.json()
    results = []

    if "error" in data:
        print("Error in fetching data")
        print(data)
        return

    dimensions = data["dimension"]

    parsed_dimensions = defaultdict(list)
    for key, dimension in dimensions.items():
        if key == "time":
            continue

        categories = dimension["category"]["index"]

        for category in categories:
            parsed_dimensions[key].append(category)

    permutations = []
    for key, values in parsed_dimensions.items():
        permutations.append(list(itertools.product([key], values)))

    all_permutations = list(itertools.product(*permutations))
    print("Total permutations", len(all_permutations))
    i = 0
    for permutation in all_permutations:
        urlparams = {
            "lang": "EN",
            "freq": "M",
            "geo": "EA20",
        }

        for key, value in permutation:
            urlparams[key] = value

        querystring = urllib.parse.urlencode(urlparams)
        URL = BASE_URL + series_id + "?" + querystring
        print("Fetching data for", URL)
        response = requests.get(URL)
        data = response.json()
        i += 1

        # Process the data here
        parse_and_store_observations(
            {"data": data, "url": URL, "params": urlparams}, suffix=f"_{suffix}_{i}"
        )

    return results


def read_eurostat_csv(file):
    # file = "./data/Eurostat.csv"
    with open(file, "r") as f:
        lines = f.readlines()

    headers = lines[0].strip().split(",")

    data = []
    skip_headers = ["exclude", "Theme", "URL", "Source", "freq", ""]
    for line in lines[1:]:
        params = dict(zip(headers, line.strip().split(",")))
        # Skip row
        if params["exclude"] != "":
            continue

        for header in skip_headers:
            params.pop(header, None)

        data.append(params)

    return data


def get_eurostat_params(datasets):
    to_load = []

    for dataset in datasets:
        series_id = dataset["Code"]
        multi_params = []
        params = {
            "lang": "EN",
            "geo": "EA20",
            "freq": "M",
            "sinceTimePeriod": "2000",
        }
        multi_params.append(params)
        for key, value in dataset.items():
            if key == "Code":
                continue

            if value == "":
                continue

            values = value.split("&")
            params_to_add = []
            for param in multi_params:
                if len(values) == 1:
                    param[key] = value
                else:
                    copy = False
                    for val in values:
                        if copy:
                            new_param = param.copy()
                        else:
                            param[key] = val
                            copy = True
                            continue
                        new_param[key] = val
                        params_to_add.append(new_param)
            multi_params += params_to_add

        for params in multi_params:
            to_load.append((series_id, params))

    return to_load


def update_names():
    d = read_eurostat_csv("./data/Eurostat.csv")
    ds = {
        es["Code"]: DatasetMetadata.objects.filter(
            internal_name__startswtith=es["Code"]
        )
        for es in d
    }

    for series_id, dm in ds.items():
        feature_labels = defaultdict(dict)
        features = defaultdict(set)
        for f in dm:
            desc = f.description
            params = eval(desc.split("\n")[2])
            for k, v in params.items():
                features[k].add(v)

        querystring = urllib.parse.urlencode(BASE_URL_PARAMS)
        URL = BASE_URL + series_id + "?" + querystring
        response = requests.get(URL)
        data = response.json()
        dimensions = data["dimension"]
        for k, v in dimensions.items():
            for cat, name in v["category"]["label"].items():
                feature_labels[k][cat] = name

        for f in dm:
            desc = f.description
            params = eval(desc.split("\n")[2])
            new_name = f.external_name
            for k, v in params.items():
                if len(features[k]) > 1:
                    suffix = feature_labels[k][v]
                    new_name += " - " + suffix
            f.external_name = new_name
            f.save()
