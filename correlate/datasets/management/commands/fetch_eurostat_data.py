# prompt: Generate querystring from urlparams

import urllib.parse
import urllib.request
from collections import defaultdict
import itertools
import requests


BASE_URL = "http://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"


BASE_URL_PARAMS = {
    "lang": "EN",
    "geo": "EA20",
    "freq": "M",
    "sinceTimePeriod": "2000",
    "unit": "mio_m3",
    "partner": "total",
}


results = []
lengths = []

def get_eurostat_data(series_id):
    global urlparams
    global results
    global lengths
    global BASE_URL

querystring = urllib.parse.urlencode(urlparams)
URL = BASE_URL + series_id + "?" + querystring
response = requests.get(URL)
data = response.json()

size = data["size"]
dimensions = data["dimension"]

parsed_dimensions = defaultdict(list)
for key, dimension in dimensions.items():
    if key == "time":
        continue

    label = dimension["label"]
    categories = dimension["category"]["index"]

    for category in categories:
        parsed_dimensions[key].append(category)

permutations = []
for key, values in parsed_dimensions.items():
    permutations.append(list(itertools.product([key], values)))

all_permutations = list(itertools.product(*permutations))
print(all_permutations)

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
    print(URL)
    response = requests.get(URL)
    data = response.json()

    # Process the data here
    results.append(data)
    lengths.append(data["value"])


def read_eurostat_csv(file):
    # file = "./data/Eurostat.csv"
    with open(file, "r") as f:
        lines = f.readlines()

    headers = lines[0].strip().split(",")
    data = []
    for line in lines[1:]:
        data.append(dict(zip(headers, line.strip().split(","))))

    return data