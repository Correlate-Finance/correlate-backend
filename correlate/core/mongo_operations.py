# this is the dataset_service

from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import certifi
from frozendict import frozendict
from functools import cache, wraps

MONGO_URI = "mongodb+srv://cmd2:VXSkRSG3kbRLIoJd@cluster0.fgu6ofc.mongodb.net/?retryWrites=true&w=majority"
DATABASE_NAME = "test"

HIGH_LEVEL_TABLES = [
    "heating_Days_united_states",
    "cooling_Days_united_states",
    "total us construction spend",
    "total ressie construction spend",
    "total non res const spend",
    "total private const spend",
    "total public construction spend",
    "manuf ex transport orders",
    "manuf ex defense orders",
    "duarble goods shipments",
    "total manufacturing orders",
    "manuf ex transport orders",
    "manuf ex defense orders",
    "manuf unfilled orders",
    "durable goods orders",
    "fast daily sales",
    "south_korea_exp_total",
    "domestic auto sales",
    "domestic auto production",
    "total dod o&m",
    "total dod procurement",
    "total dod rdt&e",
]


def connect_to_mongo(uri, db_name):
    client = MongoClient(uri, tlsCAFile=certifi.where())
    return client[db_name]


def fetch_category_names(db):
    category_collection = db["category"]
    category_documents = category_collection.find({}, {"_id": 1, "name": 1})
    category_data = {
        document["name"]: document["_id"] for document in category_documents
    }

    return category_data


def fetch_data_table_ids(
    db, selected_names: list[str] | None = None, selected_category: str | None = None
):
    dataTable_documents = db["dataTable"]
    if selected_category is not None:
        category_data = fetch_category_names(db)
        selected_id = category_data[selected_category]
        dataTable_documents = dataTable_documents.find(
            {"category": selected_id}, {"title": 1, "_id": 1}
        )
    elif selected_names is not None:
        dataTable_documents = dataTable_documents.find(
            {"title": {"$in": selected_names}}, {"title": 1, "_id": 1}
        )
    else:
        dataTable_documents = dataTable_documents.find({}, {"title": 1, "_id": 1})

    dataTable_ids = {}
    for document in dataTable_documents:
        if "_id" in document and "title" in document:
            dataTable_ids[str(document["_id"])] = document["title"]

    return dataTable_ids


def get_all_dfs(selected_names: list[str] | None = None) -> dict[str, pd.DataFrame]:
    db = connect_to_mongo(MONGO_URI, DATABASE_NAME)

    dataTable_ids = fetch_data_table_ids(db, selected_names=selected_names)
    dfs = fetch_data_frames(db, dataTable_ids)
    db.client.close()
    return dfs


def get_df(name: str) -> pd.DataFrame | None:
    db = connect_to_mongo(MONGO_URI, DATABASE_NAME)
    dataTable_ids = fetch_data_table_ids(db, selected_names=[name])
    dfs = fetch_data_frames(db, dataTable_ids)
    db.client.close()
    return dfs.get(name)


def freezeargs(func):
    """
    Transform mutable dictionary into immutable.
    Useful to be compatible with cache.
    """

    @wraps(func)
    def wrapped(*args, **kwargs):
        args = tuple(
            [frozendict(arg) if isinstance(arg, dict) else arg for arg in args]
        )
        kwargs = {
            k: frozendict(v) if isinstance(v, dict) else v for k, v in kwargs.items()
        }
        return func(*args, **kwargs)

    return wrapped


@freezeargs
@cache
def fetch_data_frames(
    db,
    dataTable_ids,
    date_threshold=datetime.strptime("1971-01-01", "%Y-%m-%d"),
    max=None,
) -> dict[str, pd.DataFrame]:
    data_collection = db["data"]
    dfs = {}

    for id_as_string, title in dataTable_ids.items():
        pipeline = [
            {
                "$match": {
                    "dataTable_Id": id_as_string,
                    "date": {"$gte": date_threshold},
                }
            },
            {"$project": {"date": 1, "value": 1}},
            {"$sort": {"date": -1}},
        ]
        data_documents = list(data_collection.aggregate(pipeline))
        dates = [doc["date"] for doc in data_documents]
        values = [doc["value"] for doc in data_documents]
        dfs[title] = pd.DataFrame({"Date": dates, "Value": values})

        if max and len(dfs) >= max:
            break
    return frozendict(dfs)
