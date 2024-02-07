# this is the dataset_service

from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import certifi
from frozendict import frozendict
from functools import cache, wraps


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


def fetch_data_table_ids(db, selected_name=None):
    category_data = fetch_category_names(db)

    dataTable_documents = db["dataTable"]
    if selected_name is not None:
        selected_id = category_data[selected_name]
        dataTable_documents = dataTable_documents.find(
            {"category": selected_id}, {"title": 1, "_id": 1}
        )
    else:
        dataTable_documents = dataTable_documents.find({}, {"title": 1, "_id": 1})

    dataTable_ids = {}
    for document in dataTable_documents:
        if "_id" in document and "title" in document:
            dataTable_ids[str(document["_id"])] = document["title"]

    return dataTable_ids


def get_all_dfs() -> dict[str, pd.DataFrame]:
    mongo_uri = "mongodb+srv://cmd2:VXSkRSG3kbRLIoJd@cluster0.fgu6ofc.mongodb.net/?retryWrites=true&w=majority"
    database_name = "test"

    db = connect_to_mongo(mongo_uri, database_name)

    dataTable_ids = fetch_data_table_ids(db)
    dfs = fetch_data_frames(db, dataTable_ids)
    db.client.close()
    return dfs


def freezeargs(func):
    """Transform mutable dictionnary
    Into immutable
    Useful to be compatible with cache
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
