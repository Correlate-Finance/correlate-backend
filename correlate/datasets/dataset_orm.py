from .models import DatasetMetadata, Dataset
from datetime import datetime
import openpyxl
from django.core.files.uploadedfile import UploadedFile
import pytz
import pandas as pd
from dateutil.parser import parse
from frozendict import frozendict
from django.conf import settings
from core.data_processing import transform_data_base
from datasets.mongo_operations import (
    get_all_mongo_dfs,
    get_mongo_df,
)
from ddtrace import tracer

CACHED_DFS = None


def add_dataset(records: list[tuple[datetime, float]], metadata: DatasetMetadata):
    total_new = 0
    for record in records:
        date = record[0]
        value = record[1]
        _, created = Dataset.objects.get_or_create(
            metadata=metadata, date=date, value=value
        )
        if created:
            total_new += 1

    return total_new


def add_dataset_bulk(records: list[tuple[datetime, float]], metadata: DatasetMetadata):
    to_add = []
    for record in records:
        date = record[0]
        value = record[1]
        to_add.append(Dataset(metadata=metadata, date=date, value=value))
    added = len(Dataset.objects.bulk_create(to_add))
    return added


def parse_excel_file_for_datasets(excel_file: UploadedFile):
    workbook = openpyxl.load_workbook(filename=excel_file, data_only=True)

    results = []

    for sheet in workbook:
        # Step 1: Parse Metadata
        metadata = {}

        dataset: list[tuple[datetime, float]] = []
        data_start = False
        for row in sheet.iter_rows():
            if row[0].value == "Date" and row[1].value == "Value":
                data_start = True  # Found the dataset header
                continue

            if not data_start:
                # Until data starts we keep parsing metadata
                key, value = [
                    cell.value for cell in row[:2]
                ]  # Assuming key-value in first two columns
                if key and value:  # Check if both key and value are present
                    metadata[key] = value

            else:
                raw_date, value = str(row[0].value), row[1].value

                if raw_date and value:  # Check if both date and value are present
                    date = (
                        parse(raw_date).replace(tzinfo=pytz.utc)
                        if isinstance(raw_date, str)
                        else raw_date
                    )
                    dataset.append((date, float(value)))  # type:ignore

                if not row[0].value and not row[1].value:
                    # Stop parsing if we find an empty row after the dataset
                    break

        dataset_metadata, created = DatasetMetadata.objects.get_or_create(
            internal_name=sheet.title,
            defaults=dict(
                external_name=metadata.get("Title", sheet.title),
                source=metadata.get("Source", None),
                description=metadata.get("Description", None),
            ),
        )

        total_new = add_dataset(dataset, dataset_metadata)
        results.append((sheet.title, created, total_new))
    return results


@tracer.wrap("get_all_postgres_dfs")
def get_all_postgres_dfs(
    selected_names: list[str] | None = None,
) -> frozendict[str, pd.DataFrame]:
    global CACHED_DFS
    if CACHED_DFS:
        if selected_names is None:
            return CACHED_DFS
        else:
            return frozendict(
                {
                    name: CACHED_DFS[name]
                    for name in selected_names
                    if name in CACHED_DFS
                }
            )

    dfs = {}
    if selected_names is not None:
        datasets = Dataset.objects.filter(
            metadata__internal_name__in=selected_names
        ).prefetch_related("metadata")
    else:
        dataset_metadatas = (
            DatasetMetadata.objects.all()
            .order_by("created_at")
            .values_list("id", flat=True)[:2000]
        )
        datasets = Dataset.objects.filter(
            metadata_id__in=dataset_metadatas
        ).prefetch_related("metadata")

    datasets = datasets.values_list("metadata__internal_name", "date", "value")
    for dataset in datasets:
        title = dataset[0]
        if title not in dfs:
            dfs[title] = []
        dfs[title].append((dataset[1], dataset[2]))

    for title, data in dfs.items():
        dfs[title] = pd.DataFrame(data, columns=["Date", "Value"])
        transform_data_base(dfs[title])

    if selected_names is None:
        CACHED_DFS = dfs
    return frozendict(dfs)


@tracer.wrap("get_postgres_df")
def get_postgres_df(title: str) -> pd.DataFrame | None:
    if CACHED_DFS:
        return CACHED_DFS.get(title)
    dataset = list(Dataset.objects.filter(metadata__internal_name=title).all())
    if len(dataset) == 0:
        return None
    data = [(d.date, d.value) for d in dataset]
    return pd.DataFrame(data, columns=["Date", "Value"])


def get_all_dfs(
    selected_names: list[str] | None = None,
) -> frozendict[str, pd.DataFrame]:
    return (
        get_all_postgres_dfs(selected_names)
        if settings.USE_POSTGRES_DATASETS
        else get_all_mongo_dfs(selected_names)
    )


def get_df(title: str) -> pd.DataFrame | None:
    return (
        get_postgres_df(title)
        if settings.USE_POSTGRES_DATASETS
        else get_mongo_df(title)
    )
