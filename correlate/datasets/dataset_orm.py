from .models import DatasetMetadata, Dataset
from datetime import datetime
import openpyxl
from django.core.files.uploadedfile import UploadedFile
import pytz
import pandas as pd


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
                    date = datetime.strptime(raw_date, "%Y-%m-%d").replace(
                        tzinfo=pytz.utc
                    )
                    dataset.append((date, float(value)))  # type:ignore

                if not row[0].value and not row[1].value:
                    # Stop parsing if we find an empty row after the dataset
                    break

        dataset_metadata, created = DatasetMetadata.objects.get_or_create(
            internal_name=sheet.title,
            defaults=dict(
                external_name=metadata.get("Title", None),
                source=metadata.get("Source", None),
                description=metadata.get("Description", None),
            ),
        )

        total_new = add_dataset(dataset, dataset_metadata)
        results.append((sheet.title, created, total_new))
    return results


def get_all_dataset_dfs():
    dfs = {}
    datasets = Dataset.objects.all().prefetch_related("metadata")
    for dataset in datasets:
        title = dataset.metadata.name
        if title not in dfs:
            dfs[title] = []
        dfs[title].append((dataset.date, dataset.value))
    dfs = {
        title: pd.DataFrame(data, columns=["Date", "Value"])
        for title, data in dfs.items()
    }
    return dfs
