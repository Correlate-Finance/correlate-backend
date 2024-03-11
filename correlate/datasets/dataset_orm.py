from .models import DatasetMetadata, Dataset
from datetime import datetime


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
