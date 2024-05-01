from django.core.management.base import BaseCommand
from datasets.models import Dataset, DatasetMetadata
from datasets.orm.dataset_orm import add_dataset_bulk
from datasets.management.commands.denylisted.manual import MANUAL_DENYLIST
from datasets.management.commands.denylisted.pairwise_clusters import PAIRWISE_CLUSTERS
from datasets.management.commands.fetch_fred_data import fetch_fred_data
from datetime import datetime, timedelta

DENYLIST = MANUAL_DENYLIST + PAIRWISE_CLUSTERS


class Command(BaseCommand):
    help = "Fetches the latest data from FRED and updates the database."

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        datasets = DatasetMetadata.objects.filter(
            source__in=["FRED", "Census", "BLS", "BEA", "Board of Governors"],
            hidden=False,
        )

        # For each dataset fetch data and look for updates
        for dataset in datasets:
            records = fetch_fred_data(dataset.internal_name, self.stdout)
            stored_records = Dataset.objects.filter(metadata=dataset).values_list(
                "date", "value"
            )

            # Check if the data is already stored
            stored_dates = set([record[0] for record in stored_records])
            new_records = [
                record for record in records if record[0] not in stored_dates
            ]

            records_to_update = [
                record
                for record in (set(records) - set(stored_records))
                if record not in new_records
            ]

            if new_records:
                add_dataset_bulk(new_records, dataset)

            if records_to_update:
                for record in records_to_update:
                    Dataset.objects.filter(metadata=dataset, date=record[0]).update(
                        value=record[1]
                    )

            dataset.updated_at = datetime.now()
            dataset.save()

            self.stdout.write(
                self.style.SUCCESS(
                    f"Added {len(new_records)} & Updated {len(records_to_update)} records for {dataset.internal_name}"
                )
            )
