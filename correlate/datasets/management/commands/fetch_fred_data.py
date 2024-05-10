from django.core.management.base import BaseCommand
from datasets.models import Dataset, DatasetMetadata
from datasets.orm.dataset_orm import add_dataset_bulk
from adapters.fred import (
    fetch_fred_data,
    fetch_fred_metadata,
    fetch_fred_series,
    DENYLIST,
)


class Command(BaseCommand):
    help = "Fetches the latest data from FRED and updates the database."

    def add_arguments(self, parser):
        parser.add_argument("--series_id", type=str, help="The series id to fetch")
        parser.add_argument("--tag", type=str, nargs="+", help="The tags to fetch")
        parser.add_argument(
            "--skip_existing",
            action="store_true",
            help="Skip existing data",
            default=False,
        )
        parser.add_argument(
            "--set_hidden",
            action="store_true",
            help="Mark ingested data as hidden",
            default=False,
        )
        parser.add_argument(
            "--sub_source",
            type=str,
            help="Set the sub source",
            default=None,
        )

    def handle(self, *args, **options):
        series_id = options["series_id"]
        tags = options["tag"]
        skip_existing = options["skip_existing"]
        set_hidden = options["set_hidden"]
        sub_source = options["sub_source"]

        series = []
        metadata = {}
        if series_id:
            series = [series_id]
        elif tags:
            series_response = fetch_fred_series(tags)
            for s in series_response:
                metadata[s["id"]] = {
                    "title": s["title"],
                    "description": s.get("notes"),
                    "popularity": s.get("popularity"),
                    "group_popularity": s.get("group_popularity"),
                }
                series.append(s["id"])

        for series_id in series:
            if series_id in DENYLIST:
                self.stdout.write(f"Skipping series {series_id} due to denylist")
                continue
            if (
                skip_existing
                and Dataset.objects.filter(metadata__internal_name=series_id).exists()
            ):
                self.stdout.write(f"Skipping series {series_id}")
                continue

            self.stdout.write(f"Fetching data for series {series_id}")

            # Fetch the data from FRED
            records = fetch_fred_data(series_id, self.stdout)

            if len(metadata) == 0:
                series_metadata = fetch_fred_metadata(series_id)
                if series_metadata is None:
                    self.stdout.write(f"No data found for series {series_id}")
                    continue
                dataset_metadata, _ = DatasetMetadata.objects.get_or_create(
                    internal_name=series_id,
                    defaults={
                        "external_name": series_metadata["title"],
                        "source": "FRED",
                        "description": series_metadata["notes"],
                    },
                )
                total_new = add_dataset_bulk(records, dataset_metadata)
                self.stdout.write(f"Added {total_new} new records to the database")
            else:
                defaults = {
                    "external_name": metadata[series_id]["title"],
                    "source": "FRED",
                    "description": metadata[series_id]["description"],
                    "popularity": metadata[series_id]["popularity"],
                    "group_popularity": metadata[series_id]["group_popularity"],
                    "hidden": set_hidden,
                }
                if sub_source:
                    defaults["sub_source"] = sub_source
                dataset_metadata, _ = DatasetMetadata.objects.get_or_create(
                    internal_name=series_id,
                    defaults=defaults,
                )
                total_new = add_dataset_bulk(records, dataset_metadata)
                self.stdout.write(f"Added {total_new} new records to the database")
