from django.conf import settings
from django.core.management.base import BaseCommand
from datasets.lib.email import create_new_data_report_email
from datasets.models import Dataset, DatasetMetadata
from datasets.orm.dataset_orm import add_dataset_bulk
from datasets.management.commands.denylisted.manual import MANUAL_DENYLIST
from datasets.management.commands.denylisted.pairwise_clusters import PAIRWISE_CLUSTERS
from datasets.management.commands.fetch_fred_data import fetch_fred_data
from datetime import datetime
from django.core.mail import send_mail
from django.utils.html import strip_tags

DENYLIST = MANUAL_DENYLIST + PAIRWISE_CLUSTERS


class Command(BaseCommand):
    help = "Fetches the latest data from FRED and updates the database."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry_run",
            action="store_true",
            help="Whether to run the command in dry run mode",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        datasets = DatasetMetadata.objects.filter(
            source__in=["FRED", "Census", "BLS", "BEA", "Board of Governors"],
            hidden=False,
        )

        added_records = []
        updated_records = []
        start_time = datetime.now()

        # For each dataset fetch data and look for updates
        for dataset in datasets:
            series_id = dataset.internal_name
            records = fetch_fred_data(series_id, self.stdout)
            stored_records = Dataset.objects.filter(metadata=dataset).values_list(
                "date", "value"
            )
            stored_record_map = {record[0]: record[1] for record in stored_records}

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
                if not dry_run:
                    add_dataset_bulk(new_records, dataset)
                added_records.append((series_id, len(new_records)))

            if records_to_update:
                for record in records_to_update:
                    if not dry_run:
                        Dataset.objects.filter(metadata=dataset, date=record[0]).update(
                            value=record[1]
                        )

                    updated_records.append(
                        (
                            series_id,
                            record[0],
                            record[1],
                            stored_record_map.get(record[0], None),
                        )
                    )

            dataset.updated_at = datetime.now()
            dataset.save()

            self.stdout.write(
                self.style.SUCCESS(
                    f"Added {len(new_records)} & Updated {len(records_to_update)} records for {series_id}"
                )
            )

        total_time = datetime.now() - start_time

        email = create_new_data_report_email(
            added_records, updated_records, "FRED", total_time
        )
        subject = "FRED Data Updates on: " + str(datetime.now())
        email_from = settings.EMAIL_HOST_USER
        plain_message = strip_tags(email)
        send_mail(
            subject,
            plain_message,
            email_from,
            ["contact@correlatefinance.com"],
            html_message=email,
        )
        self.stdout.write(self.style.SUCCESS("FRED data fetch and update complete"))
