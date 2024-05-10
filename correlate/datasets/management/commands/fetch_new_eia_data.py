from django.core.management.base import BaseCommand, CommandParser
from adapters.eia import fetch_eia_data, fetch_records_from_eia_data
from datasets.lib.email import create_new_data_report_email
from datasets.models import DatasetMetadata, Dataset
from datetime import datetime
import pytz
from datasets.orm.dataset_orm import add_dataset_bulk
from django.conf import settings
from django.core.mail import send_mail
from django.utils.html import strip_tags


class Command(BaseCommand):
    help = "Fetches the latest data from EIA and updates the database."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--dry_run",
            action="store_true",
            help="Whether to run the command in dry run mode",
        )

        parser.add_argument(
            "--n",
            type=int,
            help="The max number of series to update",
            default=10000,
            required=False,
        )

        return super().add_arguments(parser)

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        n = options["n"]
        # First we need to find all EIA datasets in our DB
        datasets_to_update = DatasetMetadata.objects.filter(source="EIA")
        # Then we need to fetch all EIA datasets from the EIA API

        added_records = []
        updated_records = []
        start_time = datetime.now()

        for dataset in datasets_to_update[:n]:
            series_id = dataset.internal_name
            try:
                data = fetch_eia_data(series_id)
                records = fetch_records_from_eia_data(data["data"], series_id)
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"Error fetching data for {series_id}: {e}")
                )
                import pdb

                pdb.set_trace()

            records = [
                record
                for record in records
                if record[0] > datetime(2000, 1, 1, tzinfo=pytz.utc)
            ]

            stored_records = Dataset.objects.filter(metadata=dataset).values_list(
                "date", "value"
            )
            stored_records_map = {record[0]: record[1] for record in stored_records}
            records_map = {record[0]: record[1] for record in records}

            new_records = []
            records_to_update = []

            for date, value in records_map.items():
                if date in stored_records_map:
                    if stored_records_map[date] != value:
                        records_to_update.append((date, value))
                else:
                    new_records.append((date, value))

            if len(new_records) > 0:
                added_records.append((series_id, len(new_records)))
                if not dry_run:
                    add_dataset_bulk(new_records, dataset)

            if records_to_update:
                for record in records_to_update:
                    if not dry_run:
                        Dataset.objects.filter(metadata=dataset, date=record[0]).update(
                            value=record[1]
                        )
                    updated_records.append(
                        (series_id, record[0], record[1], stored_records_map[record[0]])
                    )

            if not dry_run:
                dataset.updated_at = datetime.now()
                dataset.save()

            self.stdout.write(
                self.style.SUCCESS(
                    f"Added {len(new_records)} & Updated {len(records_to_update)} records for {series_id} in {datetime.now() - start_time}"
                )
            )

        total_time = datetime.now() - start_time

        email = create_new_data_report_email(
            added_records, updated_records, "EIA", total_time
        )

        subject = "EIA Data Updates on: " + str(datetime.now())
        email_from = settings.EMAIL_HOST_USER
        plain_message = strip_tags(email)
        send_mail(
            subject,
            plain_message,
            email_from,
            ["contact@correlatefinance.com"],
            html_message=email,
        )
        self.stdout.write(self.style.SUCCESS("EIA data fetch and update complete"))
