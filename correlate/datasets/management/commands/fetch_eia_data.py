from django.core.management.base import BaseCommand, CommandParser
from adapters.eia import (
    fetch_all_eia_series,
    fetch_and_store_eia_series,
    BLOCKED_SERIES,
)


class Command(BaseCommand):
    help = "Fetches the latest data from EIA and updates the database."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--n",
            type=int,
            help="The max number of series to fetch",
            default=1000,
            required=False,
        )
        return super().add_arguments(parser)

    def handle(self, *args, **options):
        all_series = fetch_all_eia_series()
        for series_id in all_series[: int(options["n"])]:
            if series_id in BLOCKED_SERIES:
                continue
            fetch_and_store_eia_series(series_id, self.stdout)
