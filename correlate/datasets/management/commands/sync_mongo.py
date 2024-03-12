from django.core.management.base import BaseCommand
from datasets.models import DatasetMetadata
from datasets.mongo_operations import (
    connect_to_mongo,
    MONGO_URI,
    DATABASE_NAME,
)
import pytz


class Command(BaseCommand):
    help = "Syncs the DatasetMetadata table with the mongo db"

    def handle(self, *args, **options):
        db = connect_to_mongo(MONGO_URI, DATABASE_NAME)
        dataTable_documents = db["dataTable"]
        dataTable_documents = dataTable_documents.find(
            {}, {"title": 1, "_id": 1, "created_at": 1}
        )

        dataTables = {}
        for document in dataTable_documents:
            if "_id" in document and "title" in document:
                dataTables[document["title"]] = document["created_at"].replace(
                    tzinfo=pytz.UTC
                )

        for title in dataTables.keys():
            DatasetMetadata.objects.get_or_create(internal_name=title)[0]
            # Update the updated_at field so that save doesn't overwrite the updated at field
            DatasetMetadata.objects.filter(internal_name=title).update(
                updated_at=dataTables[title]
            )

        self.stdout.write(
            self.style.SUCCESS("Successfully synced the DatasetMetadata table")
        )
