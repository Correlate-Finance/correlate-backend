from django.contrib import admin

# Register your models here.

from .models import DatasetMetadata

admin.site.register(DatasetMetadata)
