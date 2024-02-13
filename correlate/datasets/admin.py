from django.contrib import admin

# Register your models here.

from .models import DatasetMetadata


class DatasetMetadataAdmin(admin.ModelAdmin):
    list_display = ("internal_name", "external_name")
    search_fields = ("internal_name",)


admin.site.register(DatasetMetadata, DatasetMetadataAdmin)
