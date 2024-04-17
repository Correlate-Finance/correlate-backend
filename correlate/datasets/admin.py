from django.contrib import admin

# Register your models here.

from .models import DatasetMetadata, Dataset, Index, IndexDataset
from datasets.dataset_orm import (
    parse_excel_file_for_datasets,
    parse_metadata_from_excel,
)
from django.urls import path
from django.shortcuts import render
from .forms import ExcelUploadForm
from django.http import HttpRequest


@admin.register(DatasetMetadata)
class DatasetMetadataAdmin(admin.ModelAdmin):
    list_display = ("internal_name", "external_name", "updated_at")
    search_fields = ("internal_name",)


@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ("metadata", "date", "value")
    search_fields = ("metadata__internal_name",)

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path("upload-datasets/", self.admin_site.admin_view(self.upload_excel)),
            path("upload-metadata/", self.admin_site.admin_view(self.upload_metadata)),
        ]
        return custom_urls + urls

    def upload_excel(self, request: HttpRequest):
        form = ExcelUploadForm()
        context = dict(
            # Include common variables for rendering the admin template.
            self.admin_site.each_context(request),
            title="Upload Excel",
            form=form,
        )

        if request.method == "POST":
            form = ExcelUploadForm(request.POST, request.FILES)
            if form.is_valid():
                excel_file = request.FILES["excel_file"]
                results = parse_excel_file_for_datasets(excel_file)
                updated = []
                created = []
                for title, new, rows in results:
                    if new:
                        created.append((title, rows))
                    else:
                        updated.append((title, rows))

                # Handle file upload and processing
                html = """<div>"""
                if created:
                    html = """<div><h2 style="color=green">Datasets Created</h2>"""
                    html += "<table>"
                    for title, rows in created:
                        html += "<tr>"
                        html += f"<td>{title}</td>"
                        html += f"<td>{rows} rows updated</td>"
                        html += "</tr>"

                    html += "</table>"

                if updated:
                    html += """<h2 style="color=yellow">Datasets Updated</h2>"""
                    html += "<table>"
                    for title, rows in updated:
                        html += "<tr>"
                        html += f"<td>{title}</td>"
                        html += f"<td>{rows} rows updated</td>"
                        html += "</tr>"

                    html += "</table>"
                html += "</div><div style='margin-bottom: 20px'></div>"

                context["message"] = html
                return render(
                    request,
                    "admin/excel_form.html",
                    context,
                )

        return render(request, "admin/excel_form.html", context)

    def upload_metadata(self, request: HttpRequest):
        form = ExcelUploadForm()
        context = dict(
            # Include common variables for rendering the admin template.
            self.admin_site.each_context(request),
            title="Upload Metadata Excel",
            form=form,
        )

        if request.method == "POST":
            form = ExcelUploadForm(request.POST, request.FILES)
            if form.is_valid():
                excel_file = request.FILES["excel_file"]
                results = parse_metadata_from_excel(excel_file)

                html = """<div>"""
                for status, message in results:
                    html += f"<p style='color: {'green' if status == 'success' else 'red'}'>{message}</p>"
                html += "</div>"

                context["message"] = html
                return render(
                    request,
                    "admin/excel_form.html",
                    context,
                )

        return render(request, "admin/excel_form.html", context)


admin.site.register(Index)
admin.site.register(IndexDataset)
