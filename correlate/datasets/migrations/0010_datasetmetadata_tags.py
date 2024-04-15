# Generated by Django 4.1.13 on 2024-04-15 00:54

import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("datasets", "0009_datasetmetadata_units_datasetmetadata_units_short"),
    ]

    operations = [
        migrations.AddField(
            model_name="datasetmetadata",
            name="tags",
            field=django.contrib.postgres.fields.ArrayField(
                base_field=models.CharField(max_length=50),
                blank=True,
                null=True,
                size=None,
            ),
        ),
    ]
