# Generated by Django 4.1.13 on 2024-03-11 17:53

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("datasets", "0004_alter_datasetmetadata_external_name"),
    ]

    operations = [
        migrations.CreateModel(
            name="Dataset",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                ("date", models.DateTimeField()),
                ("value", models.FloatField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "metadata",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="datasets.datasetmetadata",
                    ),
                ),
            ],
        ),
    ]
