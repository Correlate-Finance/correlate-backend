# Generated by Django 4.1.13 on 2024-05-07 06:55

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("datasets", "0020_report_description"),
    ]

    operations = [
        migrations.AddField(
            model_name="report",
            name="name",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
