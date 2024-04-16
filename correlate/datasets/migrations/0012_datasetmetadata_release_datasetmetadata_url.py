# Generated by Django 4.1.13 on 2024-04-16 18:45

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("datasets", "0011_tag_remove_datasetmetadata_tags_datasetmetadata_tags"),
    ]

    operations = [
        migrations.AddField(
            model_name="datasetmetadata",
            name="release",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="datasetmetadata",
            name="url",
            field=models.URLField(blank=True, null=True),
        ),
    ]
