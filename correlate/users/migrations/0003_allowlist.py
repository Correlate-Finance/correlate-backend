# Generated by Django 4.1.13 on 2024-03-13 02:14

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0002_watchlist"),
    ]

    operations = [
        migrations.CreateModel(
            name="Allowlist",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("email", models.EmailField(max_length=254, unique=True)),
            ],
        ),
    ]
