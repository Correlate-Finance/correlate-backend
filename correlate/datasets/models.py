from django.db import models
from pydantic import BaseModel


class CorrelateDataPoint(BaseModel):
    title: str
    pearson_value: float
    p_value: float
    lag: int = 0

    # Data points
    dates: list[str]
    input_data: list[float]
    dataset_data: list[float]


class CorrelateData(BaseModel):
    data: list[CorrelateDataPoint]


# Create your models here.
class DatasetMetadata(models.Model):
    class Categories(models.TextChoices):
        DEFENSE = "DEFENSE", "Defense"
        CONSTRUCTION = "CONSTRUCTION", "Construction"

    internal_name = models.CharField(
        max_length=255, unique=True, blank=False, null=False
    )
    external_name = models.CharField(max_length=255, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # ForeignKey on Category
    category = models.CharField(
        choices=Categories.choices, max_length=255, blank=True, null=True
    )
