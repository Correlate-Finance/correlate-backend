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
    # These are in camel case since they are sent to the frontend
    data: list[CorrelateDataPoint]
    aggregationPeriod: str
    correlationMetric: str
    fiscalYearEnd: str = "December"


# Create your models here.
class DatasetMetadata(models.Model):
    class Categories(models.TextChoices):
        DEFENSE = "DEFENSE", "Defense"
        CONSTRUCTION = "CONSTRUCTION", "Construction"

    internal_name = models.CharField(
        max_length=255, unique=True, blank=False, null=False
    )
    external_name = models.CharField(
        max_length=255, blank=True, null=True, db_index=True
    )
    description = models.TextField(blank=True, null=True)
    source = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # ForeignKey on Category
    category = models.CharField(
        choices=Categories.choices, max_length=255, blank=True, null=True
    )
    high_level = models.BooleanField(default=False)

    def __str__(self):
        return f"{self.external_name if self.external_name is not None else self.internal_name}"
