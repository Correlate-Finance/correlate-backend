from django.db import models
from pydantic import BaseModel
from enum import Enum
from django.contrib.postgres.fields import ArrayField


class AggregationPeriod(str, Enum):
    QUARTERLY = "Quarterly"
    ANNUALLY = "Annually"


class CorrelationMetric(str, Enum):
    RAW_VALUE = "RAW_VALUE"
    YOY_GROWTH = "YOY_GROWTH"


class CompanyMetric(str, Enum):
    REVENUE = "revenue"
    COST_OF_REVENUE = "costOfRevenue"
    GROSS_PROFIT = "grossProfit"
    OPERATING_INCOME = "operatingIncome"
    NET_INCOME = "netIncome"
    EBITDA = "ebitda"
    EPS = "eps"


class CorrelateDataPoint(BaseModel):
    title: str
    internal_name: str | None = None

    pearson_value: float
    p_value: float = 0
    lag: int = 0

    # Data points
    dates: list[str]
    input_data: list[float]
    dataset_data: list[float]

    # Optional Metadata
    source: str | None = None
    description: str | None = None


class CorrelateData(BaseModel):
    # These are in camel case since they are sent to the frontend
    data: list[CorrelateDataPoint]
    aggregationPeriod: AggregationPeriod
    correlationMetric: str
    fiscalYearEnd: str = "December"


class Tag(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=50, unique=True)
    description = models.TextField(null=True, blank=True)
    popularity = models.IntegerField(blank=True, null=True)

    def __str__(self):
        return f"{self.name} -- {self.description}"


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
    url = models.URLField(blank=True, null=True)
    release = models.CharField(max_length=255, blank=True, null=True)

    # For example within FRED we can have BLS as well
    sub_source = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # ForeignKey on Category
    category = models.CharField(
        choices=Categories.choices, max_length=255, blank=True, null=True
    )
    high_level = models.BooleanField(default=False)

    popularity = models.IntegerField(blank=True, null=True)
    group_popularity = models.IntegerField(blank=True, null=True)
    hidden = models.BooleanField(default=False)

    units = models.CharField(max_length=255, blank=True, null=True)
    units_short = models.CharField(max_length=255, blank=True, null=True)

    tags = models.ManyToManyField(Tag)

    def __str__(self):
        return f"{self.source} -- {self.internal_name}: {self.name}"

    @property
    def name(self):
        return (
            self.external_name if self.external_name is not None else self.internal_name
        )


class Dataset(models.Model):
    class Meta:
        unique_together = ["metadata", "date"]

    id = models.AutoField(primary_key=True)
    metadata = models.ForeignKey(DatasetMetadata, on_delete=models.CASCADE)
    # We don't currently need time but using time for future proofing
    date = models.DateTimeField()
    value = models.FloatField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.metadata.name}"


class Index(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255)
    user = models.ForeignKey("users.User", on_delete=models.CASCADE)
    aggregation_period = models.CharField(max_length=255)
    correlation_metric = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.user.email} - {self.dataset.name}"


class IndexDataset(models.Model):
    id = models.AutoField(primary_key=True)
    dataset = models.ForeignKey(DatasetMetadata, on_delete=models.CASCADE)
    weight = models.FloatField()
    index = models.ForeignKey(Index, on_delete=models.CASCADE)

    def __str__(self):
        return f"{self.dataset.name}"
