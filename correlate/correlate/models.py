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
