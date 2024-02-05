from pydantic import BaseModel

class CorrelateDataPoint(BaseModel):
    title: str
    pearson_value: float
    lag: int = 0


class CorrelateData(BaseModel):
    data: list[CorrelateDataPoint]
