from pydantic import BaseModel


class CorrelateIndexRequestBody(BaseModel):
    index_name: str
    dates: list[str]
    input_data: list[float]
    index_percentages: list[float]
    index_datasets: list[str]
