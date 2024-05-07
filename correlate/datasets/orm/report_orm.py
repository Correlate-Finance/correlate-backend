from datasets.models import Report, CorrelationParameters, CorrelateDataPoint
from users.models import User


def create_report(
    user: User,
    parameters: CorrelationParameters | int,
    llm_response: dict,
    name: str,
    report_data: list[CorrelateDataPoint],
    description: str | None = None,
) -> Report:
    correlation_parameters_id = (
        parameters.id if isinstance(parameters, CorrelationParameters) else parameters
    )

    report_json_data = [model.model_dump() for model in report_data]

    report = Report.objects.create(
        name=name,
        user=user,
        parameters_id=correlation_parameters_id,
        llm_response=llm_response,
        report_data=report_json_data,
        description=description,
    )
    return report
