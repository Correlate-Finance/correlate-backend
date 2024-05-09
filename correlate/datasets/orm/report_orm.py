from datasets.models import Report, CorrelationParameters, CorrelateDataPoint
from users.models import User


def create_report(
    user: User | int,
    parameters: CorrelationParameters | int,
    llm_response: dict,
    name: str,
    report_data: list[CorrelateDataPoint],
    description: str | None = None,
) -> Report:
    correlation_parameters_id = (
        parameters.id if isinstance(parameters, CorrelationParameters) else parameters
    )
    user_id = user.id if isinstance(user, User) else user

    report_json_data = [model.model_dump() for model in report_data]

    report = Report.objects.create(
        name=name,
        user_id=user_id,
        parameters_id=correlation_parameters_id,
        llm_response=llm_response,
        report_data=report_json_data,
        description=description,
    )
    return report
