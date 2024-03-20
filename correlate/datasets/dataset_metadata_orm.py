from datasets.models import CorrelateDataPoint, DatasetMetadata
from ddtrace import tracer


@tracer.wrap("augoment_with_metadata")
def augment_with_metadata(
    datasets: list[CorrelateDataPoint],
) -> list[CorrelateDataPoint]:
    dataset_metadatas = DatasetMetadata.objects.filter(
        internal_name__in=[ds.title for ds in datasets]
    ).all()
    dataset_mapping = {ds.internal_name: ds for ds in dataset_metadatas}

    for dataset in datasets:
        metadata = dataset_mapping.get(dataset.title, None)
        if metadata is None:
            continue
        if metadata.external_name is not None:
            dataset.title = metadata.external_name
        dataset.source = metadata.source
        dataset.description = metadata.description
    return datasets


def get_metadata_from_external_name(external_name: str) -> DatasetMetadata | None:
    return DatasetMetadata.objects.filter(external_name=external_name).first()


def get_metadata_from_internal_name(internal_name: str) -> DatasetMetadata | None:
    return DatasetMetadata.objects.filter(internal_name=internal_name).first()


def get_metadata_from_name(name: str) -> DatasetMetadata | None:
    metadata = get_metadata_from_external_name(name)
    if metadata is not None:
        return metadata
    return get_metadata_from_internal_name(name)


def get_internal_name_from_external_name(external_name: str) -> str:
    metadata = get_metadata_from_external_name(external_name)
    if metadata is None:
        return external_name
    return metadata.internal_name


def create_dataset_metadata(
    internal_name: str,
    external_name: str | None = None,
    source: str | None = None,
    description: str | None = None,
) -> DatasetMetadata:
    return DatasetMetadata.objects.create(
        internal_name=internal_name,
        external_name=external_name,
        source=source,
        description=description,
    )
