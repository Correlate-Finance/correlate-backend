from datasets.models import CorrelateDataPoint, DatasetMetadata


def augment_with_external_title(
    datasets: list[CorrelateDataPoint],
) -> list[CorrelateDataPoint]:
    dataset_metadatas = DatasetMetadata.objects.filter(
        internal_name__in=[ds.title for ds in datasets]
    ).all()
    dataset_mapping = {ds.internal_name: ds for ds in dataset_metadatas}

    for dataset in datasets:
        metadata = dataset_mapping.get(dataset.title, None)
        if metadata is None or metadata.external_name is None:
            continue
        dataset.title = metadata.external_name
    return datasets


def get_metadata_from_external_name(external_name: str) -> DatasetMetadata | None:
    return DatasetMetadata.objects.filter(external_name=external_name).first()
