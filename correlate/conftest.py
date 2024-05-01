import pytest
from datasets.orm.dataset_orm import get_dataset_filters


@pytest.fixture(autouse=True)
def clear_caches():
    get_dataset_filters.cache_clear()
