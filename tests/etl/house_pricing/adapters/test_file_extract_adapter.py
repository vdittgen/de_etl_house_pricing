from unittest.mock import Mock
import pytest

from etl.house_pricing.adapters.file_extract_adapter import (
    HousePricesFileExtractRepository,
)


@pytest.fixture
def mock_house_prices_store_repository() -> HousePricesFileExtractRepository:
    hp_repository = HousePricesFileExtractRepository()
    hp_repository.extract = Mock()
    return hp_repository


def test_adapter_extract(mock_house_prices_store_repository):
    result_df = mock_house_prices_store_repository.extract()
    assert result_df is not None


def test_connection_error(mock_house_prices_store_repository):
    mock_house_prices_store_repository.extract.side_effect = (
        OSError
    )
    with pytest.raises(OSError):
        mock_house_prices_store_repository.extract()
