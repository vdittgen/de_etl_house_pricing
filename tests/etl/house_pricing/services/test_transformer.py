import pickle
from unittest.mock import Mock

import pandas as pd
import pytest

from etl.house_pricing.dependencies import get_dependencies


@pytest.fixture
def pandas_df():
    input_path = "tests/fixtures/kc_house_data.csv"
    return pd.read_csv(input_path)


@pytest.fixture
def parquet_file():
    input_path = "tests/fixtures/expected_parquet_20230118.pkl"
    with open(input_path, "rb") as input_file:
        parquet_file = pickle.load(input_file)
    return parquet_file


@pytest.fixture
def mock_transformer(parquet_file):
    transformer = get_dependencies().get_transformer()
    transformer.transform = Mock()
    transformer.transform.return_value = parquet_file

    return transformer


@pytest.fixture
def mock_transformer_error(parquet_file):
    transformer = get_dependencies().get_transformer()
    transformer.transform = Mock()
    transformer.transform.side_effect = ValueError

    return transformer


def test_transformer_transform(mock_transformer, pandas_df, parquet_file):
    transformed_df = mock_transformer.transform(pandas_df)
    assert transformed_df == parquet_file


def test_transformation_error(mock_transformer_error, pandas_df):
    with pytest.raises(ValueError):
        mock_transformer_error.transform(pandas_df)
