from unittest.mock import Mock

import boto3
import pandas as pd
import pytest

from etl.house_pricing.adapters.s3_store_adapter import S3StoreRepository


@pytest.fixture
def mock_s3_store_repository() -> S3StoreRepository:
    s3_bucket = "bucket"
    s3_bucket_subdir = "subdir"
    s3_transfer = Mock()
    s3_transfer.upload_file = Mock()
    return S3StoreRepository(s3_bucket, s3_bucket_subdir, s3_transfer)


def test_s3_store(mock_s3_store_repository):
    pandas_df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    parquet = pandas_df.to_parquet()
    mock_s3_store_repository.store(parquet, "test.parquet")

    mock_s3_store_repository.s3_transfer.upload_file.assert_called()


def test_connection_error(mock_s3_store_repository):
    mock_s3_store_repository.s3_transfer.upload_file.side_effect = (
        boto3.exceptions.Boto3Error  # type: ignore
    )
    pandas_df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    parquet = pandas_df.to_parquet()

    with pytest.raises(boto3.exceptions.Boto3Error):  # type: ignore
        mock_s3_store_repository.store(parquet, "test.parquet")
