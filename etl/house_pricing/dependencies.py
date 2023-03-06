import os
import sys
from abc import ABC, abstractmethod

import boto3
from boto3.s3.transfer import S3Transfer, TransferConfig
from loguru import logger

import etl.house_pricing.settings as settings
from etl.house_pricing.adapters.file_extract_adapter import (
    FileExtractRepository,
    HousePricesFileExtractRepository,
)
from etl.house_pricing.adapters.s3_store_adapter import (
    S3StoreRepository,
    StoreRepository,
)
from etl.house_pricing.services.tranformer import HousePricingTransformer, Transformer

log = logger


class Dependencies(ABC):
    @abstractmethod
    def get_house_prices_store(self) -> FileExtractRepository:
        pass

    @abstractmethod
    def get_transformer(self) -> Transformer:
        pass

    @abstractmethod
    def get_s3_store(self) -> StoreRepository:
        pass


class ProdDependencies(Dependencies):
    def get_house_prices_store(self) -> FileExtractRepository:
        return HousePricesFileExtractRepository()

    def get_transformer(self) -> HousePricingTransformer:
        return HousePricingTransformer()

    def get_s3_store(self) -> S3StoreRepository:
        aws_access_key_id = settings.S3_KEY_ID
        aws_secret_access_key = settings.S3_KEY_PASS
        s3_bucket = str(settings.S3_BUCKET_PARQUET)
        s3_bucket_subdir = str(settings.S3_BUCKET_PARQUET_SUBDIR)

        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key,
                          region_name='us-east-2')
        config = TransferConfig(use_threads=True, max_concurrency=10)
        transfer = S3Transfer(client=s3, config=config)
        return S3StoreRepository(s3_bucket, s3_bucket_subdir, transfer)


class StagingDependencies(ProdDependencies):
    pass


class DevDependencies(StagingDependencies):
    pass


class LocalDependencies(DevDependencies):
    pass


__dependencies = None


def get_dependencies() -> Dependencies:
    global __dependencies
    if __dependencies is None:
        env = os.environ.get("ENV", "local")
        classname = f"{env.capitalize()}Dependencies"
        this_mod = sys.modules[__name__]
        class_ = getattr(this_mod, classname)
        __dependencies = class_()
    return __dependencies
