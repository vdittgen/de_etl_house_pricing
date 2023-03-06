import os
from abc import ABC, abstractmethod

import boto3
from boto3.s3.transfer import S3Transfer
from loguru import logger

log = logger


class StoreRepository(ABC):
    @abstractmethod
    def __init__(self, s3_bucket: str, s3_bucket_subdir: str, s3_transfer: S3Transfer):
        self.s3_bucket = s3_bucket
        self.s3_bucket_subdir = s3_bucket_subdir
        self.s3_transfer = s3_transfer

    @abstractmethod
    def store(self, file_bytes: bytes, file_name: str) -> None:
        pass


class S3StoreRepository(StoreRepository):
    def __init__(self, s3_bucket: str, s3_bucket_subdir: str, s3_transfer: S3Transfer):
        super().__init__(s3_bucket, s3_bucket_subdir, s3_transfer)

    def store(self, file_bytes: bytes, file_name: str) -> None:
        tmpfile = "/tmp/tmpfile"
        try:
            # Write the bytes to a local file
            with open(tmpfile, "wb") as f:
                f.write(file_bytes)

            log.info("Uploading file to S3.")
            self.s3_transfer.upload_file(
                tmpfile, self.s3_bucket, f"{self.s3_bucket_subdir}/{file_name}"
            )  # type: ignore
        except boto3.exceptions.Boto3Error as e:  # type: ignore
            log.error(f"Failed uploading parquet file to S3: {e}")
            raise
        finally:
            log.info("Cleaning temp file.")
            os.remove(tmpfile)
