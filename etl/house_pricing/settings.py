from decouple import AutoConfig  # type: ignore


config = AutoConfig(search_path=".")

S3_KEY_ID = config(
    "S3_KEY_ID",
    default="SECRET",
    cast=str,
)
S3_KEY_PASS = config(
    "S3_KEY_PASS",
    default="SECRET",
    cast=str,
)
S3_BUCKET_PARQUET = config(
    "S3_BUCKET_PARQUET",
    default="vinipd",
    cast=str,
)
S3_BUCKET_PARQUET_SUBDIR = config(
    "HOUSE_PRICE_S3_BUCKET_PARQUET_SUBDIR", default="house/prices", cast=str
)
