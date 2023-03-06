import datetime

from loguru import logger
from pandas import DataFrame

from etl.house_pricing.adapters.file_extract_adapter import FileExtractRepository
from etl.house_pricing.adapters.s3_store_adapter import StoreRepository
from etl.house_pricing.dependencies import get_dependencies
from etl.house_pricing.services.tranformer import Transformer


class HousePricingETL:
    def __init__(
        self,
        log,
        extract_adapter: FileExtractRepository,
        transformer: Transformer,
        load_store_adapter: StoreRepository,
    ) -> None:
        self.log = log
        self.extract_adapter = extract_adapter
        self.transformer = transformer
        self.load_store_adapter = load_store_adapter

    def execute(self) -> None:
        dataframe = self.extract()
        if dataframe.empty:
            self.log.info("Extract got an empty response.")
            return
        transformed_data = self.transform(dataframe)
        self.load(transformed_data)

    def extract(self) -> DataFrame:
        return self.extract_adapter.extract()

    def transform(self, pandas_df: DataFrame, datatypes_mapping=None, converters=None):
        pandas_df = pandas_df.fillna("")
        transformed_df = self.transformer.transform(
            pandas_df, datatypes_mapping, converters
        )
        return transformed_df.to_parquet()

    def load(self, final_data) -> None:
        file_name = f"{self.__compute_new_file_name()}"
        self.load_store_adapter.store(final_data, file_name)

    def __compute_new_file_name(self) -> str:
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"prices_{timestamp}.parquet"
        return file_name


if __name__ == "__main__":
    log = logger
    extract_adapter = get_dependencies().get_house_prices_store()
    transformer = get_dependencies().get_transformer()
    load_store_adapter = get_dependencies().get_s3_store()
    etl = HousePricingETL(log, extract_adapter, transformer, load_store_adapter)
    etl.execute()
