from abc import ABC, abstractmethod

from loguru import logger
from pandas import DataFrame

from etl.house_pricing.utils.data_schema import house_prices_dtypes

log = logger


class Transformer(ABC):
    @abstractmethod
    def transform(
        self, pandas_df: DataFrame, datatypes_mapping=None, converters=None
    ) -> DataFrame:
        pass


class HousePricingTransformer(Transformer):
    def transform(
        self, pandas_df: DataFrame, datatypes_mapping=None, converters=None
    ) -> DataFrame:
        log.info("Transforming dataframe.")
        try:
            sorted_df = pandas_df.sort_values(["id", "zipcode"])
            transformed_df = sorted_df.astype(house_prices_dtypes)
        except ValueError as e:
            log.error(f"Failed transforming the pandas dataframe: {e}")
            raise
        except Exception as e:
            log.error(f"Unexpectec exception: {e}")
            raise

        return transformed_df
