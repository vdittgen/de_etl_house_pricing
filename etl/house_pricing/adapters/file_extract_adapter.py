from abc import ABC, abstractmethod

import pandas as pd
from loguru import logger
from pandas import DataFrame

log = logger


class FileExtractRepository(ABC):
    @abstractmethod
    def extract(self) -> DataFrame:
        pass


class HousePricesFileExtractRepository(FileExtractRepository):
    def extract(self) -> DataFrame:
        log.info("Extracting data from file.")
        df = pd.DataFrame()
        try:
            df = pd.read_csv('etl/house_pricing/utils/kc_house_data.csv')
        except (OSError) as e:
            log.error(f"Error trying to read kc_house_data.csv file: {e}")
            raise

        return df
