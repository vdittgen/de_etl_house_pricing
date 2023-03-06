from loguru import logger

from etl.house_pricing.dependencies import get_dependencies
from etl.house_pricing.services.process_etl import HousePricingETL

log = logger


def handle(event, context):
    log.info(event)
    extract_adapter = get_dependencies().get_house_prices_store()
    transformer = get_dependencies().get_transformer()
    load_store_adapter = get_dependencies().get_s3_store()
    etl = HousePricingETL(log, extract_adapter, transformer, load_store_adapter)
    etl.execute()
