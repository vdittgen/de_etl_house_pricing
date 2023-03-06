start:
	python3.10 -m etl.house_pricing.services.process_etl
start.dev:
	ENV=dev \
	LOG_LEVEL=INFO \
	python3.10 -m etl.house_pricing.services.process_etl