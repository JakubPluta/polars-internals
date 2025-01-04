
jn:
	@echo "Starting Jupyter Notebook"
	@jupyter notebook

download-nyc:
	@echo "Downloading NYC Yellow Taxi Trip Data"
	curl -L -o ./nyc-yellow-taxi-trip-data.zip\
	https://www.kaggle.com/api/v1/datasets/download/elemento/nyc-yellow-taxi-trip-data

unzip-nyc:
	@echo "Unzipping NYC Yellow Taxi Trip Data"
	unzip nyc-yellow-taxi-trip-data.zip -d data/nycyellowtaxi/

clean:
	@rm nyc-yellow-taxi-trip-data.zip


nyc: download-nyc unzip-nyc clean


format:
	@echo "Formatting Python code"
	@black . --extend-exclude "notebooks" | ruff format . --exclude="**/*.ipynb"

up:
	@echo "Starting Minio and Postgres"
	docker compose up -d

down:
	@echo "Stopping Minio and Postgres"
	docker compose down