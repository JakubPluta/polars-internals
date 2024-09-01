import polars as pl
import os
import pathlib
from polars.datatypes import Int64, Float64, String
from polars import Schema
import logging
from utils import timeit

ROOT_DIR_PATH = pathlib.Path(__file__).resolve().parent.parent
DATA_DIR_PATH = ROOT_DIR_PATH / "data"
NYC_YELLOW_TAXI_DIR = DATA_DIR_PATH / "nycyellotaxi"


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


schema = Schema(
    {
        "VendorID": Int64,
        "tpep_pickup_datetime": String,
        "tpep_dropoff_datetime": String,
        "passenger_count": Int64,
        "trip_distance": Float64,
        "pickup_longitude": Float64,
        "pickup_latitude": Float64,
        "RatecodeID": Int64,
        "store_and_fwd_flag": String,
        "dropoff_longitude": Float64,
        "dropoff_latitude": Float64,
        "payment_type": Int64,
        "fare_amount": Float64,
        "extra": Float64,
        "mta_tax": Float64,
        "tip_amount": Float64,
        "tolls_amount": Float64,
        "improvement_surcharge": Float64,
        "total_amount": Float64,
    }
)



@timeit
def main():
    log.info("Starting data processing...")
    # Lazy read, process, and save data
    data = (
        pl.scan_csv(NYC_YELLOW_TAXI_DIR / "*.csv", schema=schema, has_header=True)
        .with_columns(
            [
                pl.col("tpep_pickup_datetime").str.strptime(
                    pl.Datetime, "%Y-%m-%d %H:%M:%S"
                ),
                pl.col("tpep_dropoff_datetime").str.strptime(
                    pl.Datetime, "%Y-%m-%d %H:%M:%S"
                ),
            ]
        )
        .filter((pl.col("passenger_count") > 1) & (pl.col("tip_amount") > 0))
        .with_columns(
            (pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime")).alias(
                "trip_duration"
            ),
            pl.col("tpep_pickup_datetime").dt.year().alias("year"),
        )
        .select(
            [
                "VendorID",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "passenger_count",
                "trip_distance",
                "pickup_longitude",
                "pickup_latitude",
                "dropoff_longitude",
                "dropoff_latitude",
                "payment_type",
                "fare_amount",
                "extra",
                "mta_tax",
                "tip_amount",
                "tolls_amount",
                "improvement_surcharge",
                "total_amount",
            ]
        )
    )


    # Sink to Parquet with optimized settings
    data.sink_parquet(
        NYC_YELLOW_TAXI_DIR / "nycyellotaxi.parquet",
        compression="snappy",
        predicate_pushdown=True,
        projection_pushdown=True,
    )

    log.info("Data processing complete.")


if __name__ == "__main__":
    main()