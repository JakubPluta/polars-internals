import logging
import pathlib

import polars as pl
from memory_profiler import profile
from polars import Schema
from polars.datatypes import Float64, Int64, String

from utils import timeit

ROOT_DIR_PATH = pathlib.Path(__file__).resolve().parent.parent
DATA_DIR_PATH = ROOT_DIR_PATH / "data"
NYC_YELLOW_TAXI_DIR = DATA_DIR_PATH / "nycyellowtaxi"


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
@profile
def main():
    log.info("Starting data processing...")

    # Lazy read, process, and save data
    data = pl.scan_csv(NYC_YELLOW_TAXI_DIR / "*.csv", schema=schema, has_header=True)

    # Sink to Parquet with optimized settings
    data.sink_parquet(
        NYC_YELLOW_TAXI_DIR / "nycyellotaxi.parquet",
        compression="snappy",
        predicate_pushdown=True,
        projection_pushdown=True,
    )

    log.info("Data processing complete.")


if __name__ == "__main__":
    """Reading NYC Yellow Taxi data, processing it, and saving it as Parquet.
    
    Input Data Size: ~8GB - 4 CSV Files, 19 Columns, 47_248_845 Rows
    Output Data Size: ~1.76GB - 1 Parquet File, 19 Columns, 47_248_845 Rows 
    
    Processing time:
        Time: 12 seconds
        Maximum memory used: 1235 MiB
    """
    main()
