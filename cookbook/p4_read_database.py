import contextlib
from typing import Generator, Iterator

import polars as pl
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from _const import COOKBOOK_DATA_DIR

uri = "postgresql://postgres:postgres@localhost:5432/postgres"


# adbc-driver-postgresql
query = "SELECT 1"
df = pl.read_database_uri(query, uri, engine="adbc")
print(df)


# sqlalchemy
engine = create_engine(uri)
df = pl.read_database(query, engine)
print(df)
with contextlib.suppress(sqlalchemy.exc.ProgrammingError):
    with Session(engine) as session:
        session.execute(text("commit"))  # to override autocommit issue
        session.execute(text("CREATE SCHEMA dev"))
        session.commit()

# write data to database

df = pl.read_csv(
    COOKBOOK_DATA_DIR / "customer_shopping_data.csv",
    has_header=True,
    try_parse_dates=True,
)


df.write_database(
    "dev.customer_shopping_data",
    uri,
    engine="adbc",
    if_table_exists="replace",
)


df: pl.DataFrame = pl.read_database_uri(
    "SELECT * FROM dev.customer_shopping_data",
    uri,
    engine="adbc",
)
print(df.head())

# read in memory efficient way
df: Iterator[pl.DataFrame] = pl.read_database(
    "SELECT * FROM dev.customer_shopping_data",
    connection=engine,
    iter_batches=True,
    batch_size=1000,
)
print(df)
df: Iterator[pl.DataFrame]
chunk_df: pl.DataFrame
for idx, chunk_df in enumerate(df):
    print(f"Chunk: {idx}: Shape {chunk_df.shape}")
