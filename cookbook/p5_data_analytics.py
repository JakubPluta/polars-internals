import contextlib
from typing import Generator, Iterator

import polars as pl
from _const import COOKBOOK_DATA_DIR
import polars.selectors as cs

df = pl.read_csv(COOKBOOK_DATA_DIR / "covid_19_deaths.csv", has_header=True)
print(df.head())
print(df.tail(5))


#    The formatting shows one line per column so that wide dataframes display
#         cleanly. Each line shows the column name, the data type, and the first
#         few values.
print(df.glimpse(max_items_per_column=2))

print(df.estimated_size(unit="mb"))

# select only numeric columns, and describe them
print(df.select(cs.numeric()).describe())

# count nulls
print(df.null_count())
print(df.select(cs.numeric()).null_count())

# ctx manager for config changes
with pl.Config() as config:
    config.set_tbl_cols(12)
    print(df.head())


# casting datatypes

updated_df = df.with_columns(
    pl.col("Data As Of").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("Start Date").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("End Date").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("End Date").str.to_date("%m/%d/%Y").alias("End Date As Date"),
    pl.col("Year").cast(pl.Int16),
)

print(updated_df.head())

# same for lazy frame
df = pl.scan_csv(COOKBOOK_DATA_DIR / "covid_19_deaths.csv", has_header=True)
updated_df = df.with_columns(
    pl.col("Data As Of").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("Start Date").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("End Date").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("End Date").str.to_date("%m/%d/%Y").alias("End Date As Date"),
    pl.col("Year").cast(pl.Int16),
)
print(updated_df.collect().head())


# remove duplicates
df = pl.read_csv(COOKBOOK_DATA_DIR / "covid_19_deaths.csv", has_header=True)
print(df.head())
print(df.height)
print(df.is_duplicated().sum())
print(df.is_unique().sum())
print(df.n_unique())

# select all columns and calculate n_unique for each
print(df.select(pl.all().n_unique()))
print(
    df.n_unique(subset=["Start Date", "End Date"])
)  # number of unique start and end dates combinations
print(
    df.unique(
        subset=["Start Date", "End Date"],
        keep="first",
    )
)
