import os
import pprint

import polars as pl
from _const import COOKBOOK_DATA_DIR

# reading csv eagerly
df = pl.read_csv(COOKBOOK_DATA_DIR / "customer_shopping_data.csv", has_header=True)
print(df)

# overwrite header columns

column_names = [
    "invoice_number",
    "customer_id",
    "gender",
    "age",
    "category",
    "quantity",
    "price",
    "payment_method",
    "invoice_date",
    "shopping_mall",
]
df = pl.read_csv(
    COOKBOOK_DATA_DIR / "customer_shopping_data.csv",
    has_header=True,
    new_columns=column_names,
)
print(df.head(1))

# try to parse dates
df = pl.read_csv(
    COOKBOOK_DATA_DIR / "customer_shopping_data.csv",
    has_header=True,
    try_parse_dates=True,
)
print(df.head())

# use schema overrides, to define the type of a column
# read lazy and write lazy
df = pl.scan_csv(
    COOKBOOK_DATA_DIR / "customer_shopping_data.csv",
    has_header=True,
    try_parse_dates=True,
    schema_overrides={"age": pl.Int8, "quantity": pl.Int32},
)
df.sink_csv(COOKBOOK_DATA_DIR / "tmp/customer_shopping_data.csv")

# read parquet

df = pl.read_parquet(
    COOKBOOK_DATA_DIR / "venture_funding_deals.parquet",
    columns=["Company", "Amount", "Valuation", "Industry"],  # Columns to select
    row_index_name="row_cnt",
)
print(df)

# read parquet lazily
df = pl.scan_parquet(
    COOKBOOK_DATA_DIR / "venture_funding_deals.parquet",
    row_index_name="row_cnt",
).select(["Company", "Amount", "Valuation", "Industry"])

df.sink_parquet(
    COOKBOOK_DATA_DIR / "tmp/venture_funding_deals.parquet",
    compression="lz4",  # {'lz4', 'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'zstd'}
    compression_level=10,  # compression level 1-10
    maintain_order=False,
)

pprint.pprint(os.listdir(COOKBOOK_DATA_DIR / "venture_funding_deals_partitioned")[:5])
# reading partitioned parquet
df = pl.read_parquet(
    COOKBOOK_DATA_DIR / "venture_funding_deals_partitioned",
    use_pyarrow=True,  # Use PyArrow instead of the Rust-native Parquet reader. The PyArrow reader is more stable
    pyarrow_options={
        "partitioning": "hive"
    },  # we will use hive style partitioning like: Industry=Accounting/
)
print(df.head())

# scan parquet, materialize with streaming and store partitioned parquet
(
    pl.scan_parquet(
        COOKBOOK_DATA_DIR / "venture_funding_deals_partitioned", hive_partitioning=True
    )
    .collect(streaming=True)
    .write_parquet(
        COOKBOOK_DATA_DIR / "tmp/venture_funding_deals_partitioned",
        use_pyarrow=True,
        pyarrow_options={
            "partition_cols": ["Industry"],
            "existing_data_behavior": "overwrite_or_ignore",
        },
    )
)

# reading delta lake tables
path = str(
    COOKBOOK_DATA_DIR / "venture_funding_deals_delta"
)  # Note: Delta doesn't work with Path objects
df = pl.read_delta(path)
print(df.head())

# reading delta tables lazily
df = pl.scan_delta(path)
print(df.collect(streaming=True).head())

partitioned_path = str(COOKBOOK_DATA_DIR / "tmp/venture_funding_deals_delta")
df.collect(streaming=True).write_delta(

    partitioned_path, mode="overwrite", delta_write_options={"partition_by": "Industry"}
)


print(pl.read_delta(partitioned_path).head())

# read only one partition
df = pl.read_delta(
    partitioned_path,
    use_pyarrow=True,
    pyarrow_options={'partitions': [('Industry', '=', 'Accounting')]},

)
print(df.head())


# reading from AWS
