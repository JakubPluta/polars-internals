import polars as pl
from _const import COOKBOOK_DATA_DIR, NYC_YELLOW_TAXI_DIR
from polars.datatypes import Boolean, Float64, Int64, String
from polars import Schema

# Creating Lazy DataFrame

df = pl.LazyFrame(
    {"a": [1, 2, 3, 4, 5], "b": [5, 4, 3, 2, 1], "c": ["a", "b", "c", "d", "e"]}
)
print(type(df))
print(df)  # print lazy frame plan
print(df.collect())  # collect the data -> materialize the lazy frame to DataFrame
print(df.explain(optimized=True))  # print optimized plan


# reading data lazily

df: pl.LazyFrame = pl.scan_csv(
    COOKBOOK_DATA_DIR / "titanic_dataset.csv", has_header=True
)
print(
    df.limit().collect()
)  # collect the data, only 5 rows -> materialize the lazy after limiting the frame to 5 rows
print(df.collect_schema())  # collect the schema from lazy frame
print("=" * 100)
print(df.select(pl.col("Name", "Age")).explain(optimized=False))  # Before optimization
print("=" * 100)
print(df.select(pl.col("Name", "Age")).explain(optimized=True))  # After optimization


# Selecting columns

print(df.select(pl.col(["Name", "Age"])).limit(5).collect())
print(df.select(pl.col("Name", "Age")).limit(5).collect())
print(df.select(pl.col("Name"), pl.col("Age"), pl.col("Survived")).limit(5).collect())
cols = ["Name", "Age", "Survived"]
print(df.select(pl.col(cols)).limit(5).collect())

# Filtering rows

print(df.filter(pl.col("Survived") == 1).limit(5).collect())
print(df.filter((pl.col("Age") >= 30)).tail(5).collect())
print(df.filter(pl.col("Survived") == 1).collect().shape)
print(df.filter((pl.col("Age") >= 30) & (pl.col("Sex") == "male")).head(5).collect())
print(
    df.collect()[["Age", "Sex"]].head()
)  # Selecting columns using slicing, like pandas. Only works on DataFrame, not on LazyFrame

# regex filter
print(
    df.select(pl.col("^[a-zA-Z]{0,4}$")).head().collect()
)  # Select columns that match the regex
print(
    df.select(pl.col(pl.String)).head().collect()
)  # Select columns that are of type String
print(
    df.select(pl.col(pl.Int16, pl.Int32, pl.Int64, pl.Int8)).head().collect()
)  # Select columns that are of type Numeric
# or better way
print(
    df.select(pl.selectors.numeric()).head().collect()
)  # Select columns that are of type Numeric

#  Select all columns that match the given regex pattern.
print(df.select(pl.selectors.matches("se|ed")).head().collect())


# creating a new column. Maximum Fare as a new column, calculated for all rows
# value will be duplicated for all rows
print(df.with_columns(pl.col("Fare").max().alias("Max_Fare")).limit(2).collect())
# or
print(df.with_columns(max_fare=pl.col("Fare").max()).limit(1).collect())

# calculate diff between max and min fare
print(
    df.with_columns(
        (pl.col("Fare").max() - pl.col("Fare").mean()).alias("Max_Mean_Fare_Diff")
    )
    .limit(5)
    .collect()
)

# adding constant column
print(df.with_columns(pl.lit("constant").alias("constant")).limit(5).collect())

# adding row index

print(df.with_row_index(name="index", offset=1).limit(5).collect())

# string operations

print(
    df.with_columns(pl.col("Name").str.to_lowercase().alias("lower_name"))
    .limit(5)
    .collect()
)

# best practice of using with_columns


best_practice = df.with_columns(
    pl.col("Fare").max().alias("Max Fare"),
    pl.lit("Titanic"),
    pl.col("Sex").str.to_titlecase(),
)
print(best_practice.explain(optimized=False))
print(best_practice.limit(5).collect())


# bad practice of using with_columns, it's because polars need to do more work to optimize

worse_approach = (
    df.with_columns(pl.col("Fare").max().alias("Max Fare"))
    .with_columns(pl.lit("Titanic"))
    .with_columns(pl.col("Sex").str.to_titlecase())
)
print(worse_approach.explain(optimized=False))
print(worse_approach.limit(5).collect())

# bad example of not chaining operations
columns = ["Name", "Sex", "Age", "Fare", "Cabin", "Pclass", "Survived"]
df1 = df.select(pl.col(columns))
df1 = df1.filter(pl.col("Age") >= 32)
df1 = df1.sort(by=["Age", "Name"], descending=True)
print(df1.limit(5).collect())


# good example of chaining operations
df2 = (
    df.select(pl.col(columns))
    .filter(pl.col("Age") >= 32)
    .sort(by=["Age", "Name"], descending=True)
)
print(df2.limit(5).collect())


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

# Reading data from a CSV file with streaming=True
# Process the query in batches to handle larger-than-memory data.
# If set to `False` (default), the entire query is processed in a single
# batch.
# pl.Config.set_streaming_chunk_size(1000)  # set chunk size for streaming
df = pl.scan_csv(
    NYC_YELLOW_TAXI_DIR / "yellow_tripdata_2016-03.csv", has_header=True, schema=schema
).collect(streaming=True)
print(df.head(5))


# Reading multi file data from a CSV file with streaming=True,
# You can provide a list of file paths to scan_csv or you can use pattern matching
# like "*.csv".
files = [
    NYC_YELLOW_TAXI_DIR / "yellow_tripdata_2015-01.csv",
    NYC_YELLOW_TAXI_DIR / "yellow_tripdata_2016-01.csv",
    NYC_YELLOW_TAXI_DIR / "yellow_tripdata_2016-02.csv",
    NYC_YELLOW_TAXI_DIR / "yellow_tripdata_2016-03.csv",
]  # OR  files = NYC_YELLOW_TAXI_DIR / "*.csv"

total_by_payment = (
    pl.scan_csv(files, has_header=True, schema=schema)
    .group_by(pl.col("payment_type"))
    .agg(
        pl.col("total_amount").median().alias("median_amount_by_payment"),
        pl.col("total_amount").mean().alias("avg_amount_by_payment"),
        pl.col("trip_distance").mean().alias("avg_distance_by_payment"),
    )
    .sort(by="avg_distance_by_payment", descending=True)
    .collect(streaming=True) # make it streaming, so it will fit into ram.
)
print(total_by_payment)
