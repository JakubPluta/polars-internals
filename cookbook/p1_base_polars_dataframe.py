import numpy as np
import polars as pl

# Creating Polars DataFrame

df = pl.DataFrame(
    {
        "a": [1, 2, 3, 4, 5],
        "b": [5, 4, 3, 2, 1],
        "c": ["a", "b", "c", "d", "e"],
    }
)
print(df.head())  # print first 5 rows
print(df.schema)  # print schema
print(df.dtypes)  # print data types
print(df.columns)  # print column names
print(df.shape, df.height, df.width)  # print shape


# Create Polars DataFrame from Numpy
arr = np.random.rand(10, 4)
df = pl.DataFrame(
    arr,
    schema={
        "a": pl.Float64,
        "b": pl.Float64,
        "c": pl.Float64,
        "d": pl.Float64,
    },
    orient="row",
)
print(df.head())

arr2 = np.array([(1, 2.0), (3, 4.0), (5, 6.0)])
df = pl.DataFrame(
    arr2,
    schema={"a": pl.Int8, "b": pl.Int16, "c": pl.Int32},
    orient="col",
)
print(df.head())

# Create Series
s = pl.Series("a", [1, 2, 3, 4, 5], dtype=pl.Int8)
print(s.head())
s = pl.Series("a", [1, 2, 3, 4, 5], dtype=pl.UInt8)
print(s.head())


data = {"col1": [1, 2, 3, 4, 5], "col2": [7, 8, 9, 10, 11]}
df = pl.DataFrame(data)
print(df)
print(df.to_series(index=0))  # index 0 is col1
print(df.to_series(index=1))  # index 1 is col2
for idx in range(df.width):
    print(df.to_series(index=idx))

print(df.get_column("col1"))  # get column by name, return Series
print(df.get_column("col2"))
for col in df.columns:
    print(df.get_column(col))
