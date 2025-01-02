import polars as pl
from _const import COOKBOOK_DATA_DIR

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
