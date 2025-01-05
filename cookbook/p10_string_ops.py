import polars as pl
from _const import COOKBOOK_DATA_DIR
from datetime import date

df = pl.read_csv(COOKBOOK_DATA_DIR / "google_store_reviews.csv", has_header=True)
print(df.head())

print(
    df.filter(pl.col("content").str.starts_with("Very"))
    .select(pl.col("content"))
    .head()
)


print(
    df.filter(
        pl.col("content").str.contains("happy", literal=True)
    )  # Treat `pattern` as a literal string, not as a regular expression.
    .select(pl.col("content"))
    .head()
)
print(
    (
        df.filter(
            pl.col("content").str.contains(r"very happy|best app|I love")
        )  # Treat `pattern` as a regular expression
        .select("content")
        .head()
    )
)


print(df.filter(pl.col("content").str.contains_any(["happy", "love", "best"])).height)

print(
    df.filter(
        pl.col("content").str.count_matches(r"very happy|best app|I love") > 2
    ).select("content")
)
# count matches
print(
    df.with_columns(
        pl.col("content")
        .str.count_matches(r"very happy|best app|I love")
        .alias("count")
    ).sort("count", descending=True)
)

# string length
print(df.filter(pl.col("userName").str.len_chars() > 10).select("userName").head())


# date parsing

print(
    df.select(
        "at",
        pl.col("at").str.to_date(format="%Y-%m-%d %H:%M:%S").alias("at(date)"),
        pl.col("at").str.to_time(format="%Y-%m-%d %H:%M:%S").alias("at(time)"),
        pl.col("at").str.to_datetime(format="%Y-%m-%d %H:%M:%S").alias("at(datetime)"),
    ).head()
)
print(
    df.select(
        "at",
        pl.col("at").str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S").alias("at(date)"),
        pl.col("at").str.strptime(pl.Time, "%Y-%m-%d %H:%M:%S").alias("at(time)"),
        pl.col("at")
        .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
        .alias("at(datetime)"),
    ).head()
)


print(
    df.select(
        "userName",
        pl.col("userName").str.slice(3).alias("4thCharAndAfter"),
        pl.col("userName").str.slice(3, 5).alias("5CharsAfter4thChar"),
        pl.col("userName").str.slice(-2, 1).alias("TheLastToSecondChar"),
    ).head()
)

# first word
print(
    df.select(
        "content", pl.col("content").str.extract(r"([A-Za-z]+)").alias("extract")
    ).head(5)
)
print(
    df.select(
        "content",
        pl.col("content")
        .str.extract(r"([A-Za-z]{3}) ([0-9]+)", 0)
        .alias("extract whole matches specified"),
        pl.col("content")
        .str.extract(r"([A-Za-z]{3}) ([0-9]+)", 1)
        .alias("extract group 1 specified"),
        pl.col("content")
        .str.extract(r"([A-Za-z]{3}) ([0-9]+)", 2)
        .alias("extract group 2 specified"),
    ).head(5)
)


print(
    df.select(
        "content",
        pl.col("content").str.extract_all(r"(?i)([A-Z]+)").alias("extract_all"),
    ).head()
)

# split by space into a list
print(df.select("content", pl.col("content").str.split(by=" ").alias("split")).head())

print(
    df.select(
        "content",
        pl.col("content").str.splitn(by=" ", n=10).alias("splitn"),  # returns struct
        pl.col("content").str.split_exact(by=" ", n=10).alias("split_exact"),
    ).head()
)


df = pl.DataFrame({"colA": ["a", "b", "c", "d"], "colB": ["aa", "bb", "cc", "dd"]})
print(df.select(pl.all(), (pl.col("colA") + pl.col("colB")).alias("concat")))
print(
    df.select(
        pl.all(),
        pl.concat_str(
            pl.lit(100) + 3, pl.lit(" "), pl.col("colA"), pl.col("colB"), separator="::"
        ).alias("newCol"),
    )
)
