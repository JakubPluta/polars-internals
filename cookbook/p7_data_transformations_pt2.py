from datetime import date

import polars as pl
from _const import COOKBOOK_DATA_DIR
import polars.selectors as cs

df = pl.read_csv(
    COOKBOOK_DATA_DIR / "contoso_sales.csv",
    try_parse_dates=True,
)
print(df.head(1))

df = df.with_columns(
    (pl.col("Quantity") * pl.col("Net Price")).round(2).alias("Sales Amount")
)
print(df.head(1))

# calculate sales by category with window function
# it uses sum over window function
print(
    df.select(
        pl.col("Category"),
        pl.col("Subcategory"),
        pl.col("Sales Amount")
        .sum()
        .over(
            pl.col("Category").alias("Sales Amount by Category"),
        ),
    )
)

print(
    df.select(
        pl.col("Category"),
        pl.col("Brand"),
        pl.col("Subcategory"),
        pl.col("Sales Amount")
        .mean()
        .over("Category", "Brand")
        .alias("Average Sales per Category and Brand"),
    )
    .filter(pl.col("Category").eq("Computers"))
    .unique()
    .sort(pl.col("Brand"))
)


print(
    df.select(
        pl.col("Category"),
        pl.col("Brand"),
        pl.col("Customer Age"),
        pl.col("Sales Amount")
        .mean()
        .over("Category", date.today().year - pl.col("Customer Age"))
        .alias("Average Sales per Category and Customer Age"),
    )
    .filter(pl.col("Category") == "Computers")
    .unique()
    .sort("Customer Age")
)

# calculates the maximum sales amount for each category, assigns a rank
# to each category based on the maximum sales amount in descending order
print(
    df.group_by(pl.col("Category"))
    .agg(pl.col("Sales Amount").max().alias("Max Sales Amt"))
    .with_columns(
        pl.col("Max Sales Amt").rank(descending=True).cast(pl.Int64).alias("Rank")
    )
    .sort("Rank")
)


# calculates the maximum sales amount for each category and subcategory, assigns a rank to
# each subcategory within a category based on the maximum sales amount in descending order,
# and then filters the results to only include the 'Audio' and 'Computers' categories,
# sorting the output by category and rank.

print(
    df.group_by("Category", "Subcategory")
    .agg(pl.col("Sales Amount").max().round().cast(pl.Int64).alias("Max Sales Amt"))
    .with_columns(
        pl.col("Max Sales Amt")
        .rank(descending=True)
        .over("Category")
        .cast(pl.Int64)
        .alias("Rank")
    )
    .filter(pl.col("Category").is_in(["Audio", "Computers"]))
    .sort(["Category", "Rank"])
)


max_sales_rank = (
    df.group_by(
        pl.col("Category"),
        pl.col("Subcategory"),
    )
    .agg(pl.col("Sales Amount").max().round().cast(pl.Int64).alias("Max Sales Amt"))
    .with_columns(
        pl.col("Max Sales Amt")
        .rank(descending=True)
        .over("Category")
        .cast(pl.Int64)
        .alias("Rank")
    )
    .filter(pl.col("Category").is_in(["Audio", "Computers"]))
    .sort([pl.col("Category"), pl.col("Rank")])
)
print(max_sales_rank)


#   mapping_strategy: {'group_to_rows', 'join', 'explode'}
# - join -Join the groups as 'List<group_dtype>' to the row positions. warning: this can be memory intensive.
# The output column "Lowest 3 Subcat per Cat" contains a list of the top 3 subcategories by sales amount
# for each category in the max_sales_rank
print(
    max_sales_rank.with_columns(
        pl.col("Subcategory")
        .sort_by("Max Sales Amt")
        .head(3)
        .over("Category", mapping_strategy="join")
        .alias("Lowest 3 Subcat per Cat")
    )
)

# explodes the grouped data into new rows, similar to the results of
# `group_by` + `agg` + `explode`. Sorting of the given groups is required
# if the groups are not part of the window operation for the operation,
# otherwise the result would not make sense. This operation changes the
# number of rows.

print(
    (
        max_sales_rank.sort("Subcategory").with_columns(
            pl.col("Subcategory")
            .sort_by("Max Sales Amt")
            .over("Category", mapping_strategy="explode")
            .alias("Subcategory Sorted by Max Sales Amt Ascending")
        )
    )
)

# udfs

df = pl.read_csv(COOKBOOK_DATA_DIR / "contoso_sales.csv", try_parse_dates=True)
print(df.head(1))


def get_firstname(fullname: str, separator: str = " ") -> str:
    return fullname.split(separator)[0]


def get_age_band(age: int) -> str:
    match age:
        case age if age < 18:
            return "Children"
        case age if age < 35:
            return "Young Adults"
        case age if age < 60:
            return "Adults"
        case _:
            return "Seniors"


print(
    df.with_columns(
        pl.col("Customer Name")
        .map_elements(lambda el: get_firstname(el), return_dtype=pl.String)
        .alias("Customer First Name"),
        pl.col("Customer Age")
        .map_elements(lambda el: get_age_band(el), return_dtype=pl.String)
        .alias("Age Band"),
    ).head()
)

# or you can use polars directly
print(
    df.select(
        "Customer Age",
        pl.when(pl.col("Customer Age") < 18)
        .then(pl.lit("~17"))
        .when(pl.col("Customer Age") <= 30)
        .then(pl.lit("18~30"))
        .when(pl.col("Customer Age") <= 50)
        .then(pl.lit("31~50"))
        .when(pl.col("Customer Age") <= 70)
        .then(pl.lit("51~70"))
        .when(pl.col("Customer Age") > 70)
        .then(pl.lit("71~"))
        .alias("Age Range"),
    ).head()
)


# udfs like in spark have bad performance
import time

s = time.time()
print(
    df.select(
        "Customer Name",
        pl.col("Customer Name")
        .map_elements(lambda el: el.split(" ")[0], return_dtype=pl.String)
        .alias("Customer First Name"),
    ).head()
)
e = time.time()
print("UDF Performance", e - s)

# Polars has 3-4x faster performance
s = time.time()
print(
    df.select(
        "Customer Name",
        pl.col("Customer Name")
        .str.split(" ")
        .list.first()
        .alias("Customer First Name"),
    ).head()
)
e = time.time()
print("Polars Performance", e - s)
