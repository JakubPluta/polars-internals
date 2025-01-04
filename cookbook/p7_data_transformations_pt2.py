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
