import polars as pl
from _const import COOKBOOK_DATA_DIR
import polars.selectors as cs

df = pl.read_csv(
    COOKBOOK_DATA_DIR / "contoso_sales.csv",
)
print(df.head(1))


# select only numeric columns, and sum them
print(df.select(cs.numeric()).sum())

# select one column and sum it
print(df.select(pl.col("Quantity")).to_series().sum())
print(df.select(pl.col("Quantity")).sum())

# select 1 string columns and take first and last value
print(
    df.select(
        pl.col("Customer Name").first().alias("Cust Name First"),
        pl.col("Customer Name").last().alias("Cust Name Last"),
    )
)

# select only records with quantity >= 4, and sum them
print(df.select((pl.col("Quantity") >= 4).sum()))

df = pl.read_csv(COOKBOOK_DATA_DIR / "contoso_sales.csv", try_parse_dates=True)

# group by with aggregate and sort
print(
    df.group_by(pl.col("Brand"))
    .agg(
        pl.col("Quantity").sum().alias("Total Quantity"),
        pl.col("Unit Price").mean().alias("Avg Unit Price"),
        pl.col("Unit Price").min().alias("Min Unit Price"),
        pl.col("Unit Price").max().alias("Max Unit Price"),
        pl.col("Quantity").mean().alias("Avg Quantity"),
    )
    .sort(pl.col("Total Quantity"), descending=True)
)


print(
    df.group_by(pl.col("Brand"))
    .agg(
        pl.col("Unit Price").mean().round(2).alias("Average Unit Price"),
        (pl.col("Unit Price").sum() / pl.len()).round(2).alias("Average Unit Price 2"),
        pl.col("Customer Name").first().alias("First Customer"),
        pl.col("Category").last().alias("Last Category"),
    )
    .sort("Average Unit Price", descending=True)
    .sort("Brand")
    .head()
)

with pl.Config() as config:
    config.set_fmt_str_lengths = 50
    print(df.select(pl.col("Brand")).unique().head(15))

name: tuple  # it's a tuple in format (name,)
for idx, (name, data) in enumerate(df.group_by(["Brand"])):
    if idx > 5:
        break
    print(name[0], type(data))


# you can aggregate numeric columns without any function
# it will just convert it from rows to list of values
# eg: to -> [1, 2, 3, 4, 5]
print(df.group_by("Brand", maintain_order=True).agg(pl.col("Quantity")).head())

print(
    df.group_by(
        pl.col("Brand"),
        pl.col("Customer Country"),
        pl.col("Order Date").dt.year().alias("Order Year"),
    )
    .agg(pl.col("Unit Price").mean())
    .head()
)

# aggregate horizontally
df = pl.read_csv(COOKBOOK_DATA_DIR / "pokemon.csv", has_header=True)
print(df.head())

print(
    df.with_columns(
        pl.sum_horizontal(
            pl.col("HP"),
            pl.col("Attack"),
            pl.col("Defense"),
            pl.col("Sp. Atk"),
            pl.col("Sp. Def"),
            pl.col("Speed"),
        ).alias("Total Stats"),
        pl.mean_horizontal(
            pl.col("HP"),
            pl.col("Attack"),
            pl.col("Defense"),
            pl.col("Sp. Atk"),
            pl.col("Sp. Def"),
            pl.col("Speed"),
        ).alias("Mean Stats"),
    )
)

# it same as above
print(
    df.with_columns(
        pl.concat_list(
            pl.col("HP"),
            pl.col("Attack"),
            pl.col("Defense"),
            pl.col("Sp. Atk"),
            pl.col("Sp. Def"),
            pl.col("Speed"),
        )
        .list.sum()
        .alias("Concatenated Stats"),
    )
)

# using functional programming approach
print(
    df.with_columns(
        pl.reduce(
            function=lambda acc, col: acc + col,
            exprs=[
                pl.col("HP"),
                pl.col("Attack"),
                pl.col("Defense"),
                pl.col("Sp. Atk"),
                pl.col("Sp. Def"),
                pl.col("Speed"),
            ],
        ).alias("Total Stats"),
    )
)

print(
    df.with_columns(
        pl.fold(
            acc=pl.lit(0),
            function=lambda acc, col: acc + col,
            exprs=[
                pl.col("HP"),
                pl.col("Attack"),
                pl.col("Defense"),
                pl.col("Sp. Atk"),
                pl.col("Sp. Def"),
                pl.col("Speed"),
            ],
        ).alias("Total Stats"),
    )
)

# filter with functional programming
print(
    df.filter(
        pl.fold(
            acc=pl.lit(True),
            function=lambda acc, col: acc & col,
            exprs=[
                pl.col("HP") > 80,
                pl.col("Attack") > 80,
                pl.col("Defense") > 80,
                pl.col("Sp. Atk") > 80,
                pl.col("Sp. Def") > 80,
                pl.col("Speed") > 80,
            ],
        )
    )
)
# filter with all_horizontal function - same as above
print(
    df.filter(
        pl.all_horizontal(
            pl.col("HP") > 80,
            pl.col("Attack") > 80,
            pl.col("Defense") > 80,
            pl.col("Sp. Atk") > 80,
            pl.col("Sp. Def") > 80,
            pl.col("Speed") > 80,
        )
    )
)


str_cols = ["Name", "Type 1", "Type 2"]
str_combined = pl.fold(
    acc=pl.lit(""),
    function=lambda acc, col: acc + col,
    exprs=str_cols,
).alias("Combined Types")
print(df.select([*str_cols, str_combined]))

print(df.select(pl.concat_str(str_cols)).head())
