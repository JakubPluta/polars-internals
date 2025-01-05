import polars as pl
from _const import COOKBOOK_DATA_DIR
from datetime import date
import numpy as np
import random

date_col = pl.date_range(
    start=date(2022, 1, 1),
    end=date(2022, 1, 31),
    interval="1d",
    eager=True,
)

avg_temp_c = np.random.randint(0, 40, 31)
avg_temp_c = [random.choice([None, np.nan, *[x] * 6]) for x in avg_temp_c]
print(date_col, avg_temp_c)

df = pl.DataFrame(
    {"date": date_col, "avg_temp_c": avg_temp_c},
    strict=False,  # Throw an error if any `data` value does not exactly match the given or inferre data type for
    # that column
)
print(df.head())

# count nulls
print(df.null_count())
print(df.select(pl.col("avg_temp_c").is_null().sum()))
print(df.filter(pl.col("avg_temp_c").is_null()).select(pl.len()))
print(df.select(pl.col("avg_temp_c").is_nan().sum()))


df = pl.read_csv(COOKBOOK_DATA_DIR / "temperatures.csv")

print(df.head())
print(df.drop_nulls().null_count())
print(df.select(pl.col("avg_temp_celsius").drop_nulls().null_count()))
print(
    df.filter(
        pl.col("avg_temp_celsius")
        .is_not_null()
        .and_(pl.col("avg_temp_celsius").is_not_nan())
    )
)


cols_to_drop = [
    column for column in df.columns if df.select(pl.col(column).is_null().any()).item()
]
print(df.drop(cols_to_drop).columns)
print(df.fill_nan(None).drop_nulls())


# filling nulls with a specific value
print(
    df.select(
        "avg_temp_celsius",
        avg_temp_nulls_filled=pl.col("avg_temp_celsius").fill_null(pl.lit("1")),
    )
)
# different filling strategies for nulls
print(
    df.select(
        "avg_temp_celsius",
        forward_filled=pl.col("avg_temp_celsius").fill_null(strategy="forward"),
        backward_filled=pl.col("avg_temp_celsius").fill_null(strategy="backward"),
        mean_filled=pl.col("avg_temp_celsius").fill_null(strategy="mean"),
        min_filled=pl.col("avg_temp_celsius").fill_null(strategy="min"),
        max_filled=pl.col("avg_temp_celsius").fill_null(strategy="max"),
    )
)

# filling nulls with interpolation
print(
    df.fill_nan(None).select(
        "avg_temp_celsius",
        interpolated_linear=pl.col("avg_temp_celsius").interpolate(),
        interpolated_nearest=pl.col("avg_temp_celsius").interpolate(method="nearest"),
    )
)


print(
    df.select(
        "avg_temp_celsius",
        avg_temp_median=pl.col("avg_temp_celsius").fill_null(
            pl.col("avg_temp_celsius").median()
        ),
        avg_temp_max_minus_min=pl.col("avg_temp_celsius").fill_null(
            pl.col("avg_temp_celsius").max() - pl.col("avg_temp_celsius").min()
        ),
    )
)
