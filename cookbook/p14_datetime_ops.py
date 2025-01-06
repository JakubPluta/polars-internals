import datetime

import polars as pl
from _const import COOKBOOK_DATA_DIR
from datetime import date

df = pl.scan_csv(COOKBOOK_DATA_DIR / "toronto_weather.csv")
print(df.collect(streaming=True).head())

df = df.with_columns(pl.col("temperature") - 273.15)  # convert to celsius

print(df.collect(streaming=True).head())


df_date_parsed = pl.scan_csv(
    COOKBOOK_DATA_DIR / "toronto_weather.csv", try_parse_dates=True
)
print(df_date_parsed.head().collect())

print(df_date_parsed.collect_schema())
print(df_date_parsed.collect_schema().dtypes())

df = df.with_columns(pl.col("datetime").str.to_datetime())  # convert to datetime
print(df.head().collect())

print(
    (
        df.select(
            "datetime",
            pl.col("datetime").dt.year().alias("year"),  # extract year
            pl.col("datetime").dt.month().alias("month"),  # extract month
            pl.col("datetime").dt.day().alias("day"),  # extract day
            pl.col("datetime").dt.time().alias("time"),  # extract time
            pl.col("datetime").dt.hour().alias("hour"),  # extract hour
            pl.col("datetime").dt.minute().alias("minute"),  # extract minute
            pl.col("datetime").dt.second().alias("second"),  # extract second
        )
        .head()
        .collect()
    )
)


filtered_lf = df.filter(
    pl.col("datetime")
    .dt.date()
    .is_between(datetime.datetime(2017, 1, 1), datetime.datetime(2017, 12, 31)),
    pl.col("datetime").dt.hour() < 12,
)
print(filtered_lf.collect())


print(
    filtered_lf.select(
        pl.col("datetime").dt.year().unique().implode().list.len().alias("year_count"),
        pl.col("datetime").dt.hour().unique().implode().list.len().alias("hour_count"),
    ).collect()
)

# timezones examples
time_zones_lf = df.select(
    "datetime",
    pl.col("datetime")
    .dt.replace_time_zone(
        "America/Toronto"
    )  # replace time zone to Toronto  Replace time zone for an expression of type Datetime.
    .alias("replaced_time_zone_toronto"),
    pl.col("datetime")
    .dt.convert_time_zone(
        "America/Toronto"
    )  # `convert_time_zone`, this will also modify the underlying timestamp and will ignore the original time zone.
    .alias("converted_time_zone_toronto"),
)
print(time_zones_lf.head().collect())

# subtract and add dates/time
print(
    (
        df.select(
            "datetime",
            (pl.col("datetime") - pl.duration(weeks=5)).alias("minus_5weeks"),
            (pl.col("datetime") + pl.duration(milliseconds=5)).alias("plus_5ms"),
        )
        .head()
        .collect()
    )
)

# rolling window calculations:
print(
    df.select(
        pl.col("datetime"),
        pl.col("temperature"),
        pl.col("temperature").rolling_mean(3).alias("3hr_rolling_avg"),
    ).collect()
)

daily_avg_temperature_df = (
    df.select(pl.col("datetime").dt.date().alias("date"), pl.col("temperature"))
    .group_by("date", maintain_order=True)
    .agg(pl.col("temperature").mean().alias("daily_avg_temp"))
)
print(daily_avg_temperature_df.collect().head())

# calculating rolling averages
print(
    (
        daily_avg_temperature_df.select(
            "date",
            "daily_avg_temp",
            pl.col("daily_avg_temp").rolling_mean(3).alias("3day_rolling_avg"),
            pl.col("daily_avg_temp").rolling_min(3).alias("3day_rolling_min"),
            pl.col("daily_avg_temp").rolling_max(3).alias("3day_rolling_max"),
        )
        .head()
        .collect()
    )
)


print(
    (
        daily_avg_temperature_df.set_sorted(
            "date"
        )  # tell polars that data is sorted by date
        .select(
            "date",
            "daily_avg_temp",
            pl.col("daily_avg_temp").rolling_mean(3).alias("3day_rolling_avg"),
            pl.col("daily_avg_temp")
            .rolling_mean(
                window_size=3, min_periods=1
            )  # set min_periods to 1 to avoid empty rolling windows
            .alias("3day_rolling_avg2"),
            pl.col("daily_avg_temp")
            .mean()
            .rolling(
                index_column="date", period="3d", closed="right"
            )  # set closed to "right" to avoid empty rolling windows
            .alias("3day_rolling_avg3"),
        )
        .head(20)
        .collect()
    )
)
with pl.Config() as config:
    config.set_fmt_str_lengths(50)
    print(
        (
            daily_avg_temperature_df.set_sorted("date")
            .rolling("date", period="3d")
            .agg(
                pl.col(
                    "daily_avg_temp"
                ),  # keep the original column, but aggregate it to list
                pl.col("daily_avg_temp").mean().alias("3day_rolling_avg"),
                pl.col("daily_avg_temp").min().alias("3day_rolling_min"),
                pl.col("daily_avg_temp").max().alias("3day_rolling_max"),
            )
            .head(10)
            .collect()
        )
    )


def get_range(nums):
    return max(nums) - min(nums)


# Compute a custom rolling window function.
print(
    daily_avg_temperature_df.with_columns(
        pl.col("daily_avg_temp")
        .rolling_map(get_range, window_size=3)
        .alias("3day_rolling_range")
    )
    .head()
    .collect()
)

# Resampling

print(
    df.set_sorted("datetime")  # tell polars that data is sorted by date
    .group_by_dynamic(
        "datetime",
        every="1w",  # Group based on a time value (or index value of type Int32, Int64). Time windows are calculated and rows are assigned to windows. Different from a normal group by is that a row can be member of multiple groups.
    )
    .agg(pl.col("humidity").mean().round(1))
    .head(10)
    .collect()
)


upsampled_df = (
    df.set_sorted("datetime")
    .collect()
    .upsample(
        time_column="datetime",
        every="30m",  # sample every 30 minutes
        maintain_order=True,
    )
    .select("datetime", pl.col("humidity"))
)
print(upsampled_df.head())

print(
    (upsampled_df.with_columns(pl.col("humidity").interpolate()).head(10))
)  # interpolate missing values


# filtering df to not include hours 13, 15, 16, 19
datetime_with_gaps_lf = df.filter(~pl.col("datetime").dt.hour().is_in([13, 15, 16, 19]))

print(
    (
        datetime_with_gaps_lf.set_sorted("datetime")
        .collect()
        .upsample(time_column="datetime", every="1h", maintain_order=True)
        .select("datetime", pl.col("humidity").interpolate().round(2))
        .head(10)
    )
)

# forecasting
df = pl.scan_csv(
    COOKBOOK_DATA_DIR / "historical_temperatures.csv", try_parse_dates=True
)
print(df.collect(streaming=True).head())
time_col, entity_col, value_col = df.collect_schema().names()

ydf = (
    df.group_by_dynamic(
        time_col,
        every="1mo",
        group_by=entity_col,
    )
    .agg((pl.col(value_col).mean() - 273.15).alias(value_col))
)
print(ydf.group_by(entity_col).head(3).collect(streaming=True))

