import polars as pl
from _const import COOKBOOK_DATA_DIR
from datetime import date

df = pl.read_csv(COOKBOOK_DATA_DIR / "us_videos.csv", has_header=True)
df = df.with_columns(pl.col("trending_date").str.to_date(format="%y.%d.%m"))
print(df.head())

# split string tags into a list
print(df.select("tags", pl.col("tags").str.split("|").alias("tags in list")).head())

print(
    (
        df.group_by("trending_date").agg(
            pl.col("video_id")
        )  # if you use agg without any function,
        # it will just convert it from rows to list of values
        .sort("trending_date", descending=True)
    ).head()
)

# concat columns into list
print(
    df.select(
        pl.concat_list(
            pl.col("views"),
            pl.col("likes"),
            pl.col("dislikes"),
            pl.col("comment_count"),
        ).alias("engagement")
    ).head()
)


df = pl.read_csv(
    COOKBOOK_DATA_DIR / "us_videos.csv", try_parse_dates=True
).with_columns(pl.col("trending_date").str.to_date(format="%y.%d.%m"))


aggregated_df = (
    df.group_by(
        pl.col("trending_date"),
    )
    .agg(
        pl.col("views"),
        pl.col("likes"),
        pl.col("dislikes"),
        pl.col("comment_count"),
    )
    .sort("trending_date", descending=True)
)

print(aggregated_df.head())


print(
    aggregated_df.select(
        pl.col("trending_date"),
        pl.col("views").list.min().alias("views_min"),
        pl.col("likes").list.max().alias("likes_max"),
        pl.col("dislikes").list.mean().alias("dislikes_mean"),
        pl.col("comment_count").list.sum().alias("comment_count_sum"),
    ).sort("trending_date", descending=True)
)
print(
    (
        aggregated_df.select(
            "trending_date", pl.col("views").list.len().alias("item_cnt")
        )
    ).head()
)


print(
    (
        df.group_by("trending_date")
        .agg(pl.col("channel_title"))  # aggregate channel_title into list
        .with_columns(
            pl.col("channel_title").list.join(":")
        )  # concatenate all values from channel_title into a string
        .sort("trending_date", descending=True)
    ).head()
)


#
trending_dates_by_channel = (
    df.group_by("channel_title")
    .agg("trending_date")  # aggregate trending_date into list
    .with_columns(
        pl.col("trending_date").list.sort()
    )  # sort the list by date ascending -> sorting within single row
    .sort("channel_title")  # sorting all rows at dataframe
)

print(trending_dates_by_channel.head())

print(
    trending_dates_by_channel.with_columns(
        pl.col("trending_date")
        .list.first()
        .alias("first_trending_date"),  # extract first value
        pl.col("trending_date")
        .list.last()
        .alias("last_trending_date"),  # extract last value
    ).head()
)

print(
    trending_dates_by_channel.with_columns(
        pl.col("trending_date")
        .list.get(7, null_on_oob=True)
        .alias(
            "8th_element"
        )  # extract 8th element from list, if there is no 8th element, return null
    ).head()
)


print(
    trending_dates_by_channel.with_columns(
        pl.col("trending_date").list.head().alias("first_5"),
        pl.col("trending_date").list.tail(10).alias("last_10"),
    ).head()
)

print(
    trending_dates_by_channel.with_columns(
        pl.col("trending_date")
        .list.sort(descending=True)
        .list.head(3)
        .alias("3_most_recent_dates")
    ).head()
)

print(
    trending_dates_by_channel.select(
        "trending_date",
        pl.col("trending_date").list.slice(0, 2).alias("first_2_dates"),
        pl.col("trending_date").list.slice(-3, 1).alias("3rd_date_to_last"),
        pl.col("trending_date").list.slice(7).alias("from_8th_date_to_end"),
    ).head()
)


print(
    (
        df.group_by("trending_date")
        .agg("category_id")  # aggregate category_id into list
        .with_columns(pl.col("category_id").list.sort())  # sort the list
        .with_columns(
            pl.col("category_id"),
            pl.col("category_id")
            .list.len()
            .alias("category_id_cnt"),  # count elements inside list
            pl.col("category_id")
            .list.unique()
            .alias("category_id_unique"),  # remove duplicates
            pl.col("category_id")
            .list.unique()
            .list.len()
            .alias("category_id_unique_cnt"),  # count unique values
        )
    ).head()
)

print(
    trending_dates_by_channel.with_columns(
        pl.col("trending_date")
        .list.sample(n=2, with_replacement=True, seed=1)  #  take 2 samples from list
        .alias("samples")
    ).head()
)


aggregated_df = df.group_by("trending_date").agg("views", "channel_title")

print(aggregated_df.head())


# eval -> Run any polars expression against the lists’ elements.
channel_titles_df = aggregated_df.select(
    pl.col("channel_title").list.head(2),  # get first 2 channel titles
    pl.col("channel_title")
    .list.eval(pl.element().str.to_uppercase())  # convert to uppercase using eval
    .list.head(2)  # get first 2
    .alias("channel_title_upper"),  # alias
)
print(channel_titles_df)


print(
    (
        channel_titles_df.with_columns(
            pl.col("channel_title_upper").list.eval(
                pl.element().filter(pl.element().str.contains("A", literal=True))
            )
        )
    ).head()
)


views_rank_df = aggregated_df.select(
    "trending_date",
    "views",
    pl.col("views")
    .list.eval(pl.element().rank("dense", descending=True))
    .alias("views_rank"),
)
print(views_rank_df.head())


print(views_rank_df.explode("views", "views_rank").filter(pl.col("views_rank") <= 3))


top3_views_df = (
    views_rank_df.explode("views", "views_rank")  # explode list of views into rows
    .filter(pl.col("views_rank") <= 3)  # filter top 3
    .group_by("trending_date")  # group it now by date
    .agg(pl.all())  # aggregate all columns, it will convert rows into lists
)
print(top3_views_df.head())
