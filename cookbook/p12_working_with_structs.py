import polars as pl
from _const import COOKBOOK_DATA_DIR
from datetime import date

df = pl.read_json(COOKBOOK_DATA_DIR / "ga_20170801.json", infer_schema_length=2222)
print(df)

cols = [
    "visitId",
    "date",
    "totals",
    "trafficSource",
    "customDimensions",
    "channelGrouping",
]
df = df.select(cols)
print(df.head())

df = df.with_columns(
    pl.struct("visitId", "date", "channelGrouping").alias("structFromCols")
)
print(df.head())

print(
    (
        df.select(
            "visitId",
            "date",
            "channelGrouping",
            pl.struct("visitId", "date", "channelGrouping").alias("structFromCols"),
        )
    ).head()
)

print(
    (
        df.group_by("channelGrouping")  # group by channelGrouping
        .agg(
            "visitId", pl.col("visitId").len().alias("numVisits")
        )  # aggregate visitId into list, and count its length into numVisits column
        .sort("numVisits")
        .with_columns(
            pl.col("visitId").list.to_struct().alias("struct_from_list")
        )  # convert list to struct, with field_0, field_1, field_2 .. as keys
    )
)

print(df.select("structFromCols").head())
print(
    df.select(
        "structFromCols",
        pl.col("structFromCols").alias("structFromColsToBeUnpacked"),
    )
    .unnest("structFromColsToBeUnpacked")
    .head()  # unnest struct  Decompose struct columns into separate columns for each of their fields.
)
print(df.select("trafficSource").schema)
print(df.select(pl.col("trafficSource")).unnest("trafficSource").head())


print(
    (
        df.select(
            "structFromCols",
            pl.col("structFromCols")
            .struct.rename_fields(["a", "b", "c"])  # rename struct fields
            .alias("renamedStructToBeUnpacked"),
        ).unnest(
            "renamedStructToBeUnpacked"
        )  # and unnest
    ).head()
)


# extract single field from struct

print(
    (
        df.select(
            "structFromCols",
            pl.col("structFromCols").struct.field("channelGrouping"),
        )
    ).head()
)


print(
    (
        df.select(
            pl.struct( # create struct as combination of channelGrouping and trafficSource.source
                pl.col("channelGrouping"),
                pl.col("trafficSource").struct.field("source"),
            )
            .unique() # take unique values
            .alias("channelAndSource")
        )
        .unnest("channelAndSource") # unnest into separate columns
        .sort("channelGrouping", "source")
    )
)
