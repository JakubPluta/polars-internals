import polars as pl
from _const import COOKBOOK_DATA_DIR
from datetime import date

df = pl.read_csv(COOKBOOK_DATA_DIR / "academic.csv")
print(df)


df = df.select(
    pl.col("year").alias("academic_year"),
    pl.selectors.numeric().cast(pl.Int64),
).filter(pl.col("academic_year").str.slice(0, 4).cast(pl.Int64).ge(2018))

print(df.head())
# unpivot from many columns to many rows with one column
long_df = df.unpivot(
    index="academic_year",
    on=["students", "us_students", "undergraduate", "non_degree", "opt"],
    variable_name="student_type",
    value_name="count",
)

print(long_df.head())

# same result
print(df.unpivot(index="academic_year", on=pl.selectors.numeric()).head())


# rows into columns

long_df = (
    pl.read_csv(COOKBOOK_DATA_DIR / "academic.csv")
    .select(
        pl.col("year").alias("academic_year"),
        pl.selectors.numeric().cast(pl.Int64),
    )
    .filter(pl.col("academic_year").str.slice(0, 4).cast(pl.Int64).ge(2018))
    .unpivot(
        index="academic_year",
        on=["students", "us_students", "undergraduate", "non_degree", "opt"],
        variable_name="student_type",
        value_name="count",
    )
)
print(long_df.head())

print(
    long_df.pivot(
        index="academic_year",
        columns="student_type",
        values="count",
    ).head()
)


print(
    long_df.group_by("academic_year", maintain_order=True).agg(
        pl.col("count").filter(
            pl.col("student_type").eq("students")
        )  # aggregate count column values into list, and filter to only students
    )
)

print(
    (
        long_df.group_by("academic_year", maintain_order=True).agg(
            pl.col("count")
            .filter(pl.col("student_type").eq("students"))
            .sum()
            .alias("students"),
            pl.col("count")
            .filter(pl.col("student_type").eq("us_students"))
            .sum()
            .alias("us_students"),
            pl.col("count")
            .filter(pl.col("student_type").eq("undergraduate"))
            .sum()
            .alias("undergraduate"),
            pl.col("count")
            .filter(pl.col("student_type").eq("graduate"))
            .sum()
            .alias("graduate"),
            pl.col("count")
            .filter(pl.col("student_type").eq("non_degree"))
            .sum()
            .alias("non_degree"),
            pl.col("count").filter(pl.col("student_type").eq("opt")).sum().alias("opt"),
        )
    )
)

# unique student types as list
print(long_df.select("student_type").unique().to_series().to_list())
student_types = long_df.select("student_type").unique().to_series().to_list()


aggregation_columns = [
    pl.col("count").filter(pl.col("student_type").eq(st)).sum().alias(st)
    for st in student_types
]
print(
    long_df.group_by(pl.col("academic_year"), maintain_order=True).agg(
        aggregation_columns
    )
)


# Unstack a long table to a wide form without doing an aggregation.
# step -> number of output rows
print(long_df.unstack(step=5, columns="count", how="vertical"))

print(pl.concat([df.head(1), df]))  # kind of union all



# joining data frames