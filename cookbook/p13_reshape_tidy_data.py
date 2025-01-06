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

academic_df = (
    pl.read_csv(COOKBOOK_DATA_DIR / "academic.csv")
    .select(
        pl.col("year").alias("academic_year"),
        pl.selectors.numeric().cast(pl.Int64),
    )
    .filter(pl.col("academic_year").str.slice(0, 4).cast(pl.Int32).ge(2018))
)
print(academic_df)

status_df = pl.read_csv(COOKBOOK_DATA_DIR / "status.csv").with_columns(
    pl.selectors.float().cast(pl.Int64)
)
print(status_df)

joined_df = academic_df.join(
    status_df, left_on="academic_year", right_on="year", how="inner"
).select(pl.col("academic_year"), pl.col("students"), pl.selectors.contains("visa"))


viz_df = joined_df.unpivot(
    index=["academic_year", "students"],
    on=pl.selectors.contains("visa"),
    variable_name="visa_type",
    value_name="count",
).with_columns((pl.col("count") / pl.col("students")).alias("percent_of_total"))
print(viz_df.head())

status_long_df = status_df.unpivot(
    index="year",
    on=pl.selectors.contains("visa"),
    variable_name="visa_type",
    value_name="count",
)
print(status_long_df.head())


print(
    academic_df.join(
        status_long_df,
        left_on="academic_year",
        right_on="year",
        how="inner",
        validate="1:m",  # validate if it's one-many relationship
    ).select(
        pl.col("academic_year"),
        pl.col("students"),
        pl.col("visa_type"),
        pl.col("count"),
    )
)

try:
    academic_df.join(
        status_long_df,
        left_on="academic_year",
        right_on="year",
        how="inner",
        validate="1:1",  # validate if it's one-one relationship
    ).select(
        pl.col("academic_year"),
        pl.col("students"),
        pl.col("visa_type"),
        pl.col("count"),
    )
except pl.exceptions.ComputeError as e:
    print(f"Join keys couldn't fulfill 1:1 validation {str(e)}")


a = pl.DataFrame({"int": [1, 2, 3], "value": [10, 20, 30]}).set_sorted(
    "int"
)  # Indicate that one or multiple columns are sorted. This can speed up future operations.
b = pl.DataFrame({"int": [4, 5, 6]}).set_sorted("int")
print(a)
print(b)

# join asof
print(b.join_asof(a, on="int", strategy="backward"))
left_df = pl.DataFrame({"time": [1, 2, 3, 6], "value": [10, 20, 30, 60]})
right_df = pl.DataFrame({"time": [1, 4, 5], "info": ["A", "B", "C"]})

# Asof join This is similar to a left-join except that we match on nearest key rather than equal keys.
result = left_df.join_asof(right_df, on="time", strategy="backward")
print(result)


df1, df2, df3 = (
    academic_df.head(),
    academic_df.slice(2, 2),
    academic_df.tail(3),
)

print(df1, df2, df3)

# concat vertical similar to union
print(pl.concat([df1, df2, df3], how="vertical"))

# Grow this DataFrame vertically by stacking a DataFrame to it.
print(df1.vstack(df2).vstack(df3))

# concat horizontal it means that the columns are combined, if left have more columns, it will be filled with null
print(
    pl.concat([df1.select("academic_year"), df2.select("students")], how="horizontal")
)
print(
    pl.concat(
        [
            df1.select("academic_year"),
            df2.select("students"),
            df3.select("us_students"),
        ],
        how="horizontal",
    )
)
# Different from `vstack` which adds the chunks from `other` to the chunks of
#         this `DataFrame`, `extend` appends the data from `other` to the underlying
#         memory locations and thus may cause a reallocation.
print(df1.extend(df2).extend(df3))


# partitioning - Group by the given columns and return the groups as separate dataframes.
print(academic_df.partition_by("academic_year"))

# This is a very expensive operation. Perhaps you can do it differently.
print(academic_df.transpose(include_header=True))
