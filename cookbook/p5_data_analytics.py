import polars as pl
from _const import COOKBOOK_DATA_DIR
import polars.selectors as cs

us_state_division_dict = {
    "Connecticut": "New England",
    "Maine": "New England",
    "Massachusetts": "New England",
    "New Hampshire": "New England",
    "Rhode Island": "New England",
    "Vermont": "New England",
    "New Jersey": "Mid-Atlantic",
    "New York": "Mid-Atlantic",
    "Pennsylvania": "Mid-Atlantic",
    "Illinois": "East North Central",
    "Indiana": "East North Central",
    "Michigan": "East North Central",
    "Ohio": "East North Central",
    "Wisconsin": "East North Central",
    "Iowa": "West North Central",
    "Kansas": "West North Central",
    "Minnesota": "West North Central",
    "Missouri": "West North Central",
    "Nebraska": "West North Central",
    "North Dakota": "West North Central",
    "South Dakota": "West North Central",
    "Delaware": "South Atlantic",
    "Florida": "South Atlantic",
    "Georgia": "South Atlantic",
    "Maryland": "South Atlantic",
    "North Carolina": "South Atlantic",
    "South Carolina": "South Atlantic",
    "Virginia": "South Atlantic",
    "West Virginia": "South Atlantic",
    "Alabama": "East South Central",
    "Kentucky": "East South Central",
    "Mississippi": "East South Central",
    "Tennessee": "East South Central",
    "Arkansas": "West South Central",
    "Louisiana": "West South Central",
    "Oklahoma": "West South Central",
    "Texas": "West South Central",
    "Arizona": "Mountain",
    "Colorado": "Mountain",
    "Idaho": "Mountain",
    "Montana": "Mountain",
    "Nevada": "Mountain",
    "New Mexico": "Mountain",
    "Utah": "Mountain",
    "Wyoming": "Mountain",
    "Alaska": "Pacific",
    "California": "Pacific",
    "Hawaii": "Pacific",
    "Oregon": "Pacific",
    "Washington": "Pacific",
}


df = pl.read_csv(COOKBOOK_DATA_DIR / "covid_19_deaths.csv", has_header=True)
print(df.head())
print(df.tail(5))


#    The formatting shows one line per column so that wide dataframes display
#         cleanly. Each line shows the column name, the data type, and the first
#         few values.
print(df.glimpse(max_items_per_column=2))

print(df.estimated_size(unit="mb"))

# select only numeric columns, and describe them
print(df.select(cs.numeric()).describe())

# count nulls
print(df.null_count())
print(df.select(cs.numeric()).null_count())

# ctx manager for config changes
with pl.Config() as config:
    config.set_tbl_cols(12)
    print(df.head())


# casting datatypes

updated_df = df.with_columns(
    pl.col("Data As Of").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("Start Date").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("End Date").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("End Date").str.to_date("%m/%d/%Y").alias("End Date As Date"),
    pl.col("Year").cast(pl.Int16),
)

print(updated_df.head())

# same for lazy frame
df = pl.scan_csv(COOKBOOK_DATA_DIR / "covid_19_deaths.csv", has_header=True)
updated_df = df.with_columns(
    pl.col("Data As Of").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("Start Date").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("End Date").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("End Date").str.to_date("%m/%d/%Y").alias("End Date As Date"),
    pl.col("Year").cast(pl.Int16),
)
print(updated_df.collect().head())


# remove duplicates
df = pl.read_csv(COOKBOOK_DATA_DIR / "covid_19_deaths.csv", has_header=True)
print(df.head())
print(df.height)
print(df.is_duplicated().sum())
print(df.is_unique().sum())
print(df.n_unique())

# select all columns and calculate n_unique for each
print(df.select(pl.all().n_unique()))
print(
    df.n_unique(subset=["Start Date", "End Date"])
)  # number of unique start and end dates combinations
print(
    df.unique(
        subset=["Start Date", "End Date"],
        keep="first",
    )
)

unique_rows_by_year_deaths = df.select(["Year", "COVID-19 Deaths"]).is_unique()
print(unique_rows_by_year_deaths.sum())

print(df.filter(unique_rows_by_year_deaths).head(1))
print(df.filter(unique_rows_by_year_deaths).shape)

# This is done using the HyperLogLog++ algorithm for cardinality estimation.
print(df.select(pl.all().approx_n_unique()))


# filtering

df = pl.read_csv(COOKBOOK_DATA_DIR / "covid_19_deaths.csv", has_header=True)
age_groups = [
    "0-17 years",
    "18-29 years",
    "30-39 years",
    "40-49 years",
    "50-64 years",
    "65-74 years",
    "75-84 years",
    "85 years and over",
    "All Ages",
]


filtered_df = df.filter(
    pl.col("Month").is_not_null(),
    pl.col("Age Group").is_in(age_groups),
)
print(filtered_df.shape)
print(filtered_df.head())

# cast datatypes
filtered_df = filtered_df.with_columns(
    pl.col("Data As Of").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("Start Date").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("End Date").str.strptime(pl.Date, "%m/%d/%Y"),
    pl.col("Year").cast(pl.Int64),
    pl.col("Month").cast(pl.Int64),
)
print(filtered_df.head())

# grouping and aggregation

covid_death_by_age = (
    filtered_df.filter(
        pl.col("State").eq("United States"),
        pl.col("Year").eq(2023),
        pl.col("Age Group").ne("All Ages"),
        pl.col("Sex").eq("All Sexes"),
    )
    .group_by(pl.col("Age Group"))
    .agg(
        pl.col("COVID-19 Deaths").sum().alias("Total Deaths"),
    )
    .sort(pl.col("Total Deaths"), descending=True)
)

print(covid_death_by_age.head())


covid_death_by_sex = (
    filtered_df.filter(
        pl.col("State").eq("United States"),
        pl.col("Year").eq(2023),
        pl.col("Age Group").ne("All Ages"),
        pl.col("Sex").ne("All Sexes"),
    )
    .group_by(pl.col("Sex"))
    .agg(
        pl.col("COVID-19 Deaths").sum().alias("Total Deaths"),
    )
    .sort(pl.col("Total Deaths"), descending=True)
)

print(covid_death_by_sex.head())


covid_deaths_vs_flu_deaths = (
    filtered_df.with_columns(
        pl.col("State")
        .replace_strict(us_state_division_dict, default="Others")
        .alias("Division")
    )
    .filter(
        pl.col("State").ne("United States"),
        pl.col("Year").eq(2023),
        pl.col("Age Group").ne("All Ages"),
        pl.col("Sex").ne("All Sexes"),
    )
    .group_by("State", "Division")
    .agg(
        pl.col("COVID-19 Deaths").sum(),
        pl.col("Influenza Deaths").sum(),
        pl.col("Pneumonia Deaths").sum(),
    )
)

print(covid_deaths_vs_flu_deaths.head())

# monthly trend by year


monthly_trend_by_year = (
    filtered_df.filter(
        pl.col("State").eq("United States"),
        pl.col("Age Group").eq("All Ages"),
        pl.col("Sex").eq("All Sexes"),
    )
    .group_by(pl.col("Year"), pl.col("Month"))
    .agg(
        pl.col("COVID-19 Deaths").sum().alias("Total Deaths"),
    )
    .sort(pl.col("Year"), pl.col("Month"))
)

print(monthly_trend_by_year)

q1 = pl.col("Influenza Deaths").quantile(0.25)
q3 = pl.col("Influenza Deaths").quantile(0.75)
iqr = q3 - q1
threshold = 1.5
ll = q1 - threshold * iqr
ul = q3 + threshold * iqr
print(
    filtered_df.filter(
        (pl.col("Influenza Deaths") < ll) | (pl.col("Influenza Deaths") > ul)
    )
)
