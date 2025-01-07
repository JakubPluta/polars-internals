import datetime
import logging
import pathlib

import polars as pl
from memory_profiler import profile
from polars import Schema
from polars.datatypes import (
    Boolean,
    Float64,
    Int64,
    List,
    String,
    Struct,
)

ROOT_DIR_PATH = pathlib.Path(__file__).resolve().parent.parent
DATA_DIR_PATH = ROOT_DIR_PATH / "data"
FAKE_GAMING_DATA_DIR = DATA_DIR_PATH / "fake_gaming_data"


log = logging.getLogger(__name__)

schema = Schema(
    {
        "team_id": String,
        "name": String,
        "created_date": String,
        "ranking": Int64,
        "total_winnings": Int64,
        "members": List(
            Struct(
                {
                    "player_id": String,
                    "username": String,
                    "account_details": Struct(
                        {
                            "email": String,
                            "registration_date": String,
                            "premium_status": Boolean,
                            "country": String,
                            "language": String,
                        }
                    ),
                    "stats": Struct(
                        {
                            "level": Int64,
                            "experience": Int64,
                            "total_matches": Int64,
                            "win_rate": Float64,
                            "playtime_hours": Int64,
                            "achievements_completed": Int64,
                        }
                    ),
                    "inventory": Struct(
                        {
                            "currency": Struct({"premium": Int64, "standard": Int64}),
                            "items": List(
                                Struct(
                                    {
                                        "item_id": String,
                                        "name": String,
                                        "type": String,
                                        "rarity": String,
                                        "level_requirement": Int64,
                                        "stats": Struct(
                                            {
                                                "attack": Int64,
                                                "defense": Int64,
                                                "magic": Int64,
                                                "speed": Int64,
                                            }
                                        ),
                                    }
                                )
                            ),
                        }
                    ),
                    "achievements": List(
                        Struct(
                            {
                                "id": String,
                                "name": String,
                                "difficulty": String,
                                "completion_rate": Float64,
                                "points": Int64,
                                "date": String,
                            }
                        )
                    ),
                    "recent_matches": List(
                        Struct(
                            {
                                "match_id": String,
                                "game_mode": String,
                                "map": String,
                                "duration_minutes": Int64,
                                "date": String,
                                "stats": Struct(
                                    {
                                        "kills": Int64,
                                        "deaths": Int64,
                                        "assists": Int64,
                                        "damage_dealt": Int64,
                                        "healing_done": Int64,
                                        "accuracy": Float64,
                                        "headshot_percentage": Float64,
                                        "objectives_completed": Int64,
                                    }
                                ),
                                "rewards": Struct(
                                    {
                                        "experience": Int64,
                                        "currency": Int64,
                                        "items_dropped": List(
                                            Struct(
                                                {
                                                    "item_id": String,
                                                    "name": String,
                                                    "rarity": String,
                                                    "value": Int64,
                                                }
                                            )
                                        ),
                                    }
                                ),
                            }
                        )
                    ),
                }
            )
        ),
        "tournament_history": List(
            Struct(
                {
                    "tournament_id": String,
                    "name": String,
                    "placement": Int64,
                    "prize_money": Int64,
                    "matches_played": Int64,
                }
            )
        ),
    }
)


@profile
def simple_pipeline():
    data: pl.LazyFrame = pl.scan_ndjson(
        FAKE_GAMING_DATA_DIR / "data.json",
        schema=schema,
        batch_size=5,
    )
    # polars.exceptions.InvalidOperationError: sink_Parquet: Seems like ndjson -> parquet in lazy way is not supported
    data.sink_parquet(FAKE_GAMING_DATA_DIR / "simple_pipeline.parquet")


@profile
def main():
    # Read Lazy JSON data
    pl.Config.set_streaming_chunk_size(1000)
    data: pl.LazyFrame = pl.scan_ndjson(
        FAKE_GAMING_DATA_DIR / "data.json",
        schema=schema,
    )
    agg_data: pl.LazyFrame = (
        (
            data.explode("members")
            .unnest("members")
            .with_columns(
                pl.col("account_details").struct["country"].alias("country"),
                pl.col("account_details")
                .struct["registration_date"]
                .alias("registration_date"),
                pl.col("player_id").alias("player_id"),
                pl.col("stats").struct["level"].alias("level"),
                pl.col("stats").struct["experience"].alias("experience"),
                pl.col("stats").struct["total_matches"].alias("total_matches"),
                pl.col("stats").struct["win_rate"].alias("win_rate"),
                pl.col("stats").struct["playtime_hours"].alias("playtime_hours"),
                pl.col("inventory").struct["items"].alias("items"),
                today=datetime.date.today(),
            )
            .select(
                [
                    pl.col("team_id").alias("team_id"),
                    pl.col("created_date")
                    .str.to_date("%Y-%m-%d")
                    .alias("team_created_date"),
                    pl.col("ranking").alias("team_ranking"),
                    pl.col("total_winnings").alias("team_total_winnings"),
                    pl.col("country").alias("player_country"),
                    pl.col("registration_date")
                    .str.to_date("%Y-%m-%d")
                    .alias("player_registration_date"),
                    pl.col("today").alias("today"),
                    pl.col("level").alias("player_level"),
                    pl.col("experience").alias("player_experience"),
                    pl.col("total_matches").alias("player_total_matches"),
                    pl.col("win_rate").alias("player_win_rate"),
                    pl.col("playtime_hours").alias("player_playtime_hours"),
                    pl.col("items")
                    .list.eval(pl.element().struct["rarity"].len())
                    .list.get(0, null_on_oob=True)
                    .alias("numb_rare_items"),
                ]
            )
        )
        .group_by(
            [
                "team_id",
                "team_created_date",
                "team_ranking",
                "team_total_winnings",
            ]
        )
        .agg(
            [
                pl.col("player_country")
                .value_counts()
                .sort(descending=True)
                .first()
                .struct.field("player_country")
                .alias("most_common_country"),
                (pl.col("today") - pl.col("player_registration_date"))
                .mean()
                .dt.total_days()
                .alias("avg_days_since_registration"),
                pl.col("player_level").mean().alias("avg_player_level"),
                pl.col("player_experience").mean().alias("avg_player_experience"),
                pl.col("player_total_matches").mean().alias("avg_player_total_matches"),
                pl.col("player_win_rate").mean().alias("avg_player_win_rate"),
                pl.col("player_playtime_hours")
                .mean()
                .alias("avg_player_playtime_hours"),
                pl.col("numb_rare_items").mean().alias("avg_numb_rare_items"),
            ]
        )
        .sort(by="team_ranking", descending=False)
    )
    agg_data.collect(streaming=True).write_parquet(
        FAKE_GAMING_DATA_DIR / "agg_data.parquet"
    )
    # agg_data.sink_parquet(FAKE_GAMING_DATA_DIR / "agg_data.parquet")  # Some opartions are not supported in sink_parquet, thus using collect().write_parquet()


if __name__ == "__main__":
    simple_pipeline()
