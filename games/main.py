import logging
import pathlib

import polars as pl
from polars import Schema
from polars.datatypes import Boolean, Float64, Int64, List, String, Struct

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


data = pl.scan_ndjson(
    FAKE_GAMING_DATA_DIR / "data.json",
    schema=schema,
)

print(
    data.limit(100)
    .collect()
    .select(
        pl.col("team_id"), pl.col("name"), pl.col("members"), pl.col("created_date")
    )
    .explode("members")
    .unnest("members")
    .select(
        pl.col("team_id"), pl.col("created_date"), pl.col("username"), pl.col("stats")
    )
    .unnest("stats")
    .group_by("team_id", "created_date")
    .agg(
        [
            pl.col("username").first().alias("team_name"),
            pl.col("level").sum().alias("total_level"),
            pl.col("experience").sum().alias("total_experience"),
        ]
    )
)
