import json
import random
import uuid
from datetime import datetime, timedelta, timezone

from faker import Faker

fake = Faker()

NOW = datetime.now(tz=timezone.utc)
START_DATE = NOW - timedelta(days=90)
END_DATE = NOW


def random_date():
    return START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days))


def generate_achievement():
    return {
        "id": str(uuid.uuid4()),
        "name": random.choice(
            [
                "Master of Combat",
                "Speed Runner",
                "100% Completion",
                "Hidden Treasure",
                "Ultimate Boss Slayer",
            ]
        ),
        "difficulty": random.choice(["Easy", "Medium", "Hard", "Extreme"]),
        "completion_rate": round(random.uniform(0.1, 100.0), 2),
        "points": random.randint(10, 1000),
        "date": random_date().strftime("%Y-%m-%d"),
    }


def generate_match_stats():
    return {
        "kills": random.randint(0, 30),
        "deaths": random.randint(0, 20),
        "assists": random.randint(0, 15),
        "damage_dealt": random.randint(1000, 50000),
        "healing_done": random.randint(0, 10000),
        "accuracy": round(random.uniform(20.0, 95.0), 2),
        "headshot_percentage": round(random.uniform(5.0, 40.0), 2),
        "objectives_completed": random.randint(0, 10),
    }


def generate_match():
    return {
        "match_id": str(uuid.uuid4()),
        "game_mode": random.choice(["Ranked", "Casual", "Custom", "Tournament"]),
        "map": random.choice(
            [
                "Desert Temple",
                "Arctic Base",
                "Jungle Ruins",
                "Space Station",
                "Underground City",
            ]
        ),
        "duration_minutes": random.randint(15, 45),
        "date": random_date().strftime("%Y-%m-%d"),
        "stats": generate_match_stats(),
        "rewards": {
            "experience": random.randint(100, 1000),
            "currency": random.randint(50, 500),
            "items_dropped": [
                {
                    "item_id": str(uuid.uuid4()),
                    "name": random.choice(
                        ["Rare Sword", "Epic Shield", "Legendary Armor", "Mythic Ring"]
                    ),
                    "rarity": random.choice(
                        ["Common", "Rare", "Epic", "Legendary", "Mythic"]
                    ),
                    "value": random.randint(100, 10000),
                }
                for _ in range(random.randint(1, 4))
            ],
        },
    }


def generate_player():
    level = random.randint(1, 100)
    return {
        "player_id": str(uuid.uuid4()),
        "username": fake.user_name(),
        "account_details": {
            "email": fake.email(),
            "registration_date": random_date().strftime("%Y-%m-%d"),
            "premium_status": random.choice([True, False]),
            "country": fake.country(),
            "language": random.choice(["en", "es", "fr", "de", "ja"]),
        },
        "stats": {
            "level": level,
            "experience": level * random.randint(1000, 2000),
            "total_matches": random.randint(100, 1000),
            "win_rate": round(random.uniform(40.0, 70.0), 2),
            "playtime_hours": random.randint(100, 5000),
            "achievements_completed": random.randint(10, 100),
        },
        "inventory": {
            "currency": {
                "premium": random.randint(0, 10000),
                "standard": random.randint(1000, 100000),
            },
            "items": [
                {
                    "item_id": str(uuid.uuid4()),
                    "name": random.choice(
                        [
                            "Dragon Sword",
                            "Mage Staff",
                            "Heavy Armor",
                            "Magic Ring",
                            "Ancient Relic",
                        ]
                    ),
                    "type": random.choice(
                        ["Weapon", "Armor", "Accessory", "Consumable"]
                    ),
                    "rarity": random.choice(["Common", "Rare", "Epic", "Legendary"]),
                    "level_requirement": random.randint(1, 100),
                    "stats": {
                        "attack": random.randint(0, 100),
                        "defense": random.randint(0, 100),
                        "magic": random.randint(0, 100),
                        "speed": random.randint(0, 100),
                    },
                }
                for _ in range(random.randint(5, 15))
            ],
        },
        "achievements": [generate_achievement() for _ in range(random.randint(5, 20))],
        "recent_matches": [generate_match() for _ in range(random.randint(5, 10))],
    }


def generate_team():
    return {
        "team_id": str(uuid.uuid4()),
        "name": f"{random.choice(['Team', 'Squad', 'Guild'])} {fake.company()}",
        "created_date": random_date().strftime("%Y-%m-%d"),
        "ranking": random.randint(1, 1000),
        "total_winnings": random.randint(1000, 1000000),
        "members": [generate_player() for _ in range(random.randint(5, 10))],
        "tournament_history": [
            {
                "tournament_id": str(uuid.uuid4()),
                "name": f"{random.choice(['Champion', 'Pro', 'Elite'])} Series {random.randint(1, 10)}",
                "placement": random.randint(1, 16),
                "prize_money": random.randint(1000, 100000),
                "matches_played": random.randint(3, 10),
            }
            for _ in range(random.randint(3, 8))
        ],
    }


def generate_large_dataset_ndjson(output_file, target_size_gb=1):
    """
    Generate a dataset targeting a specific size in gigabytes in NDJSON format.
    Each line contains one complete JSON record.
    """
    bytes_per_gb = 1024 * 1024 * 1024  # 1 GB in bytes
    target_bytes = target_size_gb * bytes_per_gb
    current_size = 0
    records_written = 0

    with open(output_file, "w") as f:
        while current_size < target_bytes:
            data = generate_team()  # używamy istniejącej funkcji generate_team()
            json_line = json.dumps(data) + "\n"  # dodajemy znak nowej linii
            f.write(json_line)

            current_size = f.tell()
            records_written += 1

            if records_written % 100 == 0:
                print(
                    f"Generated {records_written} records... Current size: {current_size / bytes_per_gb:.2f} GB"
                )

    print(f"Final size: {current_size / bytes_per_gb:.2f} GB")
    print(f"Total records: {records_written}")


# Usage example:
if __name__ == "__main__":
    # Generate approximately 1GB of data
    generate_large_dataset_ndjson("data/fake_gaming_data/data.json", target_size_gb=1)
