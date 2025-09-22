# ml/stages/ingest_once.py
import os
from typing import List, Dict
from supabase import create_client, Client


def _chunked(rows: List[Dict], n: int = 500):
    """Yield successive n-sized chunks from a list."""
    for i in range(0, len(rows), n):
        yield rows[i:i + n]


def upsert_table(sb: Client, table: str, rows: List[Dict]):
    """Helper to upsert rows into a Supabase table in batches."""
    if not rows:
        return
    for chunk in _chunked(rows, 500):
        sb.table(table).upsert(chunk).execute()


def run(season: int = 2025, week: int = 3):
    """
    Minimal ingestion job:
    - Reads Supabase credentials from environment
    - Upserts example rows into core tables
    """
    # ✅ Read env vars *inside* the function (Modal injects secrets here)
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError(
            "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment. "
            "Did you create the Modal secret 'supabase' and attach it to this function?"
        )

    sb: Client = create_client(url, key)

    # Example data (stub)
    players = [
        {"player_id": "123", "position": "RB", "team": "NYJ", "name": "Example RB"},
        {"player_id": "456", "position": "WR", "team": "BUF", "name": "Example WR"},
    ]

    schedule = [
        {"season": season, "week": week, "team": "NYJ", "opp": "BUF", "home": True},
        {"season": season, "week": week, "team": "BUF", "opp": "NYJ", "home": False},
    ]

    dvp = [
        {"season": season, "week": week, "team": "BUF", "position": "RB", "dvp": 3.2},
        {"season": season, "week": week, "team": "NYJ", "position": "WR", "dvp": 6.7},
    ]

    odds = [
        {
            "season": season,
            "week": week,
            "game_id": "nyj-buf-w3",
            "team": "NYJ",
            "opp": "BUF",
            "spread": -2.5,
            "moneyline": -135,
            "total": 42.5,
        },
        {
            "season": season,
            "week": week,
            "game_id": "nyj-buf-w3",
            "team": "BUF",
            "opp": "NYJ",
            "spread": 2.5,
            "moneyline": 115,
            "total": 42.5,
        },
    ]

    # Upsert to Supabase
    upsert_table(sb, "players", players)
    upsert_table(sb, "schedule", schedule)
    upsert_table(sb, "defense_vs_pos", dvp)
    upsert_table(sb, "odds", odds)

    return {
        "inserted": {
            "players": len(players),
            "schedule": len(schedule),
            "defense_vs_pos": len(dvp),
            "odds": len(odds),
        }
    }
