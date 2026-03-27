"""
ingest_weekly.py — pull the latest week of game data, build in-season features,
and upsert to the player_weeks table in Supabase.

Runs every Tuesday at 6am during the NFL season (Sep–Jan) via GitHub Actions.
Defaults to the most recently completed week; can be overridden for backfills.

Usage:
    python pipelines/ingest_weekly.py
    python pipelines/ingest_weekly.py --season 2024 --week 12
    python pipelines/ingest_weekly.py --season 2024  # ingests all weeks in that season
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import nfl_data_py as nfl
from dotenv import load_dotenv
from supabase import create_client, Client

from pipelines.features import build_weekly_features
from pipelines.utils import df_to_records

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

TABLE = "player_weeks"
UPSERT_BATCH_SIZE = 500

# Regular season only (weeks 1–18); excludes playoffs
MAX_REGULAR_SEASON_WEEK = 18


def main() -> None:
    args = _parse_args()

    season = args.season or _current_nfl_season()
    weeks = [args.week] if args.week else _completed_weeks(season)

    if not weeks:
        print(f"No completed weeks found for season {season}. Exiting.")
        sys.exit(0)

    print(f"Building weekly features for season {season}, weeks {weeks}")
    df = build_weekly_features(season, weeks=weeks)
    print(f"  {len(df)} rows built")

    if df.empty:
        print("No rows to upsert. Exiting.")
        sys.exit(0)

    rows = df_to_records(df)
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    total = _upsert_batched(client, rows)
    print(f"Upserted {total} rows to {TABLE}")


# ---------------------------------------------------------------------------
# NFL season / week helpers
# ---------------------------------------------------------------------------

def _current_nfl_season() -> int:
    """
    Return the current NFL season year.
    The NFL season that starts in September belongs to that calendar year,
    so before September we return the previous year's season.
    """
    now = datetime.now()
    return now.year if now.month >= 9 else now.year - 1


def _completed_weeks(season: int) -> list[int]:
    """
    Return the list of regular-season weeks that have completed results available.
    Uses nfl_data_py schedules to find weeks where all games have a recorded winner.
    Falls back to returning [1] if schedule data is unavailable.
    """
    try:
        schedule = nfl.import_schedules([season])
        # A game is complete when home_score is populated
        completed = schedule[schedule["home_score"].notna()]
        completed = completed[completed["week"] <= MAX_REGULAR_SEASON_WEEK]
        weeks = sorted(completed["week"].unique().tolist())
        return [int(w) for w in weeks]
    except Exception as e:
        print(f"Warning: could not determine completed weeks ({e}). Defaulting to week 1.")
        return [1]


# ---------------------------------------------------------------------------
# Shared helpers (same pattern as ingest_seasonal)
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest weekly features into Supabase.")
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="NFL season year (e.g. 2024). Defaults to the current season.",
    )
    parser.add_argument(
        "--week",
        type=int,
        default=None,
        help="Specific week to ingest. Defaults to all completed weeks in the season.",
    )
    return parser.parse_args()


def _upsert_batched(client: Client, rows: list[dict]) -> int:
    """Upsert rows in batches. Returns total rows upserted."""
    total = 0
    for i in range(0, len(rows), UPSERT_BATCH_SIZE):
        batch = rows[i : i + UPSERT_BATCH_SIZE]
        client.table(TABLE).upsert(batch).execute()
        total += len(batch)
        print(f"  {total}/{len(rows)} rows upserted...")
    return total


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
