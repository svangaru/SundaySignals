"""
ingest_seasonal.py — pull historical nfl_data_py data, build pre-season features,
and upsert to the player_seasons table in Supabase.

Runs twice a year via GitHub Actions:
  - March: captures final stats from the previous season
  - August: overwrites with updated rosters, OC/QB changes before Week 1

Usage:
    python pipelines/ingest_seasonal.py
    python pipelines/ingest_seasonal.py --seasons 2022 2023 2024
    python pipelines/ingest_seasonal.py --seasons 2024 --context-flags overrides/context_flags.csv
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client, Client

from pipelines.features import build_seasonal_features
from pipelines.utils import df_to_records

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

TABLE = "player_seasons"
UPSERT_BATCH_SIZE = 500
DEFAULT_START_SEASON = 2020


def main() -> None:
    args = _parse_args()

    seasons = args.seasons or list(range(DEFAULT_START_SEASON, datetime.now().year + 1))
    context_flags = args.context_flags

    print(f"Building seasonal features for seasons: {seasons}")
    df = build_seasonal_features(seasons, context_flags_path=context_flags)
    print(f"  {len(df)} rows built across {df['season'].nunique()} seasons")

    # Cast smallint DB columns to Python int before upsert
    # Note: draft_number, receiving_yards, rushing_yards are stored as smallint in DB
    SMALLINT_COLS = [
        "season", "games", "targets", "receptions", "receiving_tds", "carries",
        "draft_number", "receiving_yards", "rushing_yards",
    ]
    rows = df_to_records(df, int_cols=SMALLINT_COLS)

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    total = _upsert_batched(client, rows)
    print(f"Upserted {total} rows to {TABLE}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest seasonal features into Supabase.")
    parser.add_argument(
        "--seasons",
        nargs="+",
        type=int,
        default=None,
        help="Seasons to ingest (e.g. --seasons 2022 2023 2024). Defaults to 2020–current year.",
    )
    parser.add_argument(
        "--context-flags",
        type=Path,
        default=Path("overrides/context_flags.csv"),
        help="Path to context_flags.csv. Silently skipped if missing.",
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
