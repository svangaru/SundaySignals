"""
backfill_names.py — One-shot script to populate player_name and position
in the player_seasons table from import_weekly_data().

import_seasonal_data() does not return player_name or reliable position values.
This script fetches them from weekly data (which does), deduplicates to one row
per player_id, and bulk-updates player_seasons via Supabase.

Usage:
    PYTHONPATH=. python pipelines/backfill_names.py
    PYTHONPATH=. python pipelines/backfill_names.py --seasons 2020 2021 2022 2023 2024
"""

from __future__ import annotations

import argparse
import os
import sys

import nfl_data_py as nfl
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

from pipelines.constants import POSITIONS

load_dotenv()

SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
TABLE                = "player_seasons"
BATCH_SIZE           = 200


def main() -> None:
    args = _parse_args()
    seasons = args.seasons or list(range(2020, 2025))

    print(f"Fetching weekly data for seasons {seasons} to extract names + positions...")
    weekly = nfl.import_weekly_data(seasons)

    # Keep only skill positions and pick the most informative name column
    pos_col  = next((c for c in ("position", "player_position") if c in weekly.columns), None)
    name_col = next((c for c in ("player_display_name", "player_name") if c in weekly.columns), None)

    if pos_col is None or name_col is None:
        sys.exit(f"Could not find position ({pos_col}) or name ({name_col}) in weekly data columns: {list(weekly.columns)}")

    if pos_col != "position":
        weekly = weekly.rename(columns={pos_col: "position"})
    if name_col != "player_name":
        weekly = weekly.rename(columns={name_col: "player_name"})

    weekly = weekly[weekly["position"].isin(POSITIONS)].copy()
    print(f"  {len(weekly)} weekly rows for skill positions across {weekly['season'].nunique()} seasons")

    # One row per player — use the latest season + latest week so we pick up
    # any mid-career position changes and the most current name spelling.
    meta = (
        weekly[["player_id", "player_name", "position", "season", "week"]]
        .dropna(subset=["player_id", "player_name", "position"])
        .sort_values(["season", "week"])
        .groupby("player_id", as_index=False)
        .last()[["player_id", "player_name", "position"]]
    )
    print(f"  {len(meta)} unique players found")

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # Fetch all (player_id, season) pairs from player_seasons so we can build
    # a full upsert payload (upsert requires the complete PK).
    # Supabase Python client defaults to 1000 rows — paginate to get all rows.
    existing_raw: list[dict] = []
    page_size = 1000
    offset = 0
    while True:
        page = (
            client.table(TABLE)
            .select("player_id, season")
            .range(offset, offset + page_size - 1)
            .execute()
            .data
        )
        existing_raw.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    existing = pd.DataFrame(existing_raw)  # columns: player_id, season
    print(f"  {len(existing)} existing rows in {TABLE}")

    # Join name/position onto every existing (player_id, season) row
    merged = existing.merge(meta, on="player_id", how="inner")
    print(f"  {len(merged)} rows will be updated with player_name + position")

    # Build upsert records — only send the PK + the two columns we're filling
    records = [
        {
            "player_id":   str(row["player_id"]),
            "season":      int(row["season"]),
            "player_name": str(row["player_name"]),
            "position":    str(row["position"]),
        }
        for _, row in merged[["player_id", "season", "player_name", "position"]].iterrows()
    ]

    updated = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        client.table(TABLE).upsert(batch).execute()
        updated += len(batch)
        print(f"  {updated}/{len(records)} rows upserted...")

    print(f"Done — {updated} player_seasons rows backfilled with names + positions.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill player_name and position in player_seasons.")
    parser.add_argument(
        "--seasons",
        nargs="+",
        type=int,
        default=None,
        help="Seasons to pull weekly data from (default: 2020–2024).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
