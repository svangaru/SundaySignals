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

    print(f"Fetching weekly data for seasons {seasons}...")
    weekly = nfl.import_weekly_data(seasons)

    # Prefer player_display_name (full name e.g. "Brock Bowers") over
    # player_name (abbreviated e.g. "B.Bowers"). Drop the abbreviated column
    # first to avoid duplicate column names after renaming.
    if "player_display_name" in weekly.columns:
        if "player_name" in weekly.columns:
            weekly = weekly.drop(columns=["player_name"])
        weekly = weekly.rename(columns={"player_display_name": "player_name"})
    elif "player_name" not in weekly.columns:
        sys.exit("Neither player_display_name nor player_name found in weekly data.")

    # Normalise position column name
    if "position" not in weekly.columns and "player_position" in weekly.columns:
        weekly = weekly.rename(columns={"player_position": "position"})
    if "position" not in weekly.columns:
        sys.exit("No position column found in weekly data.")

    weekly = weekly[weekly["position"].isin(POSITIONS)].copy()
    print(f"  {len(weekly)} weekly rows for skill positions across {weekly['season'].nunique()} seasons")

    # One row per player_id — latest season + latest week captures current
    # name spelling and any mid-career position changes.
    meta = (
        weekly[["player_id", "player_name", "position", "season", "week"]]
        .dropna(subset=["player_id", "player_name", "position"])
        .sort_values(["season", "week"])
        .groupby("player_id", as_index=False)
        .last()[["player_id", "player_name", "position"]]
    )
    print(f"  {len(meta)} unique players from weekly data")

    # Supplement with nfl.import_ids() for any players not found in weekly data.
    # import_ids() has broad historical coverage and returns full names.
    try:
        ids_df = nfl.import_ids()
        name_col = next(
            (c for c in ("name", "display_name", "short_name") if c in ids_df.columns),
            None,
        )
        if "gsis_id" in ids_df.columns and name_col:
            id_meta = (
                ids_df[["gsis_id", name_col]]
                .dropna()
                .rename(columns={"gsis_id": "player_id", name_col: "player_name"})
                .drop_duplicates("player_id")
            )
            # Only use id_meta for players NOT already covered by weekly data
            new_ids = ~id_meta["player_id"].isin(meta["player_id"])
            meta = pd.concat([meta, id_meta[new_ids][["player_id", "player_name"]]], ignore_index=True)
            print(f"  {new_ids.sum()} additional players added from import_ids()")
    except Exception as e:
        print(f"  import_ids() unavailable ({e}), skipping supplement")

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # Fetch all (player_id, season) pairs with pagination (default limit = 1000).
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

    # Join name/position onto every (player_id, season) row.
    # meta may lack position for the id_meta rows — fill with None so the upsert
    # only overwrites what we actually have.
    if "position" not in meta.columns:
        meta["position"] = None
    merged = existing.merge(meta, on="player_id", how="inner")
    print(f"  {len(merged)} rows to update")

    # Build records explicitly from scalar values (avoids pandas dtype surprises).
    records: list[dict] = []
    for player_id, season, player_name, position in zip(
        merged["player_id"],
        merged["season"],
        merged["player_name"],
        merged["position"],
    ):
        rec: dict = {
            "player_id": str(player_id),
            "season":    int(season),
            "player_name": str(player_name) if player_name is not None else None,
        }
        if position is not None and str(position) != "nan":
            rec["position"] = str(position)
        records.append(rec)

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
        "--seasons", nargs="+", type=int, default=None,
        help="Seasons to pull weekly data from (default: 2020–2024).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
