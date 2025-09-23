# ml/stages/backfill_history.py
import os, io
from typing import List, Dict
import pandas as pd
from supabase import create_client
import nfl_data_py as nfl  # correct import

SB_URL = os.environ["SUPABASE_URL"]
SB_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
RAW_BUCKET = "raw"


def _chunk(rows, n=1000):
    """Yield successive n-sized chunks from a list."""
    for i in range(0, len(rows), n):
        yield rows[i:i + n]


def upsert_table(sb, table: str, rows: List[Dict], chunk_size=1000):
    """Upsert rows to Supabase in safe batches."""
    if not rows:
        return
    for batch in _chunk(rows, chunk_size):
        sb.table(table).upsert(batch).execute()


def _upload_parquet(sb, bucket: str, path: str, df: pd.DataFrame):
    """Upload a DataFrame as Parquet to Supabase Storage."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    sb.storage.from_(bucket).upload(
        path, buf.read(),
        {"content-type": "application/octet-stream", "upsert": "true"}
    )


def _build_dims_from_weekly(weekly: pd.DataFrame):
    # --- Players dim ---
    players = (
        weekly.loc[:, ["player_id", "player_name", "recent_team", "position"]]
        .dropna(subset=["player_id", "player_name"])  # drop null IDs/names
        .copy()
    )
    players["player_id"] = players["player_id"].astype(str).str.strip()
    players["player_name"] = players["player_name"].astype(str).str.strip()
    players["recent_team"] = players["recent_team"].fillna("").astype(str).str.strip()
    players["position"] = players["position"].fillna("").astype(str).str.strip()

    players = players.rename(columns={
        "player_name": "name",
        "recent_team": "team"
    })[["player_id", "position", "team", "name"]]

    # Deduplicate by PK (player_id), keeping the first occurrence
    players = players.drop_duplicates(subset=["player_id"])

    # --- Schedule dim ---
    schedule = (
        weekly.loc[:, ["season", "week", "recent_team", "opponent_team"]]
        .dropna(subset=["season", "week", "recent_team", "opponent_team"])
        .copy()
    )
    schedule["season"] = schedule["season"].astype(int)
    schedule["week"] = schedule["week"].astype(int)
    schedule["recent_team"] = schedule["recent_team"].astype(str).str.strip()
    schedule["opponent_team"] = schedule["opponent_team"].astype(str).str.strip()

    schedule = schedule.rename(columns={
        "recent_team": "team",
        "opponent_team": "opp"
    })
    schedule["home"] = False

    # Deduplicate by PK (season, week, team)
    schedule = schedule.drop_duplicates(subset=["season", "week", "team"])

    return players, schedule


def run(start_season: int = 2015, end_season: int = 2024):
    sb = create_client(SB_URL, SB_SERVICE_KEY)
    seasons = list(range(start_season, end_season + 1))

    for season in seasons:
        # Download week-level player stats
        weekly = nfl.import_weekly_data([season])

        # Save to raw storage
        raw_path = f"nfl/{season}/weekly.parquet"
        _upload_parquet(sb, RAW_BUCKET, raw_path, weekly)

        # Build dimensions
        players, schedule = _build_dims_from_weekly(weekly)

        # Upsert to Postgres
        upsert_table(sb, "players", players.to_dict("records"))
        upsert_table(sb, "schedule", schedule.to_dict("records"))

        print(
            f"[backfill] season={season}: "
            f"weekly={len(weekly)}, players={len(players)}, schedule={len(schedule)}"
        )

    return {"ok": True, "start": start_season, "end": end_season}
