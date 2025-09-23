# ml/stages/backfill_history.py
import os
import io
from typing import List, Dict
import pandas as pd
from supabase import create_client
import nfl_data_py as nfl  # correct import for nflverse datasets

# --- Config / env ---
SB_URL = os.environ["SUPABASE_URL"]
SB_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
RAW_BUCKET = "raw"


# -----------------------------
# Helpers
# -----------------------------
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
    """Upload a DataFrame as Parquet to Supabase Storage (idempotent)."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    sb.storage.from_(bucket).upload(
        path, buf.read(),
        {"content-type": "application/octet-stream", "upsert": "true"}
    )


# -----------------------------
# Dimension builders
# -----------------------------
def _build_players_from_weekly(weekly: pd.DataFrame) -> pd.DataFrame:
    """
    Build the players dimension from weekly stats.
    Keeps columns: player_id (nflverse), position, team, name
    Ensures NOT NULL for name, dedupes by player_id (PK).
    """
    players = (
        weekly.loc[:, ["player_id", "player_name", "recent_team", "position"]]
        .dropna(subset=["player_id", "player_name"])  # enforce NOT NULL on name/id
        .copy()
    )

    # normalize
    players["player_id"] = players["player_id"].astype(str).str.strip()
    players["player_name"] = players["player_name"].astype(str).str.strip()
    players["recent_team"] = players["recent_team"].fillna("").astype(str).str.strip()
    players["position"] = players["position"].fillna("").astype(str).str.strip()

    # rename to match your table cols: (player_id, position, team, name)
    players = players.rename(
        columns={"player_name": "name", "recent_team": "team"}
    )[["player_id", "position", "team", "name"]]

    # dedupe on PK
    players = players.drop_duplicates(subset=["player_id"])
    return players


def _build_schedule_from_schedules(schedules: pd.DataFrame) -> pd.DataFrame:
    """
    Build a team-week schedule table from nflverse schedules.
    Output cols: season, week, team, opp, home  (PK: season, week, team)
    We expand each game into two rows: one for home team (home=True) and one for away team (home=False).
    """
    required = {"season", "week", "home_team", "away_team"}
    missing = required - set(schedules.columns)
    if missing:
        raise RuntimeError(f"schedules is missing columns: {missing}")

    sch = schedules.copy()
    sch["season"] = sch["season"].astype(int)
    sch["week"] = sch["week"].astype(int)
    sch["home_team"] = sch["home_team"].astype(str).str.strip()
    sch["away_team"] = sch["away_team"].astype(str).str.strip()

    # Build rows for home side
    home_rows = sch.rename(columns={"home_team": "team", "away_team": "opp"})[
        ["season", "week", "team", "opp"]
    ].copy()
    home_rows["home"] = True

    # Build rows for away side
    away_rows = sch.rename(columns={"away_team": "team", "home_team": "opp"})[
        ["season", "week", "team", "opp"]
    ].copy()
    away_rows["home"] = False

    out = pd.concat([home_rows, away_rows], ignore_index=True)

    # dedupe by PK (season, week, team) just in case
    out = out.drop_duplicates(subset=["season", "week", "team"])

    # basic hygiene
    out["team"] = out["team"].astype(str).str.strip()
    out["opp"] = out["opp"].astype(str).str.strip()

    return out


# -----------------------------
# Entry point
# -----------------------------
def run(start_season: int = 2015, end_season: int = 2024):
    """
    For each season:
      - download week-level player stats (weekly)
      - download official schedules
      - write both to raw storage: nfl/{season}/weekly.parquet, nfl/{season}/schedule.parquet
      - upsert players (from weekly)
      - upsert schedule (from schedules with true home/away & opp)
    """
    sb = create_client(SB_URL, SB_SERVICE_KEY)
    seasons = list(range(start_season, end_season + 1))

    for season in seasons:
        # --- Load data from nflverse / nfl_data_py ---
        weekly = nfl.import_weekly_data([season])         # player-week stats (many cols incl fantasy_points_ppr)
        schedules = nfl.import_schedules([season])        # game-level schedule (home_team / away_team)

        # --- Save raw parquet for lineage & later processing ---
        _upload_parquet(sb, RAW_BUCKET, f"nfl/{season}/weekly.parquet", weekly)
        _upload_parquet(sb, RAW_BUCKET, f"nfl/{season}/schedule.parquet", schedules)

        # --- Build & upsert players ---
        players = _build_players_from_weekly(weekly)
        upsert_table(sb, "players", players.to_dict("records"))

        # --- Build & upsert schedule (with real home flags) ---
        schedule = _build_schedule_from_schedules(schedules)
        upsert_table(sb, "schedule", schedule.to_dict("records"))

        print(
            f"[backfill] season={season}: "
            f"weekly_rows={len(weekly)}, players={len(players)}, schedule_rows={len(schedule)}"
        )

    return {"ok": True, "start": start_season, "end": end_season}
