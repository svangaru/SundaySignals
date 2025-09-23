# ml/stages/backfill_history.py
import os
import io
from typing import List, Dict
import pandas as pd
from supabase import create_client
import nfl_data_py as nfl

# --- Config / env ---
SB_URL = os.environ["SUPABASE_URL"]
SB_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
RAW_BUCKET = "raw"

# Team code normalization map
TEAM_ALIAS = {
    "SD": "LAC",   # Chargers
    "OAK": "LV",   # Raiders
    "STL": "LA",   # Rams
}


# -----------------------------
# Helpers
# -----------------------------
def _chunk(rows, n=1000):
    for i in range(0, len(rows), n):
        yield rows[i:i + n]


def upsert_table(sb, table: str, rows: List[Dict], chunk_size=1000):
    if not rows:
        return
    for batch in _chunk(rows, chunk_size):
        sb.table(table).upsert(batch).execute()


def _upload_parquet(sb, bucket: str, path: str, df: pd.DataFrame):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    sb.storage.from_(bucket).upload(
        path,
        buf.read(),
        {"content-type": "application/octet-stream", "upsert": "true"},
    )


def _normalize_team(team: str) -> str:
    if not team or team.strip() == "":
        return ""
    t = team.strip()
    return TEAM_ALIAS.get(t, t)


# -----------------------------
# Dimension builders
# -----------------------------
def _build_players_from_weekly(weekly: pd.DataFrame) -> pd.DataFrame:
    players = (
        weekly.loc[:, ["player_id", "player_name", "recent_team", "position"]]
        .dropna(subset=["player_id", "player_name"])
        .copy()
    )

    players["player_id"] = players["player_id"].astype(str).str.strip()
    players["player_name"] = players["player_name"].astype(str).str.strip()
    players["recent_team"] = players["recent_team"].fillna("").astype(str).map(_normalize_team)
    players["position"] = players["position"].fillna("").astype(str).str.strip()

    players = players.rename(
        columns={"player_name": "name", "recent_team": "team"}
    )[["player_id", "position", "team", "name"]]

    players = players.drop_duplicates(subset=["player_id"])
    return players


def _build_schedule_from_schedules(schedules: pd.DataFrame) -> pd.DataFrame:
    required = {"season", "week", "home_team", "away_team"}
    missing = required - set(schedules.columns)
    if missing:
        raise RuntimeError(f"schedules is missing columns: {missing}")

    sch = schedules.copy()
    sch["season"] = sch["season"].astype(int)
    sch["week"] = sch["week"].astype(int)
    sch["home_team"] = sch["home_team"].astype(str).map(_normalize_team)
    sch["away_team"] = sch["away_team"].astype(str).map(_normalize_team)

    # Home rows
    home_rows = sch.rename(columns={"home_team": "team", "away_team": "opp"})[
        ["season", "week", "team", "opp"]
    ].copy()
    home_rows["home"] = True

    # Away rows
    away_rows = sch.rename(columns={"away_team": "team", "home_team": "opp"})[
        ["season", "week", "team", "opp"]
    ].copy()
    away_rows["home"] = False

    out = pd.concat([home_rows, away_rows], ignore_index=True)
    out = out.drop_duplicates(subset=["season", "week", "team"])
    return out


# -----------------------------
# Entry point
# -----------------------------
def run(start_season: int = 2015, end_season: int = 2024):
    sb = create_client(SB_URL, SB_SERVICE_KEY)
    seasons = list(range(start_season, end_season + 1))

    for season in seasons:
        weekly = nfl.import_weekly_data([season])
        schedules = nfl.import_schedules([season])

        # Save raw parquet
        _upload_parquet(sb, RAW_BUCKET, f"nfl/{season}/weekly.parquet", weekly)
        _upload_parquet(sb, RAW_BUCKET, f"nfl/{season}/schedule.parquet", schedules)

        # Build & upsert dims
        players = _build_players_from_weekly(weekly)
        upsert_table(sb, "players", players.to_dict("records"))

        schedule = _build_schedule_from_schedules(schedules)
        upsert_table(sb, "schedule", schedule.to_dict("records"))

        print(
            f"[backfill] season={season}: weekly={len(weekly)}, "
            f"players={len(players)}, schedule={len(schedule)}"
        )

    return {"ok": True, "start": start_season, "end": end_season}
