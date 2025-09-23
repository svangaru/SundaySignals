# ml/stages/sync_league_index.py
"""
Sync a Sleeper league's season index: league meta, users, and rosters.
Snapshots are written to Supabase Storage under:
  raw/sleeper/leagues/{league_id}/season={YYYY}/(league|users|rosters).json

Upserts into tables:
  leagues(platform, league_id, season, ...)
  league_users(platform, league_id, user_id, ...)
  league_rosters(platform, league_id, season, roster_id, ...)

Run:
    modal run -m ml.modal_app::sync_league_index -- --league_id <LEAGUE> --season 2025
"""
from __future__ import annotations
import os
import io
import json
import requests
from typing import Any, Dict, List
from supabase import create_client

SB_URL = os.environ["SUPABASE_URL"]
SB_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SLEEPER_BASE = os.getenv("SLEEPER_BASE", "https://api.sleeper.app")
RAW_BUCKET = "raw"


def _sb():
    return create_client(SB_URL, SB_SERVICE_KEY)


def _storage_upload_json(sb, path: str, obj: Any):
    data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    try:
        sb.storage.from_(RAW_BUCKET).remove([path])
    except Exception:
        pass
    sb.storage.from_(RAW_BUCKET).upload(path, data, file_options={"content-type": "application/json"})


def _get(url: str):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def _upsert_league(sb, league_id: str, season: int, league_obj: Dict[str, Any]):
    row = {
        "platform": "sleeper",
        "league_id": league_id,
        "season": season,
        "name": league_obj.get("name"),
        "scoring_settings": league_obj.get("scoring_settings"),
        "roster_settings": league_obj.get("roster_settings"),
        "metadata": league_obj,
    }
    sb.table("leagues").upsert(row, on_conflict="platform,league_id,season").execute()


def _upsert_users(sb, league_id: str, users: List[Dict[str, Any]]):
    rows = []
    for u in users:
        rows.append({
            "platform": "sleeper",
            "league_id": league_id,
            "user_id": u.get("user_id"),
            "display_name": u.get("display_name"),
            "metadata": u,
        })
    if rows:
        sb.table("league_users").upsert(rows, on_conflict="platform,league_id,user_id").execute()


def _upsert_rosters(sb, league_id: str, season: int, rosters: List[Dict[str, Any]]):
    rows = []
    for r in rosters:
        rows.append({
            "platform": "sleeper",
            "league_id": league_id,
            "season": season,
            "roster_id": r.get("roster_id"),
            "owner_id": r.get("owner_id"),
            "starters": r.get("starters") or [],
            "players": r.get("players") or [],
            "taxi": r.get("taxi") or [],
            "reserve": r.get("reserve") or [],
            "settings": r.get("settings") or {},
        })
    if rows:
        sb.table("league_rosters").upsert(rows, on_conflict="platform,league_id,season,roster_id").execute()


def run(league_id: str, season: int):
    sb = _sb()
    league = _get(f"{SLEEPER_BASE}/v1/league/{league_id}")
    users = _get(f"{SLEEPER_BASE}/v1/league/{league_id}/users")
    rosters = _get(f"{SLEEPER_BASE}/v1/league/{league_id}/rosters")

    # Snapshots
    base = f"sleeper/leagues/{league_id}/season={season}"
    _storage_upload_json(sb, f"{base}/league.json", league)
    _storage_upload_json(sb, f"{base}/users.json", users)
    _storage_upload_json(sb, f"{base}/rosters.json", rosters)

    # Upserts
    _upsert_league(sb, league_id, season, league)
    _upsert_users(sb, league_id, users)
    _upsert_rosters(sb, league_id, season, rosters)

    print(f"[sync_league_index] league={league_id} season={season} users={len(users)} rosters={len(rosters)}")
    return {"ok": True, "league_id": league_id, "season": season, "users": len(users), "rosters": len(rosters)}