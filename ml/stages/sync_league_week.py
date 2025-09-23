# ml/stages/sync_league_week.py
"""
Sync weekly Sleeper data for a league: matchups and transactions for (season, week).
Snapshots to Supabase Storage:
  raw/sleeper/leagues/{league_id}/season={YYYY}/week={W}/(matchups|transactions).json

Upserts into:
  matchups(platform, league_id, season, week, matchup_id, roster_id, ...)
  transactions(league_id, ts, type, payload, tx_id)

Run:
    modal run -m ml.modal_app::sync_league_week -- --league_id <LEAGUE> --season 2025 --week 3
"""
from __future__ import annotations
import os
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


def _upsert_matchups(sb, league_id: str, season: int, week: int, data: List[Dict[str, Any]]):
    rows = []
    for m in data:
        rows.append({
            "platform": "sleeper",
            "league_id": league_id,
            "season": season,
            "week": week,
            "matchup_id": m.get("matchup_id"),
            "roster_id": m.get("roster_id"),
            "points": m.get("points"),
            "starters": m.get("starters") or [],
            "players": m.get("players") or [],
        })
    if rows:
        sb.table("matchups").upsert(rows, on_conflict="platform,league_id,season,week,matchup_id,roster_id").execute()


def _upsert_transactions(sb, league_id: str, season: int, week: int, data: List[Dict[str, Any]]):
    rows = []
    for t in data:
        rows.append({
            "league_id": league_id,
            "ts": t.get("status_updated") or t.get("created"),  # Sleeper ms epoch; DB will accept timestamptz if cast, else store as epoch
            "type": t.get("type"),
            "payload": t,
            "tx_id": t.get("transaction_id"),
        })
    if rows:
        # We key on (league_id, ts) in your schema; if ts collides, you'll still have a unique row per tx_id in payload
        sb.table("transactions").upsert(rows, on_conflict="league_id,ts").execute()


def run(league_id: str, season: int, week: int):
    sb = _sb()
    matchups = _get(f"{SLEEPER_BASE}/v1/league/{league_id}/matchups/{week}")
    txs = _get(f"{SLEEPER_BASE}/v1/league/{league_id}/transactions/{week}")

    base = f"sleeper/leagues/{league_id}/season={season}/week={week}"
    _storage_upload_json(sb, f"{base}/matchups.json", matchups)
    _storage_upload_json(sb, f"{base}/transactions.json", txs)

    _upsert_matchups(sb, league_id, season, week, matchups)
    _upsert_transactions(sb, league_id, season, week, txs)

    print(f"[sync_league_week] league={league_id} season={season} week={week} matchups={len(matchups)} txs={len(txs)}")
    return {"ok": True, "league_id": league_id, "season": season, "week": week, "matchups": len(matchups), "transactions": len(txs)}