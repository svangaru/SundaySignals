# ml/stages/sync_players.py
from __future__ import annotations
import os, io, json, requests
import pandas as pd
from typing import Any, Dict, List
from supabase import create_client

SB_URL = os.environ["SUPABASE_URL"]
SB_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SLEEPER_BASE = os.getenv("SLEEPER_BASE", "https://api.sleeper.app")
RAW_BUCKET = "raw"

def _sb():
    return create_client(SB_URL, SB_SERVICE_KEY)

def _storage_put_json(sb, path: str, obj: Any):
    data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    try:
        sb.storage.from_(RAW_BUCKET).remove([path])
    except Exception:
        pass
    sb.storage.from_(RAW_BUCKET).upload(path, data, file_options={"content-type": "application/json"})

def _normalize_team(team: str | None) -> str | None:
    if not team: return None
    return {"OAK":"LV","LV":"LV","STL":"LA","LA":"LA","SD":"LAC","LAC":"LAC","WSH":"WAS","WAS":"WAS"}.get(team, team)

def run():
    sb = _sb()

    # 1) Pull Sleeper master once
    r = requests.get(f"{SLEEPER_BASE}/v1/players/nfl", timeout=60)
    r.raise_for_status()
    sleeper = r.json()
    _storage_put_json(sb, "sleeper/players.json", sleeper)

    # 2) Build compact dataframe of Sleeper players we care about
    rows = []
    for sid, p in sleeper.items():
        pos = p.get("position")
        name = p.get("full_name") or p.get("search_full_name") or p.get("first_name")
        if not name or not pos:  # skip defense, nulls, etc.
            continue
        team = _normalize_team(p.get("team"))
        rows.append({"sleeper_id": sid, "name": name.strip(), "position": pos.strip(), "team": team})
    df_sl = pd.DataFrame(rows)
    if df_sl.empty:
        print("[sync_players] Sleeper returned no usable players")
        return {"ok": False, "reason": "empty_sleeper"}

    # 3) Pull *our* players once (id + keys)
    res = sb.table("players").select("player_id,name,position,team").execute()
    df_me = pd.DataFrame(res.data or [])
    if df_me.empty:
        print("[sync_players] Your players table is empty — run backfill_history first.")
        return {"ok": False, "reason": "empty_players"}

    # 4) Match strategy: prefer (name, position, team) exact; fallback to (name, position)
    # Join on (name,position,team)
    m1 = df_me.merge(df_sl, on=["name","position","team"], how="inner", suffixes=("","_sl"))
    matched_ids = set(m1[["name","position"]].itertuples(index=False, name=None))

    # For remaining, match on (name,position) only
    left = df_me.merge(df_sl.drop(columns=["team"]), on=["name","position"], how="inner", suffixes=("","_sl"))
    # Drop ones already matched above
    left = left[~left[["name","position"]].apply(tuple, axis=1).isin(matched_ids)]

    matches = pd.concat([m1, left], ignore_index=True)

    # 5) Build upsert payload: player_id + sleeper_id
    updates = matches[["player_id","sleeper_id"]].dropna().drop_duplicates().to_dict(orient="records")
    if not updates:
        print("[sync_players] Nothing to update (all mapped already?)")
        return {"ok": True, "updated": 0, "seen": len(df_sl)}

    # 6) Batch upsert by primary key (player_id)
    CHUNK = 1000
    total = 0
    for i in range(0, len(updates), CHUNK):
        batch = updates[i:i+CHUNK]
        sb.table("players").upsert(batch, on_conflict="player_id").execute()
        total += len(batch)

    print(f"[sync_players] updated sleeper_id for {total} players (from {len(df_sl)} Sleeper rows)")
    return {"ok": True, "updated": total, "seen": len(df_sl)}
