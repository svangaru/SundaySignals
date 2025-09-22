# ml/stages/ingest_once.py
import os
from typing import List, Dict

def _env():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in env.")
    return url, key

def _upsert_table(sb, table: str, rows: List[Dict]):
    if not rows:
        print(f"[upsert:{table}] nothing to upsert")
        return
    print(f"[upsert:{table}] inserting {len(rows)} rows…")
    # Small chunks to be safe
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        sb.table(table).upsert(chunk).execute()
    # Confirm count via a cheap select (don’t do this in prod for big tables)
    try:
        resp = sb.table(table).select("*", count="exact").limit(1).execute()
        total = getattr(resp, "count", None)
        print(f"[upsert:{table}] ok. approx total rows now: {total}")
    except Exception as e:
        print(f"[upsert:{table}] select count failed: {e}")

def run(season: int, week: int):
    from supabase import create_client

    url, key = _env()
    print("[ingest] connecting to:", url)
    sb = create_client(url, key)

    # --- Minimal sample rows to prove the path ---
    players = [
        {"player_id": "p123", "position": "RB", "team": "NYJ", "name": "Example RB"},
        {"player_id": "p456", "position": "WR", "team": "BUF", "name": "Example WR"},
    ]
    schedule = [
        {"season": season, "week": week, "team": "NYJ", "opp": "BUF", "home": True},
        {"season": season, "week": week, "team": "BUF", "opp": "NYJ", "home": False},
    ]
    dvp = [
        {"season": season, "week": week, "team": "BUF", "position": "RB", "dvp": 3.2},
        {"season": season, "week": week, "team": "NYJ", "position": "WR", "dvp": 6.7},
    ]
    odds = [
        {"season": season, "week": week, "game_id": f"nyj-buf-w{week}", "team": "NYJ", "opp": "BUF",
         "spread": -2.5, "moneyline": -135, "total": 42.5},
        {"season": season, "week": week, "game_id": f"nyj-buf-w{week}", "team": "BUF", "opp": "NYJ",
         "spread": 2.5, "moneyline": 115, "total": 42.5},
    ]

    # Upserts + per-table logging
    _upsert_table(sb, "players", players)
    _upsert_table(sb, "schedule", schedule)
    _upsert_table(sb, "defense_vs_pos", dvp)
    _upsert_table(sb, "odds", odds)

    # Final sanity readbacks
    try:
        s = sb.table("schedule").select("*").eq("season", season).eq("week", week).limit(5).execute()
        print(f"[check:schedule] rows for season={season}, week={week}: {len(s.data)}")
    except Exception as e:
        print("[check:schedule] failed:", e)

    return {"ok": True, "season": season, "week": week,
            "inserted": {"players": len(players), "schedule": len(schedule),
                         "defense_vs_pos": len(dvp), "odds": len(odds)}}
