# ml/stages/sync_players.py
"""
Sync Sleeper master players -> map to our `players` table by (name, position, team)
and set players.sleeper_id. Also snapshot the raw JSON to Supabase Storage at
raw/sleeper/players.json.

Run (via Modal):
    modal run -m ml.modal_app::sync_players

ENV required (via Modal secret `supabase` or process env):
    SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
Optional:
    SLEEPER_BASE (default https://api.sleeper.app)
"""
from __future__ import annotations
import os
import io
import json
import time
import hashlib
import requests
from typing import Dict, Any, List
from supabase import create_client

SB_URL = os.environ["SUPABASE_URL"]
SB_SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SLEEPER_BASE = os.getenv("SLEEPER_BASE", "https://api.sleeper.app")
RAW_BUCKET = "raw"

# --- Helpers -----------------------------------------------------------------

def _sb():
    return create_client(SB_URL, SB_SERVICE_KEY)


def _storage_upload_bytes(sb, bucket: str, path: str, data: bytes, content_type: str = "application/json"):
    # If path exists, remove it first to avoid version clutter for now
    try:
        sb.storage.from_(bucket).remove([path])
    except Exception:
        pass
    sb.storage.from_(bucket).upload(path, data, file_options={"content-type": content_type})


def _normalize_team(team: str | None) -> str | None:
    if not team:
        return None
    alias = {
        "OAK": "LV", "LV": "LV",
        "STL": "LA", "LA": "LA",
        "SD": "LAC", "LAC": "LAC",
        "WSH": "WAS", "WAS": "WAS",
    }
    return alias.get(team, team)


# --- Core --------------------------------------------------------------------

def _fetch_sleeper_players() -> Dict[str, Any]:
    url = f"{SLEEPER_BASE}/v1/players/nfl"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def _build_updates(raw_players: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    We try to match to our `players` table primarily on (name, position) and optionally team.
    We only write sleeper_id for existing rows; we are *not* creating new core players here
    because historical stats come from nflverse.
    """
    updates: List[Dict[str, Any]] = []
    # Build a simple index: (name.lower(), position) -> sleeper_id, team
    for sleeper_id, p in raw_players.items():
        name = p.get("full_name") or p.get("search_full_name") or p.get("first_name")
        pos = p.get("position")
        team = _normalize_team(p.get("team"))
        if not name or not pos:
            continue
        updates.append({
            "name": name.strip(),
            "position": pos.strip(),
            "team": team,
            "sleeper_id": sleeper_id,
        })
    return updates


def _upsert_sleeper_ids(sb, updates: List[Dict[str, Any]]) -> int:
    """
    For each candidate, attempt to set players.sleeper_id where (name, position) matches,
    and team matches when provided. We do this in chunks.
    """
    CHUNK = 1000
    total = 0
    for i in range(0, len(updates), CHUNK):
        batch = updates[i:i+CHUNK]
        # We'll fetch matching players by name+position in a single RPC-like loop
        # Simpler approach: for each batch row, try a targeted update.
        for row in batch:
            name = row["name"]
            pos = row["position"]
            team = row.get("team")
            sleeper_id = row["sleeper_id"]
            try:
                # Prefer matching by name+position+team if team exists; fallback to name+position
                if team:
                    resp = sb.table("players").update({"sleeper_id": sleeper_id}) \
                        .eq("name", name).eq("position", pos).eq("team", team).execute()
                    updated = len(resp.data or [])
                    if updated == 0:
                        resp2 = sb.table("players").update({"sleeper_id": sleeper_id}) \
                            .eq("name", name).eq("position", pos).is_("team", None).execute()
                        updated += len(resp2.data or [])
                else:
                    resp = sb.table("players").update({"sleeper_id": sleeper_id}) \
                        .eq("name", name).eq("position", pos).execute()
                    updated = len(resp.data or [])
                total += updated
            except Exception:
                # continue on best-effort basis
                continue
    return total


def run():
    sb = _sb()
    raw = _fetch_sleeper_players()

    # Snapshot raw JSON to storage
    raw_bytes = json.dumps(raw, separators=(",", ":")).encode("utf-8")
    _storage_upload_bytes(sb, RAW_BUCKET, "sleeper/players.json", raw_bytes)

    updates = _build_updates(raw)
    updated = _upsert_sleeper_ids(sb, updates)

    print(f"[sync_players] updated sleeper_id for ~{updated} players (best-effort)")
    return {"ok": True, "updated": updated, "raw_count": len(raw)}