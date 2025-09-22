# ml/stages/build_features.py
import os
import pandas as pd

def _client():
    from supabase import create_client
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)

def run(season: int, week: int) -> pd.DataFrame:
    sb = _client()

    # pull dims
    players = sb.table("players").select("*").execute().data
    sched   = sb.table("schedule").select("*").eq("season", season).eq("week", week).execute().data
    dvp     = sb.table("defense_vs_pos").select("*").eq("season", season).eq("week", week).execute().data
    odds    = sb.table("odds").select("*").eq("season", season).eq("week", week).execute().data

    df_p = pd.DataFrame(players)
    df_s = pd.DataFrame(sched)
    df_d = pd.DataFrame(dvp)
    df_o = pd.DataFrame(odds)

    if df_p.empty or df_s.empty:
        print("[features] nothing to build"); return pd.DataFrame()

    # join schedule onto players by team
    fx = df_p.merge(df_s, on="team", how="inner")  # season/week come from schedule
    # add DVP by opponent+position
    fx = fx.merge(df_d.rename(columns={"team": "opp", "dvp": "opp_dvp"}),
                  on=["season","week","opp","position"], how="left")
    # add spread by team
    fx = fx.merge(df_o[["team","spread","season","week"]], on=["team","season","week"], how="left")

    # fill and simple encodings
    fx["home"] = fx["home"].astype(bool).astype(int)
    fx["opp_dvp"] = fx["opp_dvp"].fillna(fx["opp_dvp"].median() if not fx["opp_dvp"].empty else 0.0)
    fx["spread"]  = fx["spread"].astype(float).fillna(0.0)

    # tiny numeric encodings
    pos_map = {"QB":0,"RB":1,"WR":2,"TE":3,"K":4,"DEF":5}
    fx["pos_id"] = fx["position"].map(pos_map).fillna(9).astype(int)

    # minimalist features
    features = fx[[
        "player_id","season","week","team","opp","position",
        "home","pos_id","opp_dvp","spread"
    ]].copy()

    print(f"[features] built {len(features)} rows for {season} W{week}")
    return features
