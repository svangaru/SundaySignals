def run(season: int = 2025, week: int = 3):
    print(f"[build_features] Building features for season={season}, week={week}")
    # TODO: read players/schedule/odds/dvp from Supabase
    #       assemble features parquet to features/season=YYYY/week=W/
    return {"ok": True, "season": season, "week": week}
