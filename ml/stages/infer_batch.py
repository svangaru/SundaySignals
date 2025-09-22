def run(season: int, week: int):
    print(f"[infer_batch] Inference for season={season}, week={week}")
    # TODO: load best model, score features, upsert predictions
    return {"ok": True, "season": season, "week": week}
