def run(season: int, week: int):
    print(f"[validate_promote] Validating model for season={season}, week={week}")
    # TODO: compute metrics, update model_registry
    return {"ok": True, "season": season, "week": week}
