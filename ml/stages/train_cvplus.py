def run(season: int, week: int, learner: str = "xgb"):
    print(f"[train_cvplus] Training model={learner} for season={season}, week={week}")
    # TODO: load features, train with CV+, log metrics
    return {"ok": True, "season": season, "week": week, "learner": learner}
