
# Skeleton for swappable learners and CV+
import numpy as np
from dataclasses import dataclass

class Estimator:
    def fit(self, X, y, X_val=None, y_val=None): ...
    def predict(self, X): ...

class XGBEstimator(Estimator):
    ...  # Wrap xgboost.XGBRegressor with baseline params

class LGBMEstimator(Estimator):
    ...  # Wrap lightgbm.LGBMRegressor with baseline params

@dataclass
class CVPlusResult:
    model: Estimator
    q_alpha: float
    metrics: dict

def train_with_cvplus(estimator: Estimator, X, y, K=5, alpha=0.2) -> CVPlusResult:
    # time-ordered K folds -> residuals -> q_alpha = quantile(|residuals|, 1-alpha)
    ...
