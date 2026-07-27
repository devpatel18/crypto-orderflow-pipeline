"""Baselines the model must beat, per SPEC.md.

Persistence: RV_hat(t+5m) = RV(t-5m..t). Volatility clusters, so this
is strong. HAR-RV: OLS on lagged RV at multiple horizons (intraday
analog of Corsi 2009 daily/weekly/monthly components).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .data import LABEL

HAR_COLS = ["rv_1m", "rv_5m", "rv_15m", "rv_60m"]


def persistence(df: pd.DataFrame) -> np.ndarray:
    return df["rv_5m"].to_numpy()


class HarRV:
    def fit(self, df: pd.DataFrame) -> HarRV:
        x = self._design(df)
        y = df[LABEL].to_numpy()
        self.coef_, *_ = np.linalg.lstsq(x, y, rcond=None)
        return self

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        return self._design(df) @ self.coef_

    @staticmethod
    def _design(df: pd.DataFrame) -> np.ndarray:
        return np.column_stack([df[c].to_numpy() for c in HAR_COLS] + [np.ones(len(df))])


def mae(y: np.ndarray, pred: np.ndarray) -> float:
    return float(np.mean(np.abs(y - pred)))


def rmse(y: np.ndarray, pred: np.ndarray) -> float:
    return float(np.sqrt(np.mean((y - pred) ** 2)))
