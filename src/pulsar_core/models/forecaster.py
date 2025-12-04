from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from ..store import TimeSeriesStore


@dataclass
class ForecastResult:
    hexagon: int
    service_type: int
    horizon_min: int
    demand: float
    driver_gap: float
    surge_delta_percent: float
    confidence: float

    def as_dict(self) -> Dict:
        return {
            "hexagon": self.hexagon,
            "service_type": self.service_type,
            "horizon_min": self.horizon_min,
            "demand": self.demand,
            "driver_gap": self.driver_gap,
            "surge_delta_percent": self.surge_delta_percent,
            "confidence": self.confidence,
        }


class SimpleForecaster:
    def __init__(self, store: TimeSeriesStore):
        self.store = store

    def forecast(
        self, hexagon: int, service_type: int, horizons: List[int]
    ) -> List[ForecastResult]:
        history = self.store.history(hexagon, service_type)
        if history.empty or len(history) < 4:
            return []

        demand_series = history["demand_signal"].astype(float)
        drivers = history["supply_signal"].astype(float)

        model = LinearRegression()
        x = np.arange(len(demand_series)).reshape(-1, 1)
        model.fit(x, demand_series.values)

        latest_index = len(demand_series) - 1
        latest_demand = demand_series.iloc[-1]
        latest_drivers = drivers.iloc[-1]

        results: List[ForecastResult] = []
        for horizon in horizons:
            step = horizon // 5 if horizon >= 5 else 1
            future_index = latest_index + step
            pred = model.predict([[future_index]])[0]
            driver_gap = pred - latest_drivers
            surge_delta = self._surge_delta(pred, latest_demand)
            confidence = float(
                max(0.2, 1 - demand_series.std() / (demand_series.mean() + 1e-6))
            )
            results.append(
                ForecastResult(
                    hexagon=hexagon,
                    service_type=service_type,
                    horizon_min=horizon,
                    demand=round(float(pred), 2),
                    driver_gap=round(float(driver_gap), 2),
                    surge_delta_percent=round(surge_delta, 2),
                    confidence=round(confidence, 2),
                )
            )
        return results

    def _surge_delta(self, predicted: float, current: float) -> float:
        if current <= 0:
            current = 1.0
        delta = (predicted - current) / current * 100
        return float(np.clip(delta, -20, 80))
