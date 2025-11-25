from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

import numpy as np

from ..config import PulsarConfig
from ..periods import PeriodWindow, with_duration
from ..signals import ImportTask


@dataclass
class HexagonSnapshot:
    hexagon: int
    service_type: int
    city_id: int
    period_start: datetime
    period_end: datetime
    acceptance_rate: float
    price_conversion: float
    demand_signal: float
    supply_signal: float
    rule_sheet_id: Optional[int] = None
    surge_percent: Optional[float] = None
    surge_absolute: Optional[float] = None
    cumulative_surge_percent: Optional[float] = None
    cumulative_surge_absolute: Optional[float] = None

    def as_dict(self) -> Dict:
        return {
            "hexagon": self.hexagon,
            "service_type": self.service_type,
            "city_id": self.city_id,
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "acceptance_rate": self.acceptance_rate,
            "price_conversion": self.price_conversion,
            "demand_signal": self.demand_signal,
            "supply_signal": self.supply_signal,
            "rule_sheet_id": self.rule_sheet_id,
            "surge_percent": self.surge_percent,
            "surge_absolute": self.surge_absolute,
            "cumulative_surge_percent": self.cumulative_surge_percent,
            "cumulative_surge_absolute": self.cumulative_surge_absolute,
        }


class SnapshotBuilder:
    def __init__(self, cfg: PulsarConfig, loader):
        """Loader can be RedisSignalLoader or CSVSignalLoader - both have the same interface."""
        self.cfg = cfg
        self.loader = loader

    def build(self, task: ImportTask) -> HexagonSnapshot:
        period = task.period
        acceptance = self.loader.acceptance_metrics(period, task.hexagon, task.service_type)
        price = self.loader.price_metrics(period, task.hexagon, task.service_type, task.service_type)

        acceptance_rate = self._safe_ratio(acceptance["accepts"], acceptance["requests"])
        price_conversion = self._safe_ratio(price["ride_requests"], price["get_prices"])
        demand_signal = price["ride_requests"] + 0.5 * price["get_prices"]
        supply_signal = max(acceptance["accepts"], 1)
        surge_data = None
        if hasattr(self.loader, "surge_metrics"):
            surge_data = self.loader.surge_metrics(period, task.hexagon, task.service_type)

        return HexagonSnapshot(
            hexagon=task.hexagon,
            service_type=task.service_type,
            city_id=task.city_id,
            period_start=period.start,
            period_end=period.end,
            acceptance_rate=acceptance_rate,
            price_conversion=price_conversion,
            demand_signal=float(demand_signal),
            supply_signal=float(supply_signal),
            rule_sheet_id=surge_data.get("rule_sheet_id") if surge_data else None,
            surge_percent=surge_data.get("surge_percent") if surge_data else None,
            surge_absolute=surge_data.get("surge_absolute") if surge_data else None,
            cumulative_surge_percent=surge_data.get("cumulative_surge_percent") if surge_data else None,
            cumulative_surge_absolute=surge_data.get("cumulative_surge_absolute") if surge_data else None,
        )

    def _safe_ratio(self, numerator: int, denominator: int) -> float:
        if denominator <= 0:
            return 0.0
        return float(np.clip(numerator / denominator, 0, 5))

    def latest_period(self, now: datetime) -> PeriodWindow:
        return with_duration(
            now,
            self.cfg.period_duration_minutes,
            self.cfg.collect_duration_minutes,
        )

