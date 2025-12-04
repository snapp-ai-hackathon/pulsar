from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict

from ..periods import PeriodWindow, period_string


@dataclass(frozen=True)
class ExpansionWeight:
    pc_weight: float = 1.0
    ar_weight: float = 1.0

    @classmethod
    def from_dict(cls, data: Dict[str, float]) -> "ExpansionWeight":
        return cls(
            pc_weight=data.get("pc_weight", 1.0),
            ar_weight=data.get("ar_weight", 1.0),
        )


@dataclass(frozen=True)
class ImportTask:
    hexagon: int
    service_type: int
    city_id: int
    period: PeriodWindow
    expansion_weight: Optional[ExpansionWeight] = None

    @classmethod
    def from_payload(cls, payload: Dict) -> "ImportTask":
        period_data = payload["period"]
        start = datetime.fromisoformat(period_data["from"])
        end = datetime.fromisoformat(period_data["to"])
        period = PeriodWindow(
            start=start,
            end=end,
            duration=end - start,
            collect_duration=datetime.fromisoformat(
                period_data.get("collect_duration", period_data["from"])
            )
            - start
            if "collect_duration" in period_data
            else (end - start),
        )
        expansion_weight = ExpansionWeight.from_dict(
            payload.get("expansion_weight", {})
        )
        return cls(
            hexagon=int(payload["hexagon"]),
            service_type=int(payload["service_type"]),
            city_id=int(payload.get("city_id", 0)),
            period=period,
            expansion_weight=expansion_weight,
        )

    def period_key(self) -> str:
        return period_string(self.period.start, self.period.end)
