from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import List

import pandas as pd

from ..features import HexagonSnapshot


class TimeSeriesStore:
    """Lightweight parquet-backed store for hexagon level features."""

    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, hexagon: int, service_type: int) -> Path:
        return self.root / f"{hexagon}_{service_type}.parquet"

    def append(self, snapshot: HexagonSnapshot) -> None:
        path = self._path(snapshot.hexagon, snapshot.service_type)
        frame = pd.DataFrame([snapshot.as_dict()])
        frame["surge_percent"] = frame.get("surge_percent", 0.0).fillna(0.0)
        frame["surge_absolute"] = frame.get("surge_absolute", 0.0).fillna(0.0)

        existing = None
        if path.exists():
            existing = pd.read_parquet(path)

        prev_cum_percent = 0.0
        prev_cum_absolute = 0.0
        if existing is not None and not existing.empty:
            if "cumulative_surge_percent" in existing.columns:
                prev_cum_percent = float(existing["cumulative_surge_percent"].iloc[-1])
            if "cumulative_surge_absolute" in existing.columns:
                prev_cum_absolute = float(existing["cumulative_surge_absolute"].iloc[-1])

        if "cumulative_surge_percent" not in frame.columns or frame["cumulative_surge_percent"].isnull().all():
            frame["cumulative_surge_percent"] = prev_cum_percent + frame["surge_percent"]
        else:
            frame["cumulative_surge_percent"] = frame["cumulative_surge_percent"].fillna(
                prev_cum_percent + frame["surge_percent"]
            )

        if "cumulative_surge_absolute" not in frame.columns or frame["cumulative_surge_absolute"].isnull().all():
            frame["cumulative_surge_absolute"] = prev_cum_absolute + frame["surge_absolute"]
        else:
            frame["cumulative_surge_absolute"] = frame["cumulative_surge_absolute"].fillna(
                prev_cum_absolute + frame["surge_absolute"]
            )

        if existing is not None:
            frame = pd.concat([existing, frame]).drop_duplicates(subset=["period_start"])
        frame.sort_values("period_start", inplace=True)
        frame.to_parquet(path, index=False)

    def history(self, hexagon: int, service_type: int, limit: int = 96) -> pd.DataFrame:
        path = self._path(hexagon, service_type)
        if not path.exists():
            return pd.DataFrame()
        frame = pd.read_parquet(path).tail(limit)
        frame["period_start"] = pd.to_datetime(frame["period_start"])
        frame.set_index("period_start", inplace=True)
        return frame

    def latest(self, hexagon: int, service_type: int) -> List[HexagonSnapshot]:
        frame = self.history(hexagon, service_type, limit=1)
        if frame.empty:
            return []
        row = frame.reset_index().iloc[0].to_dict()
        return [HexagonSnapshot(
            hexagon=hexagon,
            service_type=service_type,
            city_id=row["city_id"],
            period_start=row["period_start"],
            period_end=pd.to_datetime(row["period_end"]),
            acceptance_rate=row["acceptance_rate"],
            price_conversion=row["price_conversion"],
            demand_signal=row["demand_signal"],
            supply_signal=row["supply_signal"],
            rule_sheet_id=row.get("rule_sheet_id"),
            surge_percent=row.get("surge_percent"),
            surge_absolute=row.get("surge_absolute"),
            cumulative_surge_percent=row.get("cumulative_surge_percent"),
            cumulative_surge_absolute=row.get("cumulative_surge_absolute"),
        )]

