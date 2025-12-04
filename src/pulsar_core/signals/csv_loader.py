from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from ..periods import PeriodWindow, with_duration


@dataclass
class CSVSignalLoader:
    """Signal loader that extracts metrics from CSV files instead of Redis."""

    csv_data: pd.DataFrame

    def __init__(self, csv_data: pd.DataFrame):
        self.csv_data = csv_data.copy()
        # Convert clickhouse_time to datetime with timezone awareness
        if "clickhouse_time" in self.csv_data.columns:
            self.csv_data["clickhouse_time"] = pd.to_datetime(
                self.csv_data["clickhouse_time"], utc=True
            ).dt.tz_localize(None)  # Remove timezone for comparison

    def acceptance_metrics(
        self, period: PeriodWindow, hexagon: int, service_type: int
    ) -> Dict[str, int]:
        """Extract acceptance metrics from CSV data for a given period and hexagon."""
        period_data = self._filter_period(period, hexagon, service_type)

        if period_data.empty:
            return {"requests": 0, "accepts": 0}

        # Use surge_percent as a proxy for demand/activity
        # Higher surge means more requests
        surge_values = period_data["surge_percent"].abs()
        requests = int(
            surge_values.sum() * 10 + len(period_data) * 5
        )  # Scale based on surge
        accepts = int(requests * 0.7)  # Assume 70% acceptance rate

        return {"requests": max(requests, 0), "accepts": max(accepts, 0)}

    def price_metrics(
        self,
        period: PeriodWindow,
        hexagon: int,
        service_type: int,
        service_category: int,
    ) -> Dict[str, int]:
        """Extract price conversion metrics from CSV data."""
        period_data = self._filter_period(period, hexagon, service_type)

        if period_data.empty:
            return {"ride_requests": 0, "get_prices": 0}

        # Use surge data to estimate price requests
        surge_values = period_data["surge_percent"].abs()
        get_prices = int(surge_values.sum() * 15 + len(period_data) * 8)
        ride_requests = int(get_prices * 0.6)  # Assume 60% conversion

        return {
            "ride_requests": max(ride_requests, 0),
            "get_prices": max(get_prices, 0),
        }

    def surge_metrics(
        self, period: PeriodWindow, hexagon: int, service_type: int
    ) -> Dict[str, float]:
        """Return actual surge metrics captured in the CSV snapshot."""
        period_data = self._filter_period(period, hexagon, service_type)
        if period_data.empty:
            return {
                "rule_sheet_id": None,
                "surge_percent": 0.0,
                "surge_absolute": 0.0,
                "cumulative_surge_percent": 0.0,
                "cumulative_surge_absolute": 0.0,
            }

        # Use the last record within the period (closest to period end)
        latest_row = period_data.sort_values("clickhouse_time").iloc[-1]
        return {
            "rule_sheet_id": int(latest_row.get("rule_sheet_id", 0)) or None,
            "surge_percent": float(latest_row.get("surge_percent", 0.0)),
            "surge_absolute": float(latest_row.get("surge_absolute", 0.0)),
            "cumulative_surge_percent": float(
                latest_row.get("cumulative_surge_percent", 0.0)
            ),
            "cumulative_surge_absolute": float(
                latest_row.get("cumulative_surge_absolute", 0.0)
            ),
        }

    def _filter_period(
        self, period: PeriodWindow, hexagon: int, service_type: int
    ) -> pd.DataFrame:
        """Filter CSV data for a specific period, hexagon, and service type."""
        # Ensure clickhouse_time is datetime64[ns]
        if self.csv_data["clickhouse_time"].dtype != "datetime64[ns]":
            self.csv_data["clickhouse_time"] = pd.to_datetime(
                self.csv_data["clickhouse_time"]
            )

        # Convert period datetimes to pandas Timestamp (naive) for proper comparison
        period_start = pd.Timestamp(period.start.replace(tzinfo=None))
        period_end = pd.Timestamp(period.end.replace(tzinfo=None))

        mask = (
            (self.csv_data["hex_id"] == hexagon)
            & (self.csv_data["service_type"] == service_type)
            & (self.csv_data["clickhouse_time"] >= period_start)
            & (self.csv_data["clickhouse_time"] < period_end)
        )
        return self.csv_data[mask].copy()


def load_csv_files(data_dir: Path) -> pd.DataFrame:
    """Load all CSV files from the data directory."""
    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        raise ValueError(f"No CSV files found in {data_dir}")

    all_data = []
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        all_data.append(df)

    combined = pd.concat(all_data, ignore_index=True)
    combined["clickhouse_time"] = pd.to_datetime(combined["clickhouse_time"])
    return combined


def create_import_tasks_from_csv(
    csv_data: pd.DataFrame,
    period_duration_minutes: float,
    collect_duration_minutes: float,
) -> List[Dict]:
    """Convert CSV data to ImportTask payloads grouped by time periods."""
    # Group by time periods
    csv_data = csv_data.sort_values("clickhouse_time")
    csv_data["period_start"] = csv_data["clickhouse_time"].apply(
        lambda ts: with_duration(
            ts, period_duration_minutes, collect_duration_minutes
        ).start
    )

    tasks = []
    for (period_start, service_type, city_id), group in csv_data.groupby(
        ["period_start", "service_type", "city_id"]
    ):
        for hex_id in group["hex_id"].unique():
            period_end = period_start + pd.Timedelta(minutes=period_duration_minutes)
            collect_end = period_start + pd.Timedelta(minutes=collect_duration_minutes)

            task_payload = {
                "hexagon": int(hex_id),
                "service_type": int(service_type),
                "city_id": int(city_id),
                "period": {
                    "from": period_start.isoformat(),
                    "to": period_end.isoformat(),
                    "collect_duration": collect_end.isoformat(),
                },
                "expansion_weight": {"pc_weight": 1.0, "ar_weight": 1.0},
            }
            tasks.append(task_payload)

    return tasks
