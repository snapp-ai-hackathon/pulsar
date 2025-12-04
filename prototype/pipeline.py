"""Prototype demand forecasting pipeline for Pulsar.

This script demonstrates how Pulsar can ingest Snapp trip signals, aggregate them
per H3 hexagon + service type, and emit short-term (30/60/90 min) demand and
driver-gap forecasts. Replace the sample CSV source with real collectors (Redis,
Kafka, ClickHouse) in production.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd
import h3

ROOT = Path(__file__).resolve().parent
DEFAULT_SAMPLE_PATH = ROOT / "sample_data" / "trips.csv"
DEFAULT_CACHE_PATH = ROOT / "cache" / "forecast.json"


@dataclass
class HorizonConfig:
    minutes: int
    max_surge_percent: int = 60
    min_surge_percent: int = -10


@dataclass
class PipelineConfig:
    city: str = "Tehran"
    interval_minutes: int = 15
    h3_resolution: int = 8
    service_types: Iterable[int] = field(default_factory=lambda: (1, 2))
    horizons: Iterable[HorizonConfig] = field(
        default_factory=lambda: (
            HorizonConfig(30),
            HorizonConfig(60),
            HorizonConfig(90),
        )
    )
    top_k: int = 6
    sample_path: Path = DEFAULT_SAMPLE_PATH
    cache_path: Path = DEFAULT_CACHE_PATH

    @classmethod
    def from_file(cls, path: Optional[Path]) -> "PipelineConfig":
        if path is None:
            return cls()
        data = json.loads(Path(path).read_text())
        horizons = [
            HorizonConfig(**item)
            for item in data.get(
                "horizons", [{"minutes": 30}, {"minutes": 60}, {"minutes": 90}]
            )
        ]
        return cls(
            city=data.get("city", cls.city),
            interval_minutes=data.get("interval_minutes", cls.interval_minutes),
            h3_resolution=data.get("h3_resolution", cls.h3_resolution),
            service_types=data.get("service_types", cls.service_types),
            horizons=horizons,
            top_k=data.get("top_k", cls.top_k),
            sample_path=Path(data.get("sample_path", DEFAULT_SAMPLE_PATH)),
            cache_path=Path(data.get("cache_path", DEFAULT_CACHE_PATH)),
        )


class DemandPipeline:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.df: Optional[pd.DataFrame] = None
        self.aggregated: Optional[pd.DataFrame] = None

    def load(self) -> pd.DataFrame:
        df = pd.read_csv(self.config.sample_path, parse_dates=["timestamp"])
        df = df[df["city"] == self.config.city].copy()
        df["service_type"] = df["service_type"].fillna(1).astype(int)
        df["driver_id"] = df["driver_id"].fillna(0).astype(int)
        df["bucket"] = df["timestamp"].dt.floor(f"{self.config.interval_minutes}min")
        df["hex_id"] = df.apply(
            lambda row: h3.latlng_to_cell(
                row["lat"], row["lng"], self.config.h3_resolution
            ),
            axis=1,
        )
        df["is_completed"] = (df["event_type"] == "ride_completed") & (
            df["status"] == "done"
        )
        df["is_request"] = df["event_type"] == "ride_request"
        df["is_get_price"] = df["event_type"] == "get_price"
        df["is_cancelled"] = df["event_type"] == "ride_cancelled"
        self.df = df
        return df

    def aggregate(self) -> pd.DataFrame:
        if self.df is None:
            self.load()
        df = self.df
        grouped = (
            df.groupby(["city", "service_type", "hex_id", "bucket"])
            .agg(
                completed=("is_completed", "sum"),
                requests=("is_request", "sum"),
                get_prices=("is_get_price", "sum"),
                cancelled=("is_cancelled", "sum"),
                drivers=(
                    "driver_id",
                    lambda x: x.astype(int).replace(0, np.nan).nunique(),
                ),
                avg_fare=(
                    "fare",
                    lambda x: x[x > 0].mean() if len(x[x > 0]) else np.nan,
                ),
            )
            .reset_index()
        )
        grouped["drivers"] = grouped["drivers"].ffill().fillna(8)
        grouped["acceptance_rate"] = grouped["completed"] / grouped["requests"].clip(
            lower=1
        )
        grouped["price_conversion"] = grouped["requests"] / grouped["get_prices"].clip(
            lower=1
        )
        grouped["demand_signal"] = grouped["requests"] + 0.5 * grouped["get_prices"]
        grouped["supply_signal"] = grouped["drivers"].clip(lower=1)
        grouped["net_gap"] = grouped["demand_signal"] - grouped["supply_signal"]
        self.aggregated = grouped.sort_values("bucket")
        return self.aggregated

    def forecast(self) -> List[Dict[str, object]]:
        if self.aggregated is None:
            self.aggregate()
        agg = self.aggregated
        results: List[Dict[str, object]] = []
        interval = timedelta(minutes=self.config.interval_minutes)
        for (city, service_type, hex_id), chunk in agg.groupby(
            ["city", "service_type", "hex_id"]
        ):
            chunk = chunk.set_index("bucket").sort_index()
            demand_series = (
                chunk["demand_signal"].asfreq(interval, method="ffill").fillna(0)
            )
            supply_series = (
                chunk["supply_signal"].asfreq(interval, method="ffill").ffill()
            )
            if demand_series.empty:
                continue
            last_bucket = demand_series.index.max()
            last_demand = demand_series.iloc[-1]
            last_supply = supply_series.iloc[-1] if not supply_series.empty else 0
            for horizon in self.config.horizons:
                steps = max(horizon.minutes // self.config.interval_minutes, 1)
                forecast_value = self._holts_linear(demand_series, steps)
                driver_gap = forecast_value - last_supply
                surge_delta = self._estimate_surge_percent(
                    last_demand,
                    forecast_value,
                    horizon.max_surge_percent,
                    horizon.min_surge_percent,
                )
                results.append(
                    {
                        "city": city,
                        "service_type": service_type,
                        "hex_id": hex_id,
                        "horizon_min": horizon.minutes,
                        "prediction_window_start": last_bucket.isoformat(),
                        "predicted_demand": round(forecast_value, 2),
                        "active_drivers": round(last_supply, 2),
                        "driver_gap": round(driver_gap, 2),
                        "surge_delta_percent": surge_delta,
                        "confidence": self._confidence(demand_series, steps),
                    }
                )
        return self._rank(results)

    def _holts_linear(self, series: pd.Series, steps: int) -> float:
        """Lightweight trend-aware forecast without external deps."""
        values = series.tail(12)
        level = values.iloc[-1]
        trend = (values.diff().tail(3).mean()) if len(values) > 1 else 0
        return float(max(level + trend * steps, 0))

    def _estimate_surge_percent(
        self, current: float, forecast: float, max_cap: int, min_cap: int
    ) -> float:
        delta = forecast - current
        if abs(current) < 1:
            current = 1
        percent_change = (delta / current) * 100
        return float(np.clip(percent_change, min_cap, max_cap))

    def _confidence(self, series: pd.Series, steps: int) -> float:
        if series.std() == 0:
            return 0.4
        volatility = series.std() / max(series.mean(), 1)
        confidence = max(0.2, 1 - volatility) * (1 - 0.05 * (steps - 1))
        return round(float(np.clip(confidence, 0.1, 0.95)), 2)

    def _rank(self, rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
        df = pd.DataFrame(rows)
        if df.empty:
            return []
        df["priority_score"] = (
            df["driver_gap"].clip(lower=0)
            + 0.2 * df["surge_delta_percent"].clip(lower=0)
            + 0.1 * df["confidence"]
        )
        ranked = (
            df.sort_values(["horizon_min", "priority_score"], ascending=[True, False])
            .groupby("horizon_min")
            .head(self.config.top_k)
        )
        return ranked.to_dict(orient="records")

    def save_cache(self, rows: List[Dict[str, object]]) -> None:
        self.config.cache_path.parent.mkdir(exist_ok=True, parents=True)
        self.config.cache_path.write_text(json.dumps(rows, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Pulsar prototype pipeline.")
    parser.add_argument("--config", type=Path, help="Optional JSON config path.")
    parser.add_argument(
        "--generate-cache",
        action="store_true",
        help="Persist forecasts to pulsar/prototype/cache/forecast.json",
    )
    parser.add_argument(
        "--print", action="store_true", help="Print ranked forecasts to stdout."
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = PipelineConfig.from_file(args.config)
    pipeline = DemandPipeline(config)
    rows = pipeline.forecast()
    if args.print or not args.generate_cache:
        print(json.dumps(rows, indent=2))
    if args.generate_cache:
        pipeline.save_cache(rows)
        print(f"[pulsar] wrote {len(rows)} forecasts to {config.cache_path}")


if __name__ == "__main__":
    main()
