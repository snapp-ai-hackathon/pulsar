from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from pulsar_core.config import load_config
from pulsar_core.features import HexagonSnapshot
from pulsar_core.store import TimeSeriesStore

RANDOM = random.Random(42)

SAMPLE_HEXAGONS = [
    {"hexagon": 613280476251029503, "service_type": 1, "city_id": 1},
    {"hexagon": 613653189614370815, "service_type": 1, "city_id": 1},
    {"hexagon": 613732126578901111, "service_type": 2, "city_id": 1},
]


def generate_snapshots(
    store: TimeSeriesStore,
    duration_minutes: float,
    history_points: int,
) -> None:
    duration = timedelta(minutes=duration_minutes)
    start_time = datetime.now(timezone.utc) - history_points * duration

    for entry in SAMPLE_HEXAGONS:
        hexagon = entry["hexagon"]
        service_type = entry["service_type"]
        city_id = entry["city_id"]

        for idx in range(history_points):
            period_start = start_time + idx * duration
            period_end = period_start + duration

            acceptance_rate = ROUND(RANDOM.uniform(0.6, 0.95))
            price_conversion = ROUND(RANDOM.uniform(0.3, 0.7))
            demand = ROUND(RANDOM.uniform(15, 90))
            supply = ROUND(RANDOM.uniform(8, 60))

            snapshot = HexagonSnapshot(
                hexagon=hexagon,
                service_type=service_type,
                city_id=city_id,
                period_start=period_start,
                period_end=period_end,
                acceptance_rate=acceptance_rate,
                price_conversion=price_conversion,
                demand_signal=demand,
                supply_signal=supply,
            )
            store.append(snapshot)


def ROUND(value: float) -> float:
    return round(value, 3)


def build_tasks(duration_minutes: float, future_steps: int = 6) -> List[dict]:
    duration = timedelta(minutes=duration_minutes)
    start = datetime.now(timezone.utc)
    tasks: List[dict] = []
    for step in range(future_steps):
        period_from = start + step * duration
        period_to = period_from + duration
        for entry in SAMPLE_HEXAGONS:
            tasks.append(
                {
                    "hexagon": entry["hexagon"],
                    "service_type": entry["service_type"],
                    "city_id": entry["city_id"],
                    "period": {
                        "from": period_from.isoformat(),
                        "to": period_to.isoformat(),
                    },
                    "expansion_weight": {"pc_weight": 1.0, "ar_weight": 1.0},
                }
            )
    return tasks


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic dataset for Pulsar bridge."
    )
    parser.add_argument("--config", type=Path, default=Path("config.example.yaml"))
    parser.add_argument(
        "--tasks-output",
        type=Path,
        default=Path("datasets/generated_tasks.json"),
    )
    parser.add_argument(
        "--history",
        type=int,
        default=24,
        help="number of historical periods to synthesize per hexagon",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    store = TimeSeriesStore(cfg.ensure_cache_dir() / "timeseries")

    generate_snapshots(store, cfg.period_duration_minutes, args.history)
    tasks = build_tasks(cfg.period_duration_minutes)

    args.tasks_output.parent.mkdir(parents=True, exist_ok=True)
    args.tasks_output.write_text(json.dumps(tasks, indent=2))
    print(
        f"[pulsar] generated {args.history} periods per hexagon and wrote {len(tasks)} tasks to {args.tasks_output}"
    )


if __name__ == "__main__":
    main()
