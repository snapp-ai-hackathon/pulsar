#!/usr/bin/env python3
"""
Script to process CSV files (19-22 Nov) and generate forecasts.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent.parent
SYS_PATH = CURRENT_DIR / "src"
if SYS_PATH.exists() and str(SYS_PATH) not in sys.path:
    sys.path.insert(0, str(SYS_PATH))

from pulsar_core.config import load_config
from pulsar_core.features import SnapshotBuilder
from pulsar_core.models import SimpleForecaster
from pulsar_core.signals import (
    CSVSignalLoader,
    ImportTask,
    create_import_tasks_from_csv,
    load_csv_files,
)
from pulsar_core.store import TimeSeriesStore


def process_csv_files(
    data_dir: Path,
    config_path: Path,
    output_dir: Path | None = None,
) -> None:
    """Load CSV files, process them, and generate forecasts."""
    print(f"[pulsar] Loading CSV files from {data_dir}...")
    csv_data = load_csv_files(data_dir)
    print(f"[pulsar] Loaded {len(csv_data)} rows from CSV files")
    
    # Load config
    cfg = load_config(config_path)
    
    # Create CSV signal loader
    csv_loader = CSVSignalLoader(csv_data)
    
    # Create snapshot builder with CSV loader
    builder = SnapshotBuilder(cfg, csv_loader)
    
    # Create time series store
    cache_dir = cfg.ensure_cache_dir() / "timeseries"
    store = TimeSeriesStore(cache_dir)
    
    # Convert CSV to import tasks
    print("[pulsar] Converting CSV data to import tasks...")
    task_payloads = create_import_tasks_from_csv(
        csv_data,
        cfg.period_duration_minutes,
        cfg.collect_duration_minutes,
    )
    print(f"[pulsar] Created {len(task_payloads)} import tasks")
    
    # Process tasks and build snapshots
    print("[pulsar] Processing tasks and building snapshots...")
    processed = 0
    for payload in task_payloads:
        try:
            task = ImportTask.from_payload(payload)
            snapshot = builder.build(task)
            store.append(snapshot)
            processed += 1
            if processed % 1000 == 0:
                print(f"[pulsar] Processed {processed}/{len(task_payloads)} tasks...")
        except Exception as e:
            print(f"[pulsar] Warning: Failed to process task {payload.get('hexagon')}: {e}")
            continue
    
    print(f"[pulsar] Successfully processed {processed} tasks")
    
    # Generate forecasts for some sample hexagons
    print("\n[pulsar] Generating forecasts...")
    forecaster = SimpleForecaster(store)
    
    # Get unique hexagon/service_type combinations from the data
    unique_combos = csv_data[['hex_id', 'service_type']].drop_duplicates()
    sample_size = min(10, len(unique_combos))
    sample_combos = unique_combos.head(sample_size)
    
    all_forecasts = []
    for _, row in sample_combos.iterrows():
        hexagon = int(row['hex_id'])
        service_type = int(row['service_type'])
        
        forecasts = forecaster.forecast(
            hexagon,
            service_type,
            cfg.forecast.horizons,
        )
        
        if forecasts:
            for forecast in forecasts:
                all_forecasts.append(forecast.as_dict())
            print(
                f"[pulsar] Forecast for hexagon {hexagon}, service_type {service_type}: "
                f"{len(forecasts)} horizons"
            )
    
    # Save forecasts to file
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "forecasts.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_forecasts, f, indent=2, ensure_ascii=False)
        print(f"\n[pulsar] Saved {len(all_forecasts)} forecasts to {output_file}")
    else:
        print(f"\n[pulsar] Generated {len(all_forecasts)} forecasts")
        print("\nSample forecasts:")
        for forecast in all_forecasts[:5]:
            print(f"  {forecast}")


def main():
    parser = argparse.ArgumentParser(description="Process CSV files and generate forecasts")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=CURRENT_DIR / "data",
        help="Directory containing CSV files",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=CURRENT_DIR / "config.yaml",
        help="Path to config.yaml",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output directory for forecasts JSON (optional)",
    )
    
    args = parser.parse_args()
    
    try:
        process_csv_files(args.data_dir, args.config, args.output)
    except Exception as e:
        print(f"[pulsar] Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

