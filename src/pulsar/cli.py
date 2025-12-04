from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Iterable, Tuple

import redis
import uvicorn

import logging
from datetime import date

import redis

from pulsar.clickhouse_nats import build_parameter_query, export_clickhouse_table, stream_clickhouse_table
from pulsar_core.config import PulsarConfig, load_config

# Configure basic logging so progress information is visible on the CLI
logging.basicConfig(level=logging.INFO, format="[pulsar] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
from pulsar_core.features import SnapshotBuilder
from pulsar_core.models import SimpleForecaster
from pulsar_core.models.cnn_trainer import CNNTrainer, CNNTrainerConfig
from pulsar_core.models.trainer import MLTrainer
from pulsar_core.models.training_tracker import TrainingTracker
from pulsar_core.service import create_app
from pulsar_core.signals import (
    ClickHouseLoader,
    ClickHouseSignalLoader,
    ImportTask,
    RedisSignalLoader,
    run_consumer,
    run_nats_consumer,
)
from pulsar_core.store import TimeSeriesStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pulsar bridge runner")
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to config file (defaults to $PULSAR_CONFIG or ./config.yaml)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser("sync", help="Ingest scheduler tasks and build snapshots")
    sync_parser.add_argument("--task-file", type=Path, help="Optional JSON file with import tasks array")
    sync_parser.add_argument("--canary", action="store_true", help="Listen to canary queues")

    # Online training from NATS
    train_online_parser = subparsers.add_parser("train-online", help="Online training from NATS events using CSV data")
    train_online_parser.add_argument("--data-dir", type=Path, default=Path("data"), help="Directory containing CSV files")

    api_parser = subparsers.add_parser("api", help="Serve forecast API (optional)")
    api_parser.add_argument("--host", default="0.0.0.0")
    api_parser.add_argument("--port", type=int, default=8088)

    train_parser = subparsers.add_parser("train", help="Train ML model and log to MLflow")
    train_parser.add_argument("--service-types", type=int, nargs="*", default=[], help="Filter service types")
    train_parser.add_argument("--alpha", type=float, default=0.3)
    train_parser.add_argument("--l1-ratio", type=float, default=0.1)
    train_parser.add_argument(
        "--model-type",
        choices=["elasticnet", "cnn"],
        default="elasticnet",
        help="Select training backend",
    )
    train_parser.add_argument("--cnn-window", type=int, default=12, help="Sequence window size for CNN model")
    train_parser.add_argument("--cnn-epochs", type=int, default=25)
    train_parser.add_argument("--cnn-batch-size", type=int, default=128)
    train_parser.add_argument("--train-date", type=str, help="Date to mark as trained (YYYY-MM-DD), defaults to today")
    train_parser.add_argument("--force", action="store_true", help="Force retraining even if date is already trained")
    train_parser.add_argument("--show-tracker", action="store_true", help="Show training tracker status")

    ch_parser = subparsers.add_parser(
        "clickhouse-export",
        help="Fetch a ClickHouse table in batches and publish them to NATS",
    )
    ch_parser.add_argument(
        "--table",
        default="snapp_raw_log.kandoo_parameter_nats",
        help="ClickHouse table name to read",
    )
    ch_parser.add_argument("--batch-size", type=int, default=1000, help="Rows per published batch")
    ch_parser.add_argument("--limit", type=int, help="Optional maximum row count to send")
    ch_parser.add_argument("--subject", help="Override the configured NATS subject")
    ch_parser.add_argument(
        "--start-date",
        required=True,
        help="ISO-8601 timestamp (UTC) for the lower bound filter",
    )
    ch_parser.add_argument(
        "--end-date",
        required=True,
        help="ISO-8601 timestamp (UTC) for the upper bound filter",
    )
    ch_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the ClickHouse query but log batches instead of publishing",
    )
    ch_parser.add_argument(
        "--poll-interval",
        type=float,
        default=60.0,
        help="Seconds to sleep before rerunning the export when following",
    )
    ch_parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run a single export iteration instead of looping forever",
    )

    return parser


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    return parser.parse_args(list(argv) if argv is not None else None)


def build_runtime(cfg: PulsarConfig) -> Tuple[SnapshotBuilder, TimeSeriesStore, SimpleForecaster]:
    """Build runtime components based on config mode."""
    if cfg.mode == "nats_clickhouse" or cfg.mode == "clickhouse_only":
        if not cfg.clickhouse:
            raise ValueError("ClickHouse config is required for clickhouse mode")
        ch_loader = ClickHouseLoader(cfg)
        loader = ClickHouseSignalLoader(ch_loader)
    else:
        # Legacy Redis/RabbitMQ mode
        if not cfg.redis:
            raise ValueError("Redis config is required for redis_rabbitmq mode")
        redis_client = redis.from_url(cfg.redis.prepared_slave.url())
        loader = RedisSignalLoader(redis_client)
    
    builder = SnapshotBuilder(cfg, loader)
    store = TimeSeriesStore(cfg.ensure_cache_dir() / "timeseries")
    forecaster = SimpleForecaster(store)
    return builder, store, forecaster


def process_snapshot(builder: SnapshotBuilder, store: TimeSeriesStore, task: ImportTask) -> None:
    snapshot = builder.build(task)
    store.append(snapshot)


async def run_consumer_mode(cfg: PulsarConfig, builder: SnapshotBuilder, store: TimeSeriesStore, canary: bool) -> None:
    async def handler(task: ImportTask) -> None:
        process_snapshot(builder, store, task)

    if cfg.mode == "nats_clickhouse" and cfg.nats:
        await run_nats_consumer(cfg, handler)
    elif cfg.mode == "redis_rabbitmq" and cfg.rabbitmq:
        await run_consumer(cfg, handler, canary=canary)
    else:
        raise ValueError(f"Invalid mode {cfg.mode} or missing required config")


def handle_offline_tasks(builder: SnapshotBuilder, store: TimeSeriesStore, task_file: Path) -> None:
    payloads = json.loads(task_file.read_text())
    for payload in payloads:
        task = ImportTask.from_payload(payload)
        process_snapshot(builder, store, task)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)

    if args.command == "train-online":
        # Online training from NATS using CSV data
        if not cfg.nats:
            raise ValueError("NATS config is required for online training")
        data_dir = args.data_dir if args.data_dir.exists() else Path("data")
        if not data_dir.exists():
            raise ValueError(f"Data directory not found: {data_dir}")
        # Lazy import to avoid circular imports
        from pulsar_core.signals.nats_trainer import run_nats_trainer
        asyncio.run(run_nats_trainer(cfg, data_dir))
        return

    if args.command == "api":
        app = create_app(cfg)
        uvicorn.run(app, host=args.host, port=args.port)
        return

    if args.command == "train":
        # Show tracker status if requested
        if args.show_tracker:
            tracker = TrainingTracker(cfg.ensure_cache_dir())
            summary = tracker.get_training_summary()
            print(f"[pulsar] Training Tracker Summary:")
            print(f"  Total trained dates: {summary['total_trained_dates']}")
            if summary['first_trained_date']:
                print(f"  First trained date: {summary['first_trained_date']}")
                print(f"  Last trained date: {summary['last_trained_date']}")
            return
        
        # Load CSV data from data/ directory
        data_dir = Path("data")
        if data_dir.exists():
            from pulsar_core.signals.csv_loader import CSVSignalLoader, load_csv_files
            logger.info(f"Loading CSV files from {data_dir}")
            csv_data = load_csv_files(data_dir)
            total_rows = len(csv_data)
            logger.info(f"Loaded {total_rows} rows from CSV files")

            # Filter CSV rows by requested service types (if provided) to avoid
            # processing unnecessary data for this training run.
            if args.service_types:
                before = len(csv_data)
                csv_data = csv_data[csv_data["service_type"].isin(args.service_types)]
                logger.info(
                    f"Filtered CSV rows by service_types {args.service_types}: "
                    f"{len(csv_data)} / {before} rows"
                )

            # Optionally filter by configured forecast city_ids if present
            try:
                city_ids = getattr(getattr(cfg, "forecast", None), "city_ids", None)
            except Exception:
                city_ids = None
            if city_ids:
                before_city = len(csv_data)
                csv_data = csv_data[csv_data["city_id"].isin(city_ids)]
                logger.info(
                    f"Filtered CSV rows by forecast.city_ids {city_ids}: "
                    f"{len(csv_data)} / {before_city} rows"
                )

            # For local testing and initial model bring-up, cap the number of rows
            # we process so training completes quickly even on laptops.
            FAST_ROW_LIMIT = 100_000
            if len(csv_data) > FAST_ROW_LIMIT:
                logger.info(
                    f"Capping CSV rows for fast training: using last {FAST_ROW_LIMIT} "
                    f"of {len(csv_data)} rows after filtering"
                )
                csv_data = csv_data.sort_values("clickhouse_time").tail(FAST_ROW_LIMIT)
            
            # Process CSV data and build snapshots
            loader = CSVSignalLoader(csv_data)
            builder = SnapshotBuilder(cfg, loader)
            store = TimeSeriesStore(cfg.ensure_cache_dir() / "timeseries")
            
            # Process all CSV data
            from pulsar_core.periods import PeriodWindow, with_duration
            import pandas as pd
            
            csv_data_sorted = csv_data.sort_values('clickhouse_time')
            csv_data_sorted['period_start'] = csv_data_sorted['clickhouse_time'].apply(
                lambda ts: with_duration(
                    ts,
                    cfg.period_duration_minutes,
                    cfg.collect_duration_minutes
                ).start
            )
            
            processed = 0
            from typing import List
            from pulsar_core.features import HexagonSnapshot
            snapshots: List[HexagonSnapshot] = []
            for (period_start, service_type, city_id), group in csv_data_sorted.groupby(
                ['period_start', 'service_type', 'city_id']
            ):
                for hex_id in group['hex_id'].unique():
                    period_end = period_start + pd.Timedelta(minutes=cfg.period_duration_minutes)
                    collect_end = period_start + pd.Timedelta(minutes=cfg.collect_duration_minutes)
                    
                    period = PeriodWindow(
                        start=period_start,
                        end=period_end,
                        duration=period_end - period_start,
                        collect_duration=collect_end - period_start,
                    )
                    
                    task = ImportTask(
                        hexagon=int(hex_id),
                        service_type=int(service_type),
                        city_id=int(city_id),
                        period=period,
                        expansion_weight=None,
                    )
                    
                    snapshot = builder.build(task)
                    snapshots.append(snapshot)
                    processed += 1
                    # Periodic progress logging so the user can see that work
                    # is progressing on large datasets.
                    if processed % 10_000 == 0:
                        logger.info(f"Processed {processed} snapshots so far...")
            
            # Batch append for much better performance on large datasets
            store.append_many(snapshots)
            print(f"[pulsar] Processed {processed} snapshots from CSV data (batched write)")
        
        # Parse train date
        train_date = None
        if args.train_date:
            train_date = date.fromisoformat(args.train_date)
        
        if args.model_type == "cnn":
            tcfg = CNNTrainerConfig(
                window_size=args.cnn_window,
                epochs=args.cnn_epochs,
                batch_size=args.cnn_batch_size,
            )
            trainer = CNNTrainer(cfg, tcfg)
            result = trainer.train(service_types=args.service_types)
        else:
            trainer = MLTrainer(cfg)
            result = trainer.train(
                service_types=args.service_types,
                alpha=args.alpha,
                l1_ratio=args.l1_ratio,
                train_date=train_date,
                force=args.force,
            )
        print(
            f"[pulsar] trained on {result.rows} rows ({result.hexagons} hexagons). "
            f"MAE={result.mae:.2f}, RMSE={result.rmse:.2f}, model_uri={result.model_uri}"
        )
        return

    if args.command == "sync":
        builder, store, _ = build_runtime(cfg)
        if args.task_file:
            handle_offline_tasks(builder, store, args.task_file)
        else:
            asyncio.run(run_consumer_mode(cfg, builder, store, canary=args.canary))
        return

    if args.command == "clickhouse-export":
        query, columns = build_parameter_query(args.table, args.start_date, args.end_date)

        if args.run_once:
            summary = export_clickhouse_table(
                cfg,
                table=args.table,
                batch_size=args.batch_size,
                limit=args.limit,
                subject_override=args.subject,
                dry_run=args.dry_run,
                query=query,
                columns=columns,
            )
            print(
                f"[pulsar] published {summary.rows} rows across {summary.batches} batches "
                f"to subject {summary.subject}"
            )
            return

        print(
            "[pulsar] starting continuous ClickHouse export. Press Ctrl+C to stop.",
        )
        try:
            for iteration, summary in stream_clickhouse_table(
                cfg,
                table=args.table,
                batch_size=args.batch_size,
                limit=args.limit,
                subject_override=args.subject,
                dry_run=args.dry_run,
                poll_interval=args.poll_interval,
                query=query,
                columns=columns,
            ):
                print(
                    f"[pulsar] iteration {iteration}: published {summary.rows} rows across "
                    f"{summary.batches} batches to subject {summary.subject}"
                )
        except KeyboardInterrupt:
            print("[pulsar] clickhouse-export stopped by user")
        return

    raise ValueError(f"unsupported command: {args.command}")


__all__ = ["main", "build_parser"]
