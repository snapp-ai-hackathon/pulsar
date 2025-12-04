from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Iterable, Tuple

import redis
import uvicorn

from pulsar.clickhouse_nats import (
    build_parameter_query,
    export_clickhouse_table,
    stream_clickhouse_table,
)
from pulsar_core.config import PulsarConfig, load_config
from pulsar_core.features import SnapshotBuilder
from pulsar_core.models import SimpleForecaster
from pulsar_core.models.cnn_trainer import CNNTrainer, CNNTrainerConfig
from pulsar_core.models.trainer import MLTrainer
from pulsar_core.service import create_app
from pulsar_core.signals import ImportTask, RedisSignalLoader, run_consumer
from pulsar_core.store import TimeSeriesStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pulsar bridge runner")
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to config file (defaults to $PULSAR_CONFIG or ./config.yaml)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser(
        "sync", help="Ingest scheduler tasks and build snapshots"
    )
    sync_parser.add_argument(
        "--task-file", type=Path, help="Optional JSON file with import tasks array"
    )
    sync_parser.add_argument(
        "--canary", action="store_true", help="Listen to canary queues"
    )

    api_parser = subparsers.add_parser("api", help="Serve forecast API")
    api_parser.add_argument("--host", default="0.0.0.0")
    api_parser.add_argument("--port", type=int, default=8088)

    train_parser = subparsers.add_parser(
        "train", help="Train ML model and log to MLflow"
    )
    train_parser.add_argument(
        "--service-types", type=int, nargs="*", default=[], help="Filter service types"
    )
    train_parser.add_argument("--alpha", type=float, default=0.3)
    train_parser.add_argument("--l1-ratio", type=float, default=0.1)
    train_parser.add_argument(
        "--model-type",
        choices=["elasticnet", "cnn"],
        default="elasticnet",
        help="Select training backend",
    )
    train_parser.add_argument(
        "--cnn-window", type=int, default=12, help="Sequence window size for CNN model"
    )
    train_parser.add_argument("--cnn-epochs", type=int, default=25)
    train_parser.add_argument("--cnn-batch-size", type=int, default=128)

    ch_parser = subparsers.add_parser(
        "clickhouse-export",
        help="Fetch ClickHouse data in batches and publish them to NATS",
    )
    ch_parser.add_argument(
        "--table",
        default="snapp_raw_log.kandoo_parameter_nats",
        help="ClickHouse table name to read",
    )
    ch_parser.add_argument(
        "--batch-size", type=int, default=1000, help="Rows per published batch"
    )
    ch_parser.add_argument(
        "--limit", type=int, help="Optional maximum row count to send"
    )
    ch_parser.add_argument("--subject", help="Override the configured NATS subject")
    ch_parser.add_argument(
        "--start-date", required=True, help="ISO timestamp lower bound (UTC)"
    )
    ch_parser.add_argument(
        "--end-date", required=True, help="ISO timestamp upper bound (UTC)"
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


def build_runtime(
    cfg: PulsarConfig,
) -> Tuple[SnapshotBuilder, TimeSeriesStore, SimpleForecaster]:
    redis_client = redis.from_url(cfg.redis.prepared_slave.url())
    loader = RedisSignalLoader(redis_client)
    builder = SnapshotBuilder(cfg, loader)
    store = TimeSeriesStore(cfg.ensure_cache_dir() / "timeseries")
    forecaster = SimpleForecaster(store)
    return builder, store, forecaster


def process_snapshot(
    builder: SnapshotBuilder, store: TimeSeriesStore, task: ImportTask
) -> None:
    snapshot = builder.build(task)
    store.append(snapshot)


async def run_consumer_mode(
    cfg: PulsarConfig, builder: SnapshotBuilder, store: TimeSeriesStore, canary: bool
) -> None:
    async def handler(task: ImportTask) -> None:
        process_snapshot(builder, store, task)

    await run_consumer(cfg, handler, canary=canary)


def handle_offline_tasks(
    builder: SnapshotBuilder, store: TimeSeriesStore, task_file: Path
) -> None:
    payloads = json.loads(task_file.read_text())
    for payload in payloads:
        task = ImportTask.from_payload(payload)
        process_snapshot(builder, store, task)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)

    if args.command == "api":
        app = create_app(cfg)
        uvicorn.run(app, host=args.host, port=args.port)
        return

    if args.command == "train":
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
        query, columns = build_parameter_query(
            args.table, args.start_date, args.end_date
        )

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

        print("[pulsar] starting continuous ClickHouse export. Press Ctrl+C to stop.")
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
