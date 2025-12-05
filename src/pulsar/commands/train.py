"""Train command: Train ML model and log to MLflow."""

from __future__ import annotations

import argparse
import time
from typing import TYPE_CHECKING

from pulsar_core.models.cnn_trainer import CNNTrainer, CNNTrainerConfig
from pulsar_core.models.nats_trainer import NatsMLTrainer
from pulsar_core.models.trainer import MLTrainer

if TYPE_CHECKING:
    from pulsar_core.config import PulsarConfig


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    """Register the train command parser."""
    parser = subparsers.add_parser("train", help="Train ML model and log to MLflow")
    parser.add_argument(
        "--service-types", type=int, nargs="*", default=[1], help="Filter service types"
    )
    parser.add_argument("--alpha", type=float, default=0.3)
    parser.add_argument("--l1-ratio", type=float, default=0.1)
    parser.add_argument(
        "--model-type",
        choices=["elasticnet", "cnn"],
        default="cnn",
        help="Select training backend",
    )
    parser.add_argument(
        "--data-source",
        choices=["file", "nats"],
        default="file",
        help="Data source for training: 'file' reads from parquet cache, 'nats' subscribes to NATS events",
    )
    parser.add_argument(
        "--nats-timeout",
        type=float,
        default=60.0,
        help="Timeout in seconds for collecting data from NATS (only used with --data-source nats)",
    )
    parser.add_argument(
        "--nats-max-messages",
        type=int,
        default=None,
        help="Maximum number of NATS messages to collect (None = unlimited until timeout)",
    )
    parser.add_argument(
        "--nats-subject",
        type=str,
        default=None,
        help="Override NATS subject from config (only used with --data-source nats)",
    )
    parser.add_argument(
        "--cnn-window", type=int, default=12, help="Sequence window size for CNN model"
    )
    parser.add_argument("--cnn-epochs", type=int, default=25)
    parser.add_argument("--cnn-batch-size", type=int, default=128)
    parser.add_argument(
        "--prometheus-pushgateway-url",
        default="",
        dest="prometheus_pushgateway_url",
        help="Optional Prometheus Pushgateway URL to push training metrics",
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8088)
    return parser


def _push_metrics_to_prometheus(
    args: argparse.Namespace, result, duration: float
) -> None:
    """Push training metrics to Prometheus Pushgateway if configured."""
    prom_arg = getattr(args, "prometheus_pushgateway_url", None)
    if not prom_arg or prom_arg == "":
        return

    try:
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
    except ImportError:
        print(
            "[pulsar] prometheus_client not installed, skipping Prometheus push",
            flush=True,
        )
        return

    registry = CollectorRegistry()

    g_rows = Gauge(
        "pulsar_training_rows",
        "Number of rows used for training",
        registry=registry,
    )
    g_hexagons = Gauge(
        "pulsar_training_hexagons",
        "Number of hexagons used for training",
        registry=registry,
    )
    g_mae = Gauge(
        "pulsar_training_mae",
        "Mean absolute error of the trained model",
        registry=registry,
    )
    g_rmse = Gauge(
        "pulsar_training_rmse",
        "Root mean squared error of the trained model",
        registry=registry,
    )
    g_duration = Gauge(
        "pulsar_training_duration_seconds",
        "Training duration in seconds",
        registry=registry,
    )

    g_rows.set(result.rows)
    g_hexagons.set(result.hexagons)
    g_mae.set(result.mae)
    g_rmse.set(result.rmse)
    g_duration.set(duration)

    push_to_gateway(
        args.prometheus_pushgateway_url,
        job="pulsar_training",
        registry=registry,
    )


def run(
    cfg: PulsarConfig, args: argparse.Namespace, start_time: float | None = None
) -> None:
    """Execute the train command."""
    if start_time is None:
        start_time = time.time()

    print("Training model")
    data_source = getattr(args, "data_source", "file")

    if args.model_type == "cnn":
        print("Training CNN model")
        # CNN trainer currently only supports file-based data
        if data_source == "nats":
            raise ValueError(
                "CNN trainer does not support NATS data source yet. Use --data-source file"
            )
        tcfg = CNNTrainerConfig(
            window_size=args.cnn_window,
            epochs=args.cnn_epochs,
            batch_size=args.cnn_batch_size,
        )
        trainer = CNNTrainer(cfg, tcfg)
        result = trainer.train(service_types=args.service_types)
    else:
        if data_source == "nats":
            print("Training ElasticNet model from NATS events")
            trainer = NatsMLTrainer(cfg)
            result = trainer.train(
                service_types=args.service_types,
                alpha=args.alpha,
                l1_ratio=args.l1_ratio,
                timeout_seconds=getattr(args, "nats_timeout", 60.0),
                max_messages=getattr(args, "nats_max_messages", None),
                subject_override=getattr(args, "nats_subject", None),
            )
        else:
            print("Training ElasticNet model from file cache")
            trainer = MLTrainer(cfg)
            result = trainer.train(
                service_types=args.service_types,
                alpha=args.alpha,
                l1_ratio=args.l1_ratio,
            )

    duration = time.time() - start_time
    print(
        f"[pulsar] trained on {result.rows} rows ({result.hexagons} hexagons) in {duration:.2f} seconds. "
        f"MAE={result.mae:.2f}, RMSE={result.rmse:.2f}, model_uri={result.model_uri}"
    )

    _push_metrics_to_prometheus(args, result, duration)
