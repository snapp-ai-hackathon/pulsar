"""Base utilities shared across CLI commands."""

from __future__ import annotations

from typing import TYPE_CHECKING, Tuple

import redis

from pulsar_core.features import SnapshotBuilder
from pulsar_core.models import SimpleForecaster
from pulsar_core.signals import ImportTask, RedisSignalLoader
from pulsar_core.store import TimeSeriesStore

if TYPE_CHECKING:
    from pulsar_core.config import PulsarConfig


def build_runtime(
    cfg: PulsarConfig,
) -> Tuple[SnapshotBuilder, TimeSeriesStore, SimpleForecaster]:
    """Build the runtime components for sync/import modes.

    Args:
        cfg: Pulsar configuration object

    Returns:
        Tuple of (SnapshotBuilder, TimeSeriesStore, SimpleForecaster)

    Raises:
        RuntimeError: If Redis configuration is missing
    """
    if cfg.redis is None:
        raise RuntimeError(
            "Redis configuration is required for sync/import modes but is missing."
        )
    redis_client = redis.from_url(cfg.redis.prepared_slave.url())
    loader = RedisSignalLoader(redis_client)
    builder = SnapshotBuilder(cfg, loader)
    store = TimeSeriesStore(cfg.ensure_cache_dir() / "timeseries")
    forecaster = SimpleForecaster(store)
    return builder, store, forecaster


def process_snapshot(
    builder: SnapshotBuilder, store: TimeSeriesStore, task: ImportTask
) -> None:
    """Process a single import task and store its snapshot."""
    snapshot = builder.build(task)
    store.append(snapshot)
