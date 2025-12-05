"""Sync command: Ingest scheduler tasks and build snapshots."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING

from pulsar.commands.base import build_runtime, process_snapshot
from pulsar_core.signals import ImportTask, run_consumer

if TYPE_CHECKING:
    from pulsar_core.config import PulsarConfig
    from pulsar_core.features import SnapshotBuilder
    from pulsar_core.store import TimeSeriesStore


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    """Register the sync command parser."""
    parser = subparsers.add_parser(
        "sync", help="Ingest scheduler tasks and build snapshots"
    )
    parser.add_argument(
        "--task-file", type=Path, help="Optional JSON file with import tasks array"
    )
    parser.add_argument("--canary", action="store_true", help="Listen to canary queues")
    return parser


async def _run_consumer_mode(
    cfg: PulsarConfig, builder: SnapshotBuilder, store: TimeSeriesStore, canary: bool
) -> None:
    """Run the async consumer mode."""

    async def handler(task: ImportTask) -> None:
        process_snapshot(builder, store, task)

    await run_consumer(cfg, handler, canary=canary)


def _handle_offline_tasks(
    builder: SnapshotBuilder, store: TimeSeriesStore, task_file: Path
) -> None:
    """Process tasks from a JSON file."""
    payloads = json.loads(task_file.read_text())
    for payload in payloads:
        task = ImportTask.from_payload(payload)
        process_snapshot(builder, store, task)


def run(cfg: PulsarConfig, args: argparse.Namespace) -> None:
    """Execute the sync command."""
    builder, store, _ = build_runtime(cfg)
    if args.task_file:
        _handle_offline_tasks(builder, store, args.task_file)
    else:
        asyncio.run(_run_consumer_mode(cfg, builder, store, canary=args.canary))
