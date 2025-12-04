"""Pulsar CLI - Main entry point."""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Iterable

from pulsar.commands import COMMANDS
from pulsar_core.config import load_config


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with all registered commands."""
    parser = argparse.ArgumentParser(description="Pulsar bridge runner")
    parser.add_argument(
        "--config",
        type=Path,
        default="/app/config.yaml",
        help="Path to config file (defaults to $PULSAR_CONFIG or ./config.yaml)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command_module in COMMANDS:
        command_module.register(subparsers)

    return parser


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = build_parser()
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> None:
    """Main CLI entry point."""
    start_time = time.time()
    args = parse_args(argv)
    cfg = load_config(args.config)

    from pulsar.commands import api, clickhouse_export, sync, train

    command_handlers = {
        "api": lambda: api.run(cfg, args),
        "sync": lambda: sync.run(cfg, args),
        "train": lambda: train.run(cfg, args, start_time),
        "clickhouse-export": lambda: clickhouse_export.run(cfg, args),
    }

    handler = command_handlers.get(args.command)
    if handler:
        handler()
    else:
        raise ValueError(f"unsupported command: {args.command}")


__all__ = ["main", "build_parser"]
