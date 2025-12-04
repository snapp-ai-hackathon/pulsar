"""Pulsar CLI commands module.

Each command is implemented in its own file and exposes:
- register(subparsers): Registers the command's argument parser
- run(cfg, args): Executes the command logic
"""

from __future__ import annotations

from pulsar.commands import api, clickhouse_export, sync, train

COMMANDS = [
    sync,
    api,
    train,
    clickhouse_export,
]

__all__ = ["COMMANDS", "api", "clickhouse_export", "sync", "train"]
