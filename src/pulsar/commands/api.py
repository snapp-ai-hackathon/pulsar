"""API command: Serve forecast API."""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

import uvicorn

from pulsar_core.service import create_app

if TYPE_CHECKING:
    from pulsar_core.config import PulsarConfig


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    """Register the api command parser."""
    parser = subparsers.add_parser("api", help="Serve forecast API")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8088)
    return parser


def run(cfg: PulsarConfig, args: argparse.Namespace) -> None:
    """Execute the api command."""
    app = create_app(cfg)
    uvicorn.run(app, host=args.host, port=args.port)
