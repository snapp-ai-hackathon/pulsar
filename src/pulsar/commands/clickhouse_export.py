"""ClickHouse Export command: Fetch ClickHouse data and publish to NATS."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from dateutil import parser as date_parser

from pulsar.clickhouse_nats import (
    build_parameter_query,
    export_clickhouse_table,
    stream_clickhouse_table,
)

if TYPE_CHECKING:
    from pulsar_core.config import PulsarConfig


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    """Register the clickhouse-export command parser."""
    parser = subparsers.add_parser(
        "clickhouse-export",
        help="Fetch ClickHouse data in batches and publish them to NATS",
    )
    parser.add_argument(
        "--table",
        default="snapp_raw_log.kandoo_parameter_nats",
        help="ClickHouse table name to read",
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Rows per published batch"
    )
    parser.add_argument("--limit", type=int, help="Optional maximum row count to send")
    parser.add_argument("--subject", help="Override the configured NATS subject")
    parser.add_argument(
        "--start-date",
        help="ISO timestamp lower bound (UTC). Omit to start from now.",
    )
    parser.add_argument(
        "--end-date",
        help="ISO timestamp upper bound (UTC). If omitted, the exporter will keep following new data.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the ClickHouse query but log batches instead of publishing",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=60.0,
        help="Seconds to sleep before rerunning the export when following",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run a single export iteration instead of looping forever",
    )
    return parser


def run(cfg: PulsarConfig, args: argparse.Namespace) -> None:
    """Execute the clickhouse-export command."""
    if args.end_date and not args.start_date:
        raise SystemExit("--end-date requires --start-date")

    def _now_utc() -> datetime:
        return datetime.now(timezone.utc)

    def _parse_dt(value: str) -> datetime:
        dt = date_parser.isoparse(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    follow_mode = args.end_date is None
    start_dt = _parse_dt(args.start_date) if args.start_date else _now_utc()
    end_dt = _parse_dt(args.end_date) if args.end_date else _now_utc()

    query, columns, params = build_parameter_query(
        args.table, start_dt.isoformat(), end_dt.isoformat()
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
            params=params,
        )
        print(
            f"[pulsar] published {summary.rows} rows across {summary.batches} batches "
            f"to subject {summary.subject}"
        )
        return

    print("[pulsar] starting continuous ClickHouse export. Press Ctrl+C to stop.")

    def query_factory():
        nonlocal start_dt
        current_end = _now_utc()
        iter_query, iter_columns, iter_params = build_parameter_query(
            args.table, start_dt.isoformat(), current_end.isoformat()
        )
        start_dt = current_end
        return iter_query, iter_columns, iter_params

    try:
        for iteration, summary in stream_clickhouse_table(
            cfg,
            table=args.table,
            batch_size=args.batch_size,
            limit=args.limit,
            subject_override=args.subject,
            dry_run=args.dry_run,
            poll_interval=args.poll_interval,
            query=None if follow_mode else query,
            columns=None if follow_mode else columns,
            params=None if follow_mode else params,
            query_factory=query_factory if follow_mode else None,
        ):
            print(
                f"[pulsar] iteration {iteration}: published {summary.rows} rows across "
                f"{summary.batches} batches to subject {summary.subject}"
            )
    except KeyboardInterrupt:
        print("[pulsar] clickhouse-export stopped by user")
