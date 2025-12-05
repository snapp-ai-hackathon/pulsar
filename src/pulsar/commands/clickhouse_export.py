"""ClickHouse Export command: Fetch ClickHouse data and publish to NATS."""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

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
        "--start-date", required=True, help="ISO timestamp lower bound (UTC)"
    )
    parser.add_argument(
        "--end-date", required=True, help="ISO timestamp upper bound (UTC)"
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
    query, columns, params = build_parameter_query(
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
            params=params,
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
            params=params,
        ):
            print(
                f"[pulsar] iteration {iteration}: published {summary.rows} rows across "
                f"{summary.batches} batches to subject {summary.subject}"
            )
    except KeyboardInterrupt:
        print("[pulsar] clickhouse-export stopped by user")
