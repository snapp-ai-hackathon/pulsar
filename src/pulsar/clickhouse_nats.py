from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import time
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple
from uuid import UUID

from clickhouse_driver import Client as ClickHouseClient
from dateutil import parser as date_parser
import nats

from pulsar_core.config import PulsarConfig

PARAMETER_COLUMNS: Tuple[str, ...] = (
    "from",
    "to",
    "service_type",
    "city_id",
    "hex_id",
    "rule_sheet_id",
    "surge_percent",
    "cumulative_surge_percent",
    "surge_absolute",
    "cumulative_surge_absolute",
    "rule_id",
    "increase_factor",
    "decrease_factor",
    "resolution",
    "reason",
    "absolute_reason",
    "accept_rate",
    "price_cnvr",
    "logstash_time",
    "created_date",
    "clickhouse_time",
)

PARAMETER_QUERY = (
    "SELECT `from`, to, service_type, city_id, hex_id, rule_sheet_id, surge_percent, "
    "cumulative_surge_percent, surge_absolute, cumulative_surge_absolute, rule_id, increase_factor, "
    "decrease_factor, resolution, reason, absolute_reason, accept_rate, price_cnvr, logstash_time, "
    "created_date, clickhouse_time FROM {table}"
    " WHERE `from` > toDateTime('{start}') AND `from` <= toDateTime('{end}')"
)


@dataclass
class PublishSummary:
    table: str
    subject: str
    batches: int
    rows: int


def _quote_identifier(identifier: str) -> str:
    parts = identifier.split(".")
    quoted_parts = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        escaped = part.replace("`", "``")
        quoted_parts.append(f"`{escaped}`")
    return ".".join(quoted_parts) if quoted_parts else ""


def _serialize_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    return value


def _row_dict(columns: List[str], row: Iterable[object]) -> dict:
    return {column: _serialize_value(value) for column, value in zip(columns, row)}


def _iter_clickhouse_batches(
    cfg: PulsarConfig,
    table: Optional[str],
    batch_size: int,
    limit: Optional[int],
    query: Optional[str],
    columns: Optional[Sequence[str]],
) -> Iterator[List[dict]]:
    client = ClickHouseClient(
        host=cfg.clickhouse.host,
        port=cfg.clickhouse.port,
        user=cfg.clickhouse.user,
        password=cfg.clickhouse.password or "",
        database=cfg.clickhouse.database,
        secure=cfg.clickhouse.secure,
    )

    described_table = _quote_identifier(table) if table else None
    query_text = query.strip().rstrip(";") if query else None
    if not query_text:
        if not described_table:
            raise ValueError("table is required when query is not provided")
        query_text = f"SELECT * FROM {described_table}"
    if limit:
        query_text = f"{query_text} LIMIT {limit}"

    if columns:
        column_names = list(columns)
    elif described_table:
        column_names = [row[0] for row in client.execute(f"DESCRIBE TABLE {described_table}")]
    else:
        raise ValueError("columns must be provided when describing arbitrary queries")

    base_settings = {"max_block_size": batch_size}
    if cfg.clickhouse.settings:
        base_settings.update(cfg.clickhouse.settings)

    def iterator() -> Iterator[List[dict]]:
        try:
            batch: List[dict] = []
            emitted = 0
            for row in client.execute_iter(query_text, settings=base_settings):
                batch.append(_row_dict(column_names, row))
                emitted += 1
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
                if limit and emitted >= limit:
                    break
            if batch:
                yield batch
        finally:
            client.disconnect()

    return iterator()


async def _publish_batches(
    batches: Iterator[List[dict]],
    *,
    table: str,
    subject: str,
    address: str,
    dry_run: bool,
) -> PublishSummary:
    rows_sent = 0
    batches_sent = 0

    if dry_run:
        for batch in batches:
            batches_sent += 1
            rows_sent += len(batch)
            print(f"[pulsar] dry-run batch {batches_sent}: {len(batch)} rows")
        return PublishSummary(table=table, subject=subject, batches=batches_sent, rows=rows_sent)

    nc = await nats.connect(address)
    try:
        for batch in batches:
            batches_sent += 1
            rows_sent += len(batch)
            envelope = {
                "table": table,
                "batch": batches_sent,
                "row_count": len(batch),
                "rows": batch,
            }
            await nc.publish(subject, json.dumps(envelope).encode("utf-8"))
        await nc.flush()
    finally:
        await nc.drain()

    return PublishSummary(table=table, subject=subject, batches=batches_sent, rows=rows_sent)


def export_clickhouse_table(
    cfg: PulsarConfig,
    *,
    table: str,
    batch_size: int,
    limit: Optional[int] = None,
    subject_override: Optional[str] = None,
    dry_run: bool = False,
    query: Optional[str] = None,
    columns: Optional[Sequence[str]] = None,
) -> PublishSummary:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    subject = subject_override or cfg.nats.subject
    batches = _iter_clickhouse_batches(
        cfg,
        table=table,
        batch_size=batch_size,
        limit=limit,
        query=query,
        columns=columns,
    )
    return asyncio.run(
        _publish_batches(
            batches,
            table=table,
            subject=subject,
            address=cfg.nats.address,
            dry_run=dry_run,
        )
    )


def stream_clickhouse_table(
    cfg: PulsarConfig,
    *,
    table: str,
    batch_size: int,
    limit: Optional[int] = None,
    subject_override: Optional[str] = None,
    dry_run: bool = False,
    poll_interval: float = 60.0,
    query: Optional[str] = None,
    columns: Optional[Sequence[str]] = None,
) -> Iterator[Tuple[int, PublishSummary]]:
    if poll_interval <= 0:
        raise ValueError("poll_interval must be positive seconds")

    iteration = 0
    while True:
        iteration += 1
        summary = export_clickhouse_table(
            cfg,
            table=table,
            batch_size=batch_size,
            limit=limit,
            subject_override=subject_override,
            dry_run=dry_run,
            query=query,
            columns=columns,
        )
        yield iteration, summary
        time.sleep(poll_interval)


def _format_clickhouse_timestamp(value: str) -> str:
    dt = date_parser.isoparse(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def build_parameter_query(table: str, start: str, end: str) -> Tuple[str, Sequence[str]]:
    start_fmt = _format_clickhouse_timestamp(start)
    end_fmt = _format_clickhouse_timestamp(end)
    query = PARAMETER_QUERY.format(
        table=_quote_identifier(table),
        start=start_fmt,
        end=end_fmt,
    )
    return query, PARAMETER_COLUMNS


__all__ = [
    "export_clickhouse_table",
    "stream_clickhouse_table",
    "build_parameter_query",
    "PublishSummary",
]
