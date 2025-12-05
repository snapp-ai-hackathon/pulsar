from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import time
from typing import Any
from uuid import UUID

from clickhouse_driver import Client as ClickHouseClient
from dateutil import parser as date_parser
import nats
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from pulsar_core.config import PulsarConfig

logger = logging.getLogger(__name__)

PARAMETER_COLUMNS: tuple[str, ...] = (
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

# Build SELECT columns from PARAMETER_COLUMNS (quote reserved words like 'from')
_RESERVED_WORDS = {"from", "to", "order", "group", "select", "where", "limit"}
_SELECT_COLUMNS = ", ".join(
    f"`{col}`" if col in _RESERVED_WORDS else col for col in PARAMETER_COLUMNS
)

# Parameterized query template - uses %(name)s placeholders for safe parameter binding
PARAMETER_QUERY_TEMPLATE = (
    f"SELECT {_SELECT_COLUMNS} FROM {{table}}"
    " WHERE `from` > toDateTime(%(start)s) AND `from` <= toDateTime(%(end)s)"
)

# Legacy format string query (deprecated, kept for backwards compatibility)
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
        # Use string to preserve precision for financial/high-precision data
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    return value


def _row_dict(columns: list[str], row: Iterable[object]) -> dict:
    return {column: _serialize_value(value) for column, value in zip(columns, row)}


# Default timeout settings for ClickHouse queries
DEFAULT_QUERY_SETTINGS: dict[str, Any] = {
    "max_execution_time": 300,  # 5 minutes max query execution
    "connect_timeout": 10,
    "send_receive_timeout": 300,
}


@contextmanager
def _clickhouse_client(cfg: PulsarConfig):
    """Context manager for ClickHouse client with proper cleanup."""
    client = ClickHouseClient(
        host=cfg.clickhouse.host,
        port=cfg.clickhouse.port,
        user=cfg.clickhouse.user,
        password=cfg.clickhouse.password or "",
        database=cfg.clickhouse.database,
        secure=cfg.clickhouse.secure,
    )
    try:
        yield client
    finally:
        client.disconnect()


class ClickHouseConnectionManager:
    """Manages ClickHouse client connections with optional reuse."""

    def __init__(self, cfg: PulsarConfig):
        self._cfg = cfg
        self._client: ClickHouseClient | None = None

    def get_client(self) -> ClickHouseClient:
        """Get or create a ClickHouse client."""
        if self._client is None:
            self._client = ClickHouseClient(
                host=self._cfg.clickhouse.host,
                port=self._cfg.clickhouse.port,
                user=self._cfg.clickhouse.user,
                password=self._cfg.clickhouse.password or "",
                database=self._cfg.clickhouse.database,
                secure=self._cfg.clickhouse.secure,
            )
            logger.debug(
                "Created ClickHouse client",
                extra={
                    "host": self._cfg.clickhouse.host,
                    "database": self._cfg.clickhouse.database,
                },
            )
        return self._client

    def close(self):
        """Close the client connection."""
        if self._client is not None:
            self._client.disconnect()
            self._client = None
            logger.debug("Closed ClickHouse client connection")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((ConnectionError, OSError)),
    reraise=True,
)
def _execute_query_with_retry(
    client: ClickHouseClient,
    query: str,
    params: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
):
    """Execute a ClickHouse query with retry logic for transient failures."""
    logger.debug(
        "Executing query",
        extra={"query": query[:100], "has_params": params is not None},
    )
    return client.execute(query, params, settings=settings)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((ConnectionError, OSError)),
    reraise=True,
)
def _execute_iter_with_retry(
    client: ClickHouseClient,
    query: str,
    params: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
):
    """Execute a streaming ClickHouse query with retry logic."""
    logger.debug("Executing streaming query", extra={"query": query[:100]})
    return client.execute_iter(query, params, settings=settings)


def _iter_clickhouse_batches(
    cfg: PulsarConfig,
    table: str | None,
    batch_size: int,
    limit: int | None,
    query: str | None,
    columns: Sequence[str] | None,
    params: dict[str, Any] | None = None,
    progress_callback: Callable[[int], None] | None = None,
    connection_manager: ClickHouseConnectionManager | None = None,
) -> Iterator[list[dict]]:
    """
    Iterate over ClickHouse query results in batches.

    Args:
        cfg: Pulsar configuration
        table: Table name (optional if query is provided)
        batch_size: Number of rows per batch
        limit: Maximum total rows to fetch
        query: Custom SQL query (optional)
        columns: Column names for the result set
        params: Query parameters for parameterized queries
        progress_callback: Optional callback called with row count periodically
        connection_manager: Optional connection manager for client reuse
    """
    # Use provided connection manager or create a temporary client
    own_client = connection_manager is None
    if own_client:
        client = ClickHouseClient(
            host=cfg.clickhouse.host,
            port=cfg.clickhouse.port,
            user=cfg.clickhouse.user,
            password=cfg.clickhouse.password or "",
            database=cfg.clickhouse.database,
            secure=cfg.clickhouse.secure,
        )
    else:
        client = connection_manager.get_client()

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
        column_names = [
            row[0]
            for row in _execute_query_with_retry(
                client, f"DESCRIBE TABLE {described_table}"
            )
        ]
    else:
        raise ValueError("columns must be provided when describing arbitrary queries")

    # Build settings with defaults and user overrides
    base_settings: dict[str, Any] = {
        **DEFAULT_QUERY_SETTINGS,
        "max_block_size": batch_size,
    }
    if cfg.clickhouse.settings:
        base_settings.update(cfg.clickhouse.settings)

    logger.info(
        "Starting ClickHouse batch iteration",
        extra={
            "table": table,
            "batch_size": batch_size,
            "limit": limit,
            "has_params": params is not None,
        },
    )

    def iterator() -> Iterator[list[dict]]:
        try:
            batch: list[dict] = []
            emitted = 0
            for row in _execute_iter_with_retry(
                client, query_text, params, settings=base_settings
            ):
                batch.append(_row_dict(column_names, row))
                emitted += 1
                if len(batch) >= batch_size:
                    yield batch
                    logger.debug(
                        "Yielded batch",
                        extra={"rows": len(batch), "total_emitted": emitted},
                    )
                    batch = []
                if limit and emitted >= limit:
                    break
                # Progress callback every 10000 rows
                if progress_callback and emitted % 10000 == 0:
                    progress_callback(emitted)
            if batch:
                yield batch
                logger.debug(
                    "Yielded final batch",
                    extra={"rows": len(batch), "total_emitted": emitted},
                )

            logger.info("Completed batch iteration", extra={"total_rows": emitted})
        finally:
            if own_client:
                client.disconnect()

    return iterator()


async def _publish_batches(
    batches: Iterator[list[dict]],
    *,
    table: str,
    subject: str,
    address: str,
    dry_run: bool,
    nc: nats.NATS | None = None,
) -> PublishSummary:
    """
    Publish batches to NATS.

    Args:
        batches: Iterator of row batches
        table: Source table name
        subject: NATS subject to publish to
        address: NATS server address
        dry_run: If True, don't actually publish
        nc: Optional existing NATS connection for reuse
    """
    rows_sent = 0
    batches_sent = 0

    if dry_run:
        for batch in batches:
            batches_sent += 1
            rows_sent += len(batch)
            logger.info(
                "Dry-run batch", extra={"batch": batches_sent, "rows": len(batch)}
            )
        return PublishSummary(
            table=table, subject=subject, batches=batches_sent, rows=rows_sent
        )

    own_connection = nc is None
    if own_connection:
        nc = await nats.connect(address)
        logger.debug("Connected to NATS", extra={"address": address})

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
            logger.debug(
                "Published batch",
                extra={"batch": batches_sent, "rows": len(batch), "subject": subject},
            )
        await nc.flush()
        logger.info(
            "Finished publishing batches",
            extra={"total_batches": batches_sent, "total_rows": rows_sent},
        )
    finally:
        if own_connection:
            await nc.drain()
            logger.debug("Closed NATS connection")

    return PublishSummary(
        table=table, subject=subject, batches=batches_sent, rows=rows_sent
    )


def export_clickhouse_table(
    cfg: PulsarConfig,
    *,
    table: str,
    batch_size: int,
    limit: int | None = None,
    subject_override: str | None = None,
    dry_run: bool = False,
    query: str | None = None,
    columns: Sequence[str] | None = None,
    params: dict[str, Any] | None = None,
    progress_callback: Callable[[int], None] | None = None,
    connection_manager: ClickHouseConnectionManager | None = None,
) -> PublishSummary:
    """
    Export data from a ClickHouse table to NATS.

    Args:
        cfg: Pulsar configuration
        table: ClickHouse table name
        batch_size: Number of rows per batch
        limit: Maximum rows to export
        subject_override: Override NATS subject from config
        dry_run: If True, don't publish to NATS
        query: Custom SQL query (optional)
        columns: Column names for the result set
        params: Query parameters for parameterized queries
        progress_callback: Called with row count periodically
        connection_manager: Optional connection manager for client reuse
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    subject = subject_override or cfg.nats.subject
    logger.info(
        "Starting export",
        extra={
            "table": table,
            "subject": subject,
            "batch_size": batch_size,
            "limit": limit,
        },
    )

    batches = _iter_clickhouse_batches(
        cfg,
        table=table,
        batch_size=batch_size,
        limit=limit,
        query=query,
        columns=columns,
        params=params,
        progress_callback=progress_callback,
        connection_manager=connection_manager,
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


async def stream_clickhouse_table_async(
    cfg: PulsarConfig,
    *,
    table: str,
    batch_size: int,
    limit: int | None = None,
    subject_override: str | None = None,
    dry_run: bool = False,
    poll_interval: float = 60.0,
    query: str | None = None,
    columns: Sequence[str] | None = None,
    params: dict[str, Any] | None = None,
    progress_callback: Callable[[int], None] | None = None,
    query_factory: Callable[[], tuple[str | None, Sequence[str] | None, dict[str, Any] | None]] | None = None,
):
    """
    Async streaming export with connection reuse.

    This is the preferred method for continuous streaming as it:
    - Reuses the NATS connection across iterations
    - Reuses the ClickHouse connection across iterations
    - Properly handles async/await without creating new event loops
    - Optionally rebuilds the query each iteration via query_factory

    Yields:
        Tuple of (iteration number, PublishSummary)
    """
    if poll_interval <= 0:
        raise ValueError("poll_interval must be positive seconds")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    subject = subject_override or cfg.nats.subject
    logger.info(
        "Starting async streaming export",
        extra={
            "table": table,
            "subject": subject,
            "poll_interval": poll_interval,
        },
    )

    # Reuse connections across iterations
    with ClickHouseConnectionManager(cfg) as ch_manager:
        nc = None if dry_run else await nats.connect(cfg.nats.address)
        try:
            iteration = 0
            while True:
                iteration += 1
                logger.debug("Starting iteration", extra={"iteration": iteration})

                if query_factory:
                    iter_query, iter_columns, iter_params = query_factory()
                else:
                    iter_query, iter_columns, iter_params = query, columns, params

                batches = _iter_clickhouse_batches(
                    cfg,
                    table=table,
                    batch_size=batch_size,
                    limit=limit,
                    query=iter_query,
                    columns=iter_columns,
                    params=iter_params,
                    progress_callback=progress_callback,
                    connection_manager=ch_manager,
                )

                summary = await _publish_batches(
                    batches,
                    table=table,
                    subject=subject,
                    address=cfg.nats.address,
                    dry_run=dry_run,
                    nc=nc,
                )
                yield iteration, summary

                await asyncio.sleep(poll_interval)
        finally:
            if nc is not None:
                await nc.drain()
                logger.debug("Closed NATS connection")


def stream_clickhouse_table(
    cfg: PulsarConfig,
    *,
    table: str,
    batch_size: int,
    limit: int | None = None,
    subject_override: str | None = None,
    dry_run: bool = False,
    poll_interval: float = 60.0,
    query: str | None = None,
    columns: Sequence[str] | None = None,
    params: dict[str, Any] | None = None,
    progress_callback: Callable[[int], None] | None = None,
    query_factory: Callable[[], tuple[str | None, Sequence[str] | None, dict[str, Any] | None]] | None = None,
) -> Iterator[tuple[int, PublishSummary]]:
    """
    Synchronous wrapper for streaming export.

    For better performance and resource usage, consider using
    stream_clickhouse_table_async() directly in an async context.
    Supports dynamic query rebuilding via query_factory.
    """
    if poll_interval <= 0:
        raise ValueError("poll_interval must be positive seconds")

    logger.info(
        "Starting streaming export",
        extra={"table": table, "poll_interval": poll_interval},
    )

    # Use connection manager for ClickHouse connection reuse
    with ClickHouseConnectionManager(cfg) as ch_manager:
        iteration = 0
        while True:
            iteration += 1
            if query_factory:
                iter_query, iter_columns, iter_params = query_factory()
            else:
                iter_query, iter_columns, iter_params = query, columns, params

            batches = _iter_clickhouse_batches(
                cfg,
                table=table,
                batch_size=batch_size,
                limit=limit,
                query=iter_query,
                columns=iter_columns,
                params=iter_params,
                progress_callback=progress_callback,
                connection_manager=ch_manager,
            )
            summary = asyncio.run(
                _publish_batches(
                    batches,
                    table=table,
                    subject=subject_override or cfg.nats.subject,
                    address=cfg.nats.address,
                    dry_run=dry_run,
                )
            )
            yield iteration, summary
            time.sleep(poll_interval)


def _format_clickhouse_timestamp(value: str) -> str:
    dt = date_parser.isoparse(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def build_parameter_query(
    table: str, start: str, end: str, *, use_params: bool = True
) -> tuple[str, Sequence[str], dict[str, str] | None]:
    """
    Build a parameterized query for the parameter table.

    Args:
        table: ClickHouse table name
        start: Start timestamp (ISO format)
        end: End timestamp (ISO format)
        use_params: If True, return parameterized query with params dict.
                   If False, return legacy string-formatted query (deprecated).

    Returns:
        Tuple of (query, columns, params).
        params is None if use_params=False (legacy mode).
    """
    start_fmt = _format_clickhouse_timestamp(start)
    end_fmt = _format_clickhouse_timestamp(end)

    if use_params:
        # Safe parameterized query
        query = PARAMETER_QUERY_TEMPLATE.format(table=_quote_identifier(table))
        params = {"start": start_fmt, "end": end_fmt}
        return query, PARAMETER_COLUMNS, params
    else:
        # Legacy string formatting (deprecated - kept for backwards compatibility)
        logger.warning(
            "Using legacy string-formatted query. Consider using use_params=True for safety."
        )
        query = PARAMETER_QUERY.format(
            table=_quote_identifier(table),
            start=start_fmt,
            end=end_fmt,
        )
        return query, PARAMETER_COLUMNS, None


__all__ = [
    "export_clickhouse_table",
    "stream_clickhouse_table",
    "stream_clickhouse_table_async",
    "build_parameter_query",
    "PublishSummary",
    "ClickHouseConnectionManager",
    "DEFAULT_QUERY_SETTINGS",
]
