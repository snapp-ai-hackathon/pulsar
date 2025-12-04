from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import List, Optional

import mlflow
import nats
import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split

from ..config import PulsarConfig
from .training_tracker import TrainingTracker

logger = logging.getLogger(__name__)


@dataclass
class TrainingResult:
    hexagons: int
    rows: int
    mae: float
    rmse: float
    model_uri: str


class NatsMLTrainer:
    """
    Train a regression model on data consumed from NATS events.

    This trainer subscribes to NATS, collects data from events, and trains
    a model similar to MLTrainer but using streaming data instead of parquet files.
    """

    def __init__(self, cfg: PulsarConfig):
        self.cfg = cfg
        self.tracker = TrainingTracker(cfg.ensure_cache_dir())
        self._collected_data: List[dict] = []

    def _parse_nats_message(self, msg_data: bytes) -> List[dict]:
        """
        Parse a NATS message envelope and extract rows.

        Expected format:
        {
            "table": "...",
            "batch": 1,
            "row_count": 100,
            "rows": [...]
        }
        """
        try:
            envelope = json.loads(msg_data.decode("utf-8"))
            if "rows" in envelope:
                return envelope["rows"]
            return []
        except (json.JSONDecodeError, UnicodeDecodeError, KeyError) as exc:
            logger.warning(f"Failed to parse NATS message: {exc}")
            return []

    def _transform_to_training_format(self, rows: List[dict]) -> pd.DataFrame:
        """
        Transform NATS rows (from ClickHouse parameter table) into training format.

        Maps ClickHouse columns to training features:
        - hex_id -> hexagon
        - accept_rate -> acceptance_rate
        - price_cnvr -> price_conversion
        - Calculates demand_signal and supply_signal from available metrics
        """
        if not rows:
            return pd.DataFrame()

        frames: List[pd.DataFrame] = []
        for row in rows:
            try:
                # Extract key fields from ClickHouse parameter row
                hexagon = int(row.get("hex_id", 0))
                service_type = int(row.get("service_type", 0))
                city_id = int(row.get("city_id", 0))

                # Parse timestamps
                period_start_str = row.get("from")
                period_end_str = row.get("to")
                if not period_start_str or not period_end_str:
                    continue

                try:
                    period_start = pd.to_datetime(period_start_str)
                    period_end = pd.to_datetime(period_end_str)
                except (ValueError, TypeError):
                    continue

                # Extract metrics
                acceptance_rate = float(row.get("accept_rate", 0.0))
                price_conversion = float(row.get("price_cnvr", 0.0))

                # Calculate demand/supply signals (similar to SnapshotBuilder logic)
                # For demand: use surge metrics or fallback to acceptance-based estimate
                surge_percent = (
                    float(row.get("surge_percent", 0.0))
                    if row.get("surge_percent") is not None
                    else 0.0
                )
                surge_absolute = (
                    float(row.get("surge_absolute", 0.0))
                    if row.get("surge_absolute") is not None
                    else 0.0
                )

                # Estimate demand signal from surge or acceptance metrics
                # This is a simplified calculation - adjust based on your actual data model
                demand_signal = (
                    max(acceptance_rate * 100, surge_absolute)
                    if surge_absolute > 0
                    else acceptance_rate * 100
                )
                supply_signal = max(acceptance_rate * 100, 1.0)

                row_dict = {
                    "hexagon": hexagon,
                    "service_type": service_type,
                    "city_id": city_id,
                    "period_start": period_start,
                    "period_end": period_end,
                    "acceptance_rate": acceptance_rate,
                    "price_conversion": price_conversion,
                    "demand_signal": demand_signal,
                    "supply_signal": supply_signal,
                    "surge_percent": surge_percent,
                    "surge_absolute": surge_absolute,
                }
                frames.append(pd.DataFrame([row_dict]))
            except (ValueError, KeyError, TypeError) as exc:
                logger.debug(f"Skipping invalid row: {exc}")
                continue

        if not frames:
            return pd.DataFrame()

        frame = pd.concat(frames, ignore_index=True)
        frame["period_start"] = pd.to_datetime(frame["period_start"], errors="coerce")
        frame.sort_values(["hexagon", "service_type", "period_start"], inplace=True)

        # Create lag features (same as MLTrainer)
        frame["lag_demand"] = frame.groupby(["hexagon", "service_type"])[
            "demand_signal"
        ].shift(1)
        frame["lag_supply"] = frame.groupby(["hexagon", "service_type"])[
            "supply_signal"
        ].shift(1)
        frame["lag_acceptance"] = frame.groupby(["hexagon", "service_type"])[
            "acceptance_rate"
        ].shift(1)

        frame.dropna(
            subset=[
                "lag_demand",
                "lag_supply",
                "lag_acceptance",
                "price_conversion",
                "demand_signal",
            ],
            inplace=True,
        )

        return frame

    async def _collect_from_nats(
        self,
        subject: str,
        address: str,
        max_messages: Optional[int] = None,
        timeout_seconds: Optional[float] = None,
    ) -> List[dict]:
        """
        Subscribe to NATS and collect data until timeout or max_messages reached.

        Args:
            subject: NATS subject to subscribe to
            address: NATS server address
            max_messages: Maximum number of messages to collect (None = unlimited)
            timeout_seconds: Stop collecting after this many seconds (None = no timeout)

        Returns:
            List of collected row dictionaries
        """
        collected_rows: List[dict] = []
        message_count = 0
        start_time = asyncio.get_event_loop().time()

        async def message_handler(msg):
            nonlocal message_count, collected_rows
            rows = self._parse_nats_message(msg.data)
            collected_rows.extend(rows)
            message_count += 1
            logger.debug(
                f"Collected {len(rows)} rows from message {message_count} "
                f"(total rows: {len(collected_rows)})",
            )

        nc = await nats.connect(address)
        logger.info(
            f"Connected to NATS at {address}, subscribing to {subject}"
        )

        try:
            sub = await nc.subscribe(subject, cb=message_handler)
            logger.info("Subscribed to NATS, collecting data...")

            # Wait for messages until timeout or max_messages
            while True:
                if timeout_seconds is not None:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed >= timeout_seconds:
                        logger.info(
                            f"Timeout reached ({timeout_seconds}s), stopping collection. "
                            f"Collected {message_count} messages, "
                            f"{len(collected_rows)} rows"
                        )
                        break

                if max_messages is not None and message_count >= max_messages:
                    logger.info(
                        f"Reached max_messages ({max_messages}), stopping collection. "
                        f"Collected {len(collected_rows)} rows"
                    )
                    break

                await asyncio.sleep(0.1)  # Small sleep to avoid busy-waiting

        finally:
            await sub.unsubscribe()
            await nc.drain()
            logger.info(
                f"Disconnected from NATS. Total collected: {len(collected_rows)} rows",
            )

        return collected_rows

    def _load_dataset(
        self, service_types: List[int], collected_rows: List[dict]
    ) -> pd.DataFrame:
        """
        Transform collected NATS rows into training dataset.

        Args:
            service_types: Filter by service types (empty list = all)
            collected_rows: Raw rows collected from NATS

        Returns:
            DataFrame ready for training
        """
        if not collected_rows:
            raise ValueError(
                "no data collected from NATS; ensure NATS is publishing events"
            )

        # Filter by service_types if specified
        if service_types:
            collected_rows = [
                row
                for row in collected_rows
                if row.get("service_type") in service_types
            ]

        frame = self._transform_to_training_format(collected_rows)

        if frame.empty:
            raise ValueError("no valid training data after transformation")

        return frame

    async def train_async(
        self,
        service_types: List[int],
        alpha: float = 0.3,
        l1_ratio: float = 0.1,
        train_date: Optional[date] = None,
        force: bool = False,
        max_messages: Optional[int] = None,
        timeout_seconds: Optional[float] = 60.0,
        subject_override: Optional[str] = None,
    ) -> TrainingResult:
        """
        Train model on data collected from NATS events.

        Args:
            service_types: List of service types to train on
            alpha: ElasticNet alpha parameter
            l1_ratio: ElasticNet l1_ratio parameter
            train_date: Date to mark as trained (defaults to today)
            force: Force retraining even if date is already trained
            max_messages: Maximum NATS messages to collect (None = unlimited until timeout)
            timeout_seconds: Stop collecting after this many seconds
            subject_override: Override NATS subject from config

        Returns:
            TrainingResult with metrics and model URI
        """
        if train_date is None:
            train_date = date.today()

        # Check if already trained
        if not force and self.tracker.is_trained(train_date):
            raise ValueError(
                f"Date {train_date} has already been trained. Use --force to retrain."
            )

        # Collect data from NATS
        subject = subject_override or self.cfg.nats.subject
        logger.info(
            f"Collecting training data from NATS subject '{subject}' "
            f"(timeout={timeout_seconds}s, max_messages={max_messages})",
        )

        collected_rows = await self._collect_from_nats(
            subject=subject,
            address=self.cfg.nats.address,
            max_messages=max_messages,
            timeout_seconds=timeout_seconds,
        )

        # Transform to training dataset
        dataset = self._load_dataset(service_types, collected_rows)

        # Extract date range from dataset
        if not dataset.empty and "period_start" in dataset.columns:
            min_date = dataset["period_start"].min().date()
            max_date = dataset["period_start"].max().date()
        else:
            min_date = max_date = train_date

        # Train model (same logic as MLTrainer)
        features = dataset[
            ["lag_demand", "lag_supply", "lag_acceptance", "price_conversion"]
        ].astype(float)
        target = dataset["demand_signal"].astype(float)
        x_train, x_test, y_train, y_test = train_test_split(
            features, target, test_size=0.2, shuffle=True
        )

        model = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        model.fit(x_train, y_train)

        preds = model.predict(x_test)
        mae = float(mean_absolute_error(y_test, preds))
        rmse = float(np.sqrt(mean_squared_error(y_test, preds)))

        if self.cfg.mlflow_tracking_uri:
            mlflow.set_tracking_uri(self.cfg.mlflow_tracking_uri)
        mlflow.set_experiment(self.cfg.mlflow_experiment)

        run_name = f"nats-elasticnet-demand-{train_date.isoformat()}"
        with mlflow.start_run(run_name=run_name):
            mlflow.log_param("alpha", alpha)
            mlflow.log_param("l1_ratio", l1_ratio)
            mlflow.log_param("train_date", train_date.isoformat())
            mlflow.log_param("data_min_date", min_date.isoformat())
            mlflow.log_param("data_max_date", max_date.isoformat())
            mlflow.log_param("data_source", "nats")
            mlflow.log_metric("mae", mae)
            mlflow.log_metric("rmse", rmse)
            mlflow.log_metric("hexagons", dataset["hexagon"].nunique())
            mlflow.log_metric("rows", len(dataset))
            model_info = mlflow.sklearn.log_model(model, artifact_path="model")
            model_uri = model_info.model_uri

        # Mark as trained
        self.tracker.mark_trained(train_date)

        return TrainingResult(
            hexagons=dataset["hexagon"].nunique(),
            rows=len(dataset),
            mae=mae,
            rmse=rmse,
            model_uri=model_uri,
        )

    def train(
        self,
        service_types: List[int],
        alpha: float = 0.3,
        l1_ratio: float = 0.1,
        train_date: Optional[date] = None,
        force: bool = False,
        max_messages: Optional[int] = None,
        timeout_seconds: Optional[float] = 60.0,
        subject_override: Optional[str] = None,
    ) -> TrainingResult:
        """
        Synchronous wrapper for train_async.

        See train_async for parameter documentation.
        """
        return asyncio.run(
            self.train_async(
                service_types=service_types,
                alpha=alpha,
                l1_ratio=l1_ratio,
                train_date=train_date,
                force=force,
                max_messages=max_messages,
                timeout_seconds=timeout_seconds,
                subject_override=subject_override,
            )
        )
