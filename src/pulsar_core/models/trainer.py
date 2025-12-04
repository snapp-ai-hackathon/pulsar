from __future__ import annotations

import csv
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import date
from typing import List, Optional

import mlflow
import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split

from ..config import PulsarConfig
from ..store import TimeSeriesStore
from .training_tracker import TrainingTracker

logger = logging.getLogger(__name__)


@dataclass
class TrainingResult:
    hexagons: int
    rows: int
    mae: float
    rmse: float
    model_uri: str


class MLTrainer:
    """Train a regression model on historical demand signals and log it to MLflow."""

    def __init__(self, cfg: PulsarConfig):
        self.cfg = cfg
        self.store = TimeSeriesStore(cfg.ensure_cache_dir() / "timeseries")
        self.tracker = TrainingTracker(cfg.ensure_cache_dir())

    def _load_dataset(self, service_types: List[int]) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        cache_root = self.store.root
        for parquet_path in cache_root.glob("*.parquet"):
            parts = parquet_path.stem.split("_")
            if len(parts) != 2:
                continue
            hexagon = int(parts[0])
            service_type = int(parts[1])
            if service_types and service_type not in service_types:
                continue
            df = pd.read_parquet(parquet_path)
            df["hexagon"] = hexagon
            df["service_type"] = service_type
            frames.append(df)
        if not frames:
            raise ValueError("no parquet history found; run sync first")
        frame = pd.concat(frames, ignore_index=True)
        frame["period_start"] = pd.to_datetime(frame["period_start"], errors="coerce")
        frame.sort_values(["hexagon", "service_type", "period_start"], inplace=True)
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

    def train(
        self,
        service_types: List[int],
        alpha: float = 0.3,
        l1_ratio: float = 0.1,
        train_date: Optional[date] = None,
        force: bool = False,
    ) -> TrainingResult:
        """
        Train model on historical data.

        Args:
            service_types: List of service types to train on
            alpha: ElasticNet alpha parameter
            l1_ratio: ElasticNet l1_ratio parameter
            train_date: Date to mark as trained (defaults to today)
            force: Force retraining even if date is already trained

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

        dataset = self._load_dataset(service_types)

        # Extract date range from dataset
        if not dataset.empty and "period_start" in dataset.columns:
            min_date = dataset["period_start"].min().date()
            max_date = dataset["period_start"].max().date()
        else:
            min_date = max_date = train_date

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

        # Log sample predictions vs actuals for inspection
        sample_size = min(10, len(preds))
        sample_indices = np.random.choice(len(preds), sample_size, replace=False)
        logger.info("Sample predictions vs actuals:")
        for idx in sample_indices:
            logger.info(
                f"  Prediction: {preds[idx]:.2f}, Actual: {y_test.iloc[idx]:.2f}, "
                f"Error: {abs(preds[idx] - y_test.iloc[idx]):.2f}"
            )

        if self.cfg.mlflow_tracking_uri:
            mlflow.set_tracking_uri(self.cfg.mlflow_tracking_uri)
        mlflow.set_experiment(self.cfg.mlflow_experiment)

        run_name = f"elasticnet-demand-{train_date.isoformat()}"
        with mlflow.start_run(run_name=run_name):
            mlflow.log_param("alpha", alpha)
            mlflow.log_param("l1_ratio", l1_ratio)
            mlflow.log_param("train_date", train_date.isoformat())
            mlflow.log_param("data_min_date", min_date.isoformat())
            mlflow.log_param("data_max_date", max_date.isoformat())
            mlflow.log_metric("mae", mae)
            mlflow.log_metric("rmse", rmse)
            mlflow.log_metric("hexagons", dataset["hexagon"].nunique())
            mlflow.log_metric("rows", len(dataset))
            # Log sample predictions as artifacts for inspection
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False
            ) as f:
                writer = csv.writer(f)
                writer.writerow(["actual", "predicted", "error"])
                for i in range(min(100, len(preds))):  # Log first 100 predictions
                    writer.writerow(
                        [
                            float(y_test.iloc[i]),
                            float(preds[i]),
                            float(abs(preds[i] - y_test.iloc[i])),
                        ]
                    )
                temp_path = f.name

            try:
                mlflow.log_artifact(temp_path, artifact_path="predictions")
            finally:
                os.unlink(temp_path)

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
