from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence, Tuple

import mlflow
import mlflow.pytorch
import numpy as np
import pandas as pd
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset, random_split

from ..config import PulsarConfig
from ..store import TimeSeriesStore
from .trainer import TrainingResult


class SequenceWindowDataset(Dataset):
    """Torch dataset holding (sequence, target_vector)."""

    def __init__(self, inputs: np.ndarray, targets: np.ndarray):
        self.inputs = torch.from_numpy(inputs).float()
        self.targets = torch.from_numpy(targets).float()

    def __len__(self) -> int:
        return self.inputs.shape[0]

    def __getitem__(self, idx: int):
        return self.inputs[idx], self.targets[idx]


class TemporalCNN(nn.Module):
    def __init__(self, feature_dim: int, target_dim: int):
        super().__init__()
        hidden = 64
        self.conv1 = nn.Conv1d(feature_dim, hidden, kernel_size=3, padding=1)
        self.conv2 = nn.Conv1d(hidden, hidden, kernel_size=3, padding=1)
        self.relu = nn.ReLU()
        self.pool = nn.AdaptiveAvgPool1d(1)
        self.dropout = nn.Dropout(p=0.15)
        self.fc = nn.Linear(hidden, target_dim)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x shape: (batch, window, features)
        x = x.permute(0, 2, 1)  # -> (batch, features, window)
        x = self.relu(self.conv1(x))
        x = self.relu(self.conv2(x))
        x = self.pool(x).squeeze(-1)
        x = self.dropout(x)
        return self.fc(x)


@dataclass
class CNNTrainerConfig:
    window_size: int = 12
    batch_size: int = 128
    epochs: int = 25
    lr: float = 1e-3


class CNNTrainer:
    """Temporal CNN trainer that predicts future surge deltas."""

    feature_columns = [
        "acceptance_rate",
        "price_conversion",
        "demand_signal",
        "supply_signal",
        "surge_percent",
        "surge_absolute",
        "cumulative_surge_percent",
        "cumulative_surge_absolute",
    ]

    def __init__(self, cfg: PulsarConfig, tcfg: CNNTrainerConfig | None = None):
        self.cfg = cfg
        self.store = TimeSeriesStore(cfg.ensure_cache_dir() / "timeseries")
        self.horizons = cfg.forecast.horizons
        self.steps_per_horizon = self._horizon_steps(cfg.forecast.horizons)
        self.trainer_cfg = tcfg or CNNTrainerConfig()

    def _horizon_steps(self, horizons: Sequence[int]) -> List[int]:
        step_minutes = max(self.cfg.period_duration_minutes, 1e-6)
        steps = []
        for horizon in horizons:
            multiplier = max(1, int(round(horizon / step_minutes)))
            steps.append(multiplier)
        return steps

    def _load_sequences(
        self, service_types: List[int]
    ) -> Tuple[np.ndarray, np.ndarray, int]:
        cache_root = self.store.root
        if not cache_root.exists():
            raise ValueError("timeseries history not found; run data sync first")

        sequences: List[np.ndarray] = []
        targets: List[np.ndarray] = []
        unique_hex = set()
        max_step = max(self.steps_per_horizon)
        window = self.trainer_cfg.window_size

        for parquet_path in cache_root.glob("*.parquet"):
            parts = parquet_path.stem.split("_")
            if len(parts) != 2:
                continue
            hexagon = int(parts[0])
            service_type = int(parts[1])
            if service_types and service_type not in service_types:
                continue

            df = pd.read_parquet(parquet_path)
            for column in self.feature_columns:
                if column not in df.columns:
                    df[column] = 0.0
            df.sort_values("period_start", inplace=True)
            if len(df) < window + max_step + 1:
                continue

            subset = df[self.feature_columns].copy()
            subset = subset.fillna(0.0)
            values = subset.to_numpy(dtype=np.float32)

            surge_series = df["surge_percent"].fillna(0.0).to_numpy(dtype=np.float32)

            for idx in range(window, len(values) - max_step):
                seq = values[idx - window : idx]
                target_values = []
                for step in self.steps_per_horizon:
                    target_idx = idx + step
                    if target_idx >= len(surge_series):
                        break
                    target_values.append(surge_series[target_idx])
                if len(target_values) != len(self.steps_per_horizon):
                    continue
                sequences.append(seq)
                targets.append(np.array(target_values, dtype=np.float32))
                unique_hex.add((hexagon, service_type))

        if not sequences:
            raise ValueError(
                "no sequences found for the requested service types; ensure data exists"
            )

        inputs = np.stack(sequences)
        outputs = np.stack(targets)
        return inputs, outputs, len(unique_hex)

    def train(self, service_types: List[int]) -> TrainingResult:
        inputs, targets, unique_hex = self._load_sequences(service_types)
        dataset = SequenceWindowDataset(inputs, targets)
        total_samples = len(dataset)
        if total_samples < 100:
            raise ValueError(
                "not enough samples to train CNN model (need at least 100)"
            )

        train_size = int(total_samples * 0.8)
        val_size = total_samples - train_size
        train_dataset, val_dataset = random_split(dataset, [train_size, val_size])

        train_loader = DataLoader(
            train_dataset, batch_size=self.trainer_cfg.batch_size, shuffle=True
        )
        val_loader = DataLoader(val_dataset, batch_size=self.trainer_cfg.batch_size)

        feature_dim = inputs.shape[-1]
        target_dim = targets.shape[-1]
        model = TemporalCNN(feature_dim, target_dim)

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model.to(device)

        optimizer = torch.optim.Adam(model.parameters(), lr=self.trainer_cfg.lr)
        criterion = nn.MSELoss()

        for epoch in range(self.trainer_cfg.epochs):
            model.train()
            total_loss = 0.0
            for batch_inputs, batch_targets in train_loader:
                batch_inputs = batch_inputs.to(device)
                batch_targets = batch_targets.to(device)

                optimizer.zero_grad()
                preds = model(batch_inputs)
                loss = criterion(preds, batch_targets)
                loss.backward()
                optimizer.step()
                total_loss += loss.item() * batch_inputs.size(0)

            avg_train_loss = total_loss / train_size

            model.eval()
            with torch.no_grad():
                val_loss = 0.0
                for batch_inputs, batch_targets in val_loader:
                    batch_inputs = batch_inputs.to(device)
                    batch_targets = batch_targets.to(device)
                    preds = model(batch_inputs)
                    loss = criterion(preds, batch_targets)
                    val_loss += loss.item() * batch_inputs.size(0)
                avg_val_loss = val_loss / val_size

            print(
                f"[cnn] epoch {epoch + 1}/{self.trainer_cfg.epochs} train_loss={avg_train_loss:.4f} val_loss={avg_val_loss:.4f}"
            )

        # Evaluate MAE / RMSE on full dataset
        model.eval()
        with torch.no_grad():
            inputs_tensor = torch.from_numpy(inputs).float().to(device)
            preds = model(inputs_tensor).cpu().numpy()

        mae = float(np.mean(np.abs(preds - targets)))
        rmse = float(np.sqrt(np.mean((preds - targets) ** 2)))

        if self.cfg.mlflow_tracking_uri:
            mlflow.set_tracking_uri(self.cfg.mlflow_tracking_uri)
        mlflow.set_experiment(self.cfg.mlflow_experiment)

        with mlflow.start_run(run_name="cnn-surge"):
            mlflow.log_param("model_type", "cnn")
            mlflow.log_param("window_size", self.trainer_cfg.window_size)
            mlflow.log_param("batch_size", self.trainer_cfg.batch_size)
            mlflow.log_param("epochs", self.trainer_cfg.epochs)
            mlflow.log_param("forecast_horizons", self.horizons)
            mlflow.log_metric("mae", mae)
            mlflow.log_metric("rmse", rmse)
            mlflow.pytorch.log_model(model, artifact_path="model")
            model_uri = mlflow.active_run().info.artifact_uri + "/model"

        return TrainingResult(
            hexagons=unique_hex,
            rows=total_samples,
            mae=mae,
            rmse=rmse,
            model_uri=model_uri,
        )
