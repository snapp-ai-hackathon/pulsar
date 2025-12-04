from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import List, Dict, Tuple

import pandas as pd

from ..features import HexagonSnapshot


class TimeSeriesStore:
    """Lightweight parquet-backed store for hexagon level features."""

    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, hexagon: int, service_type: int) -> Path:
        return self.root / f"{hexagon}_{service_type}.parquet"

    def append(self, snapshot: HexagonSnapshot) -> None:
        """Append a single snapshot (kept for backward compatibility).

        Internally this delegates to append_many so that high-volume writers
        can use an efficient, batched code path.
        """
        self.append_many([snapshot])

    def append_many(self, snapshots: List[HexagonSnapshot]) -> None:
        """Efficiently append many snapshots.

        This groups snapshots by (hexagon, service_type) so that for each
        parquet file we:
        - Read existing data at most once
        - Concatenate in-memory rows once
        - Write back a single sorted, de-duplicated frame

        This avoids quadratic behavior when ingesting millions of rows.
        """
        if not snapshots:
            return

        # Group by target parquet path
        grouped: Dict[Path, List[HexagonSnapshot]] = {}
        for snap in snapshots:
            path = self._path(snap.hexagon, snap.service_type)
            grouped.setdefault(path, []).append(snap)

        for path, snaps in grouped.items():
            # Load existing frame if present
            existing = None
            prev_cum_percent = 0.0
            prev_cum_absolute = 0.0
            if path.exists():
                existing = pd.read_parquet(path)
                if not existing.empty:
                    existing["period_start"] = pd.to_datetime(existing["period_start"])
                    if "cumulative_surge_percent" in existing.columns:
                        prev_cum_percent = float(
                            existing["cumulative_surge_percent"].iloc[-1]
                        )
                    if "cumulative_surge_absolute" in existing.columns:
                        prev_cum_absolute = float(
                            existing["cumulative_surge_absolute"].iloc[-1]
                        )

            # Build new rows, updating cumulative fields incrementally
            rows: List[Dict] = []
            for snap in snaps:
                row = snap.as_dict()
                # Normalize and fill basic surge fields
                surge_percent = row.get("surge_percent") or 0.0
                surge_absolute = row.get("surge_absolute") or 0.0
                row["surge_percent"] = float(surge_percent)
                row["surge_absolute"] = float(surge_absolute)

                # Update cumulative surge percent
                if row.get("cumulative_surge_percent") is None:
                    row["cumulative_surge_percent"] = (
                        prev_cum_percent + row["surge_percent"]
                    )
                else:
                    # Preserve explicit values, only filling missing ones
                    if pd.isna(row["cumulative_surge_percent"]):
                        row["cumulative_surge_percent"] = (
                            prev_cum_percent + row["surge_percent"]
                        )
                prev_cum_percent = float(row["cumulative_surge_percent"])

                # Update cumulative surge absolute
                if row.get("cumulative_surge_absolute") is None:
                    row["cumulative_surge_absolute"] = (
                        prev_cum_absolute + row["surge_absolute"]
                    )
                else:
                    if pd.isna(row["cumulative_surge_absolute"]):
                        row["cumulative_surge_absolute"] = (
                            prev_cum_absolute + row["surge_absolute"]
                        )
                prev_cum_absolute = float(row["cumulative_surge_absolute"])

                rows.append(row)

            frame = pd.DataFrame(rows)
            if frame.empty:
                continue

            frame["period_start"] = pd.to_datetime(frame["period_start"])

            to_concat = []
            if (
                existing is not None
                and not existing.empty
                and not existing.isna().all().all()
            ):
                to_concat.append(existing)
            if not frame.empty and not frame.isna().all().all():
                to_concat.append(frame)

            if to_concat:
                frame_out = pd.concat(to_concat, ignore_index=True).drop_duplicates(
                    subset=["period_start"]
                )
            else:
                frame_out = frame

            frame_out.sort_values("period_start", inplace=True)
            frame_out.to_parquet(path, index=False)

    def history(self, hexagon: int, service_type: int, limit: int = 96) -> pd.DataFrame:
        path = self._path(hexagon, service_type)
        if not path.exists():
            return pd.DataFrame()
        frame = pd.read_parquet(path).tail(limit)
        frame["period_start"] = pd.to_datetime(frame["period_start"])
        frame.set_index("period_start", inplace=True)
        return frame

    def latest(self, hexagon: int, service_type: int) -> List[HexagonSnapshot]:
        frame = self.history(hexagon, service_type, limit=1)
        if frame.empty:
            return []
        row = frame.reset_index().iloc[0].to_dict()
        return [
            HexagonSnapshot(
                hexagon=hexagon,
                service_type=service_type,
                city_id=row["city_id"],
                period_start=row["period_start"],
                period_end=pd.to_datetime(row["period_end"]),
                acceptance_rate=row["acceptance_rate"],
                price_conversion=row["price_conversion"],
                demand_signal=row["demand_signal"],
                supply_signal=row["supply_signal"],
                rule_sheet_id=row.get("rule_sheet_id"),
                surge_percent=row.get("surge_percent"),
                surge_absolute=row.get("surge_absolute"),
                cumulative_surge_percent=row.get("cumulative_surge_percent"),
                cumulative_surge_absolute=row.get("cumulative_surge_absolute"),
            )
        ]
