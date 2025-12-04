from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Set

logger = logging.getLogger(__name__)


class TrainingTracker:
    """Track which dates have been trained to avoid retraining."""

    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.tracker_file = cache_dir / "training_tracker.json"
        self._trained_dates: Set[date] = set()
        self._load()

    def _load(self) -> None:
        """Load training tracker from file."""
        if self.tracker_file.exists():
            try:
                data = json.loads(self.tracker_file.read_text())
                # Convert date strings back to date objects
                self._trained_dates = {
                    date.fromisoformat(d) for d in data.get("trained_dates", [])
                }
                logger.info(
                    f"Loaded {len(self._trained_dates)} trained dates from tracker"
                )
            except Exception as exc:
                logger.warning(f"Failed to load training tracker: {exc}")
                self._trained_dates = set()

    def _save(self) -> None:
        """Save training tracker to file."""
        try:
            data = {
                "trained_dates": [d.isoformat() for d in sorted(self._trained_dates)],
                "last_updated": datetime.now().isoformat(),
            }
            self.tracker_file.write_text(json.dumps(data, indent=2))
            logger.info(f"Saved training tracker with {len(self._trained_dates)} dates")
        except Exception as exc:
            logger.error(f"Failed to save training tracker: {exc}")

    def is_trained(self, train_date: date) -> bool:
        """Check if a specific date has been trained."""
        return train_date in self._trained_dates

    def mark_trained(self, train_date: date) -> None:
        """Mark a date as trained."""
        self._trained_dates.add(train_date)
        self._save()

    def mark_trained_range(self, start_date: date, end_date: date) -> None:
        """Mark a range of dates as trained."""
        current = start_date
        while current <= end_date:
            self._trained_dates.add(current)
            # Move to next day
            from datetime import timedelta

            current += timedelta(days=1)
        self._save()

    def get_trained_dates(self) -> List[date]:
        """Get all trained dates."""
        return sorted(self._trained_dates)

    def get_missing_dates(self, start_date: date, end_date: date) -> List[date]:
        """Get dates in range that haven't been trained yet."""
        missing = []
        current = start_date
        while current <= end_date:
            if current not in self._trained_dates:
                missing.append(current)
            from datetime import timedelta

            current += timedelta(days=1)
        return missing

    def get_training_summary(self) -> Dict:
        """Get summary of training status."""
        if not self._trained_dates:
            return {
                "total_trained_dates": 0,
                "first_trained_date": None,
                "last_trained_date": None,
                "trained_dates": [],
            }

        sorted_dates = sorted(self._trained_dates)
        return {
            "total_trained_dates": len(self._trained_dates),
            "first_trained_date": sorted_dates[0].isoformat(),
            "last_trained_date": sorted_dates[-1].isoformat(),
            "trained_dates": [d.isoformat() for d in sorted_dates],
        }
