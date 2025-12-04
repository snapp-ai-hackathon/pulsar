from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


def _to_seconds(duration_minutes: float) -> int:
    return int(duration_minutes * 60)


@dataclass(frozen=True)
class PeriodWindow:
    start: datetime
    end: datetime
    duration: timedelta
    collect_duration: timedelta

    def previous(self, n: int = 1) -> "PeriodWindow":
        delta = self.duration * n
        return PeriodWindow(
            start=self.start - delta,
            end=self.end - delta,
            duration=self.duration,
            collect_duration=self.collect_duration,
        )

    def string(self) -> str:
        return period_string(self.start, self.end)


def period_string(start: datetime, end: datetime) -> str:
    return f"{start.isoformat()}_{end.isoformat()}"


def with_duration(
    value: datetime,
    duration_minutes: float,
    collect_minutes: float,
) -> PeriodWindow:
    duration_seconds = _to_seconds(duration_minutes)
    epoch = int(value.replace(tzinfo=timezone.utc).timestamp())
    current_period_index = epoch // duration_seconds
    period_start = datetime.fromtimestamp(
        current_period_index * duration_seconds, tz=timezone.utc
    )
    period_end = period_start + timedelta(seconds=duration_seconds)
    return PeriodWindow(
        start=period_start,
        end=period_end,
        duration=timedelta(seconds=duration_seconds),
        collect_duration=timedelta(minutes=collect_minutes),
    )
