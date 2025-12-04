from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Tuple

import redis

from ..periods import PeriodWindow


def acceptance_rate_rr_key(
    period: PeriodWindow, hexagon: int, service_type: int
) -> str:
    return f"surge:ar:rr:period:{period.string()}:hex:{hexagon}:st:{service_type}:set"


def acceptance_rate_ra_key(
    period: PeriodWindow, hexagon: int, service_type: int
) -> str:
    return f"surge:ar:ra:period:{period.string()}:hex:{hexagon}:st:{service_type}:set"


def price_conversion_rr_key(
    period: PeriodWindow, hexagon: int, service_type: int
) -> str:
    return f"surge:pc:rr:period:{period.string()}:hex:{hexagon}:st:{service_type}:set"


def price_conversion_gp_key(
    period: PeriodWindow, hexagon: int, service_category: int
) -> str:
    return (
        f"surge:pc:gp:period:{period.string()}:hex:{hexagon}:sc:{service_category}:set"
    )


def recently_period_key(period: PeriodWindow) -> str:
    return f"surge:period:mru:hexagons:{period.string()}:type"


@dataclass
class RedisSignalLoader:
    client: redis.Redis

    def _zrange_all(self, key: str) -> Iterable[str]:
        try:
            return self.client.zrevrange(key, 0, -1)
        except redis.RedisError:
            return []

    def recently_hexagons(self, period: PeriodWindow) -> Iterable[Tuple[int, int]]:
        members = self._zrange_all(recently_period_key(period))
        for member in members:
            payload = member.decode("utf-8")
            try:
                hexagon, service_type, *_ = payload.split(":")
                yield int(hexagon), int(service_type)
            except ValueError:
                continue

    def _scard(self, key: str) -> int:
        try:
            return int(self.client.scard(key))
        except redis.RedisError:
            return 0

    def acceptance_metrics(
        self, period: PeriodWindow, hexagon: int, service_type: int
    ) -> Dict[str, int]:
        requests = self._scard(acceptance_rate_rr_key(period, hexagon, service_type))
        accepts = self._scard(acceptance_rate_ra_key(period, hexagon, service_type))
        return {"requests": requests, "accepts": accepts}

    def price_metrics(
        self,
        period: PeriodWindow,
        hexagon: int,
        service_type: int,
        service_category: int,
    ) -> Dict[str, int]:
        ride_requests = self._scard(
            price_conversion_rr_key(period, hexagon, service_type)
        )
        get_prices = self._scard(
            price_conversion_gp_key(period, hexagon, service_category)
        )
        return {"ride_requests": ride_requests, "get_prices": get_prices}
