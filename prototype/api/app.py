"""FastAPI service exposing Pulsar prototype forecasts."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from pipeline import DemandPipeline, PipelineConfig

app = FastAPI(title="Pulsar Prototype API", version="0.1.0")


class ForecastResponse(BaseModel):
    city: str
    service_type: int
    hex_id: str
    horizon_min: int
    prediction_window_start: str
    predicted_demand: float
    active_drivers: float
    driver_gap: float
    surge_delta_percent: float
    confidence: float
    priority_score: float


class ZoneFeature(BaseModel):
    hex_id: str
    city: str
    service_type: int
    demand_30m: float
    demand_60m: float
    demand_90m: float
    surge_delta_percent: float
    average_confidence: float


class ForecastStore:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.cache_path = config.cache_path
        self.pipeline = DemandPipeline(config)
        self._rows: Optional[List[dict]] = None

    def read(self) -> List[dict]:
        if self.cache_path.exists():
            self._rows = self._load_from_disk()
        if not self._rows:
            self._rows = self.pipeline.forecast()
            self.pipeline.save_cache(self._rows)
        return self._rows

    def refresh(self) -> List[dict]:
        self._rows = self.pipeline.forecast()
        self.pipeline.save_cache(self._rows)
        return self._rows

    def _load_from_disk(self) -> List[dict]:
        import json

        return json.loads(self.cache_path.read_text())


@lru_cache
def store() -> ForecastStore:
    cfg_path = Path(__file__).resolve().parent.parent / "config.json"
    if cfg_path.exists():
        config = PipelineConfig.from_file(cfg_path)
    else:
        config = PipelineConfig()
    return ForecastStore(config)


@app.on_event("startup")
def _warm_store() -> None:
    store().read()


@app.get("/healthz", tags=["system"])
def health() -> dict:
    return {"status": "ok", "forecasts_cached": len(store().read())}


@app.get("/v1/pulsar/forecast", response_model=List[ForecastResponse])
def get_forecast(
    city: str = Query("Tehran"),
    service_type: int = Query(1, ge=1),
    horizon_min: Optional[int] = Query(
        None, description="Optional horizon filter, e.g. 30"
    ),
    limit: int = Query(10, gt=0, le=50),
) -> List[ForecastResponse]:
    rows = store().read()
    filtered = [
        row
        for row in rows
        if row["city"] == city
        and row["service_type"] == service_type
        and (horizon_min is None or row["horizon_min"] == horizon_min)
    ]
    if not filtered:
        raise HTTPException(status_code=404, detail="No forecast rows for selection")
    filtered.sort(key=lambda r: (r["horizon_min"], -r["priority_score"]))
    return [ForecastResponse(**row) for row in filtered[:limit]]


@app.get("/v1/pulsar/zones", response_model=List[ZoneFeature])
def get_zone_features(
    city: str = Query("Tehran"),
    service_type: int = Query(1, ge=1),
) -> List[ZoneFeature]:
    rows = store().read()
    df = {}
    for row in rows:
        if row["city"] != city or row["service_type"] != service_type:
            continue
        hex_id = row["hex_id"]
        bucket = df.setdefault(
            hex_id,
            {
                "hex_id": hex_id,
                "city": city,
                "service_type": service_type,
                "demand_30m": 0.0,
                "demand_60m": 0.0,
                "demand_90m": 0.0,
                "surge_sum": 0.0,
                "confidence_sum": 0.0,
                "count": 0,
            },
        )
        horizon_key = f"demand_{row['horizon_min']}m"
        bucket[horizon_key] = row["predicted_demand"]
        bucket["surge_sum"] += row["surge_delta_percent"]
        bucket["confidence_sum"] += row["confidence"]
        bucket["count"] += 1

    features = []
    for payload in df.values():
        count = max(payload["count"], 1)
        features.append(
            ZoneFeature(
                hex_id=payload["hex_id"],
                city=payload["city"],
                service_type=payload["service_type"],
                demand_30m=round(payload["demand_30m"], 2),
                demand_60m=round(payload["demand_60m"], 2),
                demand_90m=round(payload["demand_90m"], 2),
                surge_delta_percent=round(payload["surge_sum"] / count, 2),
                average_confidence=round(payload["confidence_sum"] / count, 2),
            )
        )
    if not features:
        raise HTTPException(status_code=404, detail="No zone summaries available")
    return features


@app.post("/v1/pulsar/refresh", tags=["admin"])
def refresh_cache() -> dict:
    rows = store().refresh()
    return {"rows": len(rows), "message": "cache refreshed"}
