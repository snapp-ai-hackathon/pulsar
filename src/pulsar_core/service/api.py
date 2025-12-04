from __future__ import annotations

from importlib import resources
from typing import List

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from ..config import PulsarConfig
from ..models import SimpleForecaster
from ..store import TimeSeriesStore


def create_app(cfg: PulsarConfig) -> FastAPI:
    store = TimeSeriesStore(cfg.ensure_cache_dir() / "timeseries")
    forecaster = SimpleForecaster(store)
    template_path = resources.files("pulsar_core").joinpath("templates/forecast.html")
    if not template_path.is_file():
        raise FileNotFoundError(f"template not found: {template_path}")
    template_html = template_path.read_text(encoding="utf-8")

    app = FastAPI(title="Pulsar Bridge API")

    def get_forecaster() -> SimpleForecaster:
        return forecaster

    @app.get("/", response_class=HTMLResponse)
    async def forecast_ui() -> str:
        return template_html

    @app.get("/healthz")
    async def health():
        """Health check endpoint for monitoring and orchestration."""
        try:
            # Basic health check - verify store is accessible
            store_path = cfg.ensure_cache_dir() / "timeseries"
            return {
                "status": "ok",
                "store_path": str(store_path),
                "store_exists": store_path.exists(),
            }
        except Exception as exc:
            # Return degraded status but don't fail the health check
            # This allows the service to be marked as unhealthy but still accessible
            return {
                "status": "degraded",
                "error": str(exc),
            }

    @app.get("/forecast")
    async def forecast(
        hexagon: int = Query(..., description="H3 index as int"),
        service_type: int = Query(..., description="Snapp service type"),
        horizons: List[int] = Query([30, 60, 90]),
        fc: SimpleForecaster = Depends(get_forecaster),
    ):
        results = fc.forecast(hexagon, service_type, horizons)
        if not results:
            raise HTTPException(
                status_code=404, detail="not enough history for this hexagon"
            )
        return [item.as_dict() for item in results]

    return app
