from __future__ import annotations

from importlib import metadata, resources
from typing import Any, Dict, List

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
    deployment_template_path = resources.files("pulsar_core").joinpath(
        "templates/deployment.html"
    )
    if not deployment_template_path.is_file():
        raise FileNotFoundError(f"template not found: {deployment_template_path}")
    deployment_template_html = deployment_template_path.read_text(encoding="utf-8")

    try:
        package_version = metadata.version("pulsar")
    except metadata.PackageNotFoundError:
        package_version = "0.0.0-dev"

    deployment_snapshot: Dict[str, Any] = {
        "version": package_version,
        "cache_dir": str(cfg.cache_dir),
        "period_duration_minutes": cfg.period_duration_minutes,
        "collect_duration_minutes": cfg.collect_duration_minutes,
        "mlflow_enabled": bool(cfg.mlflow_tracking_uri),
        "forecast": {
            "horizons": cfg.forecast.horizons,
            "service_types": cfg.forecast.service_types,
            "min_history_points": cfg.forecast.min_history_points,
            "max_history_points": cfg.forecast.max_history_points,
        },
        "clickhouse": {
            "host": cfg.clickhouse.host,
            "port": cfg.clickhouse.port,
            "database": cfg.clickhouse.database,
            "secure": cfg.clickhouse.secure,
        },
        "nats": {
            "address": cfg.nats.address,
            "subject": cfg.nats.subject,
        },
    }

    # Only include redis and rabbitmq metadata if they are configured
    if cfg.redis is not None:
        deployment_snapshot["redis"] = {
            "raw_slave": {
                "host": cfg.redis.raw_slave.host,
                "port": cfg.redis.raw_slave.port,
                "db": cfg.redis.raw_slave.db,
                "ssl": cfg.redis.raw_slave.ssl,
            },
            "prepared_slave": {
                "host": cfg.redis.prepared_slave.host,
                "port": cfg.redis.prepared_slave.port,
                "db": cfg.redis.prepared_slave.db,
                "ssl": cfg.redis.prepared_slave.ssl,
            },
        }

    if cfg.rabbitmq is not None:
        deployment_snapshot["rabbitmq"] = {
            "host": cfg.rabbitmq.host,
            "port": cfg.rabbitmq.port,
            "vhost": cfg.rabbitmq.vhost,
            "queues": cfg.rabbitmq.queues.dict(),
        }

    app = FastAPI(title="Pulsar Bridge API")

    def get_forecaster() -> SimpleForecaster:
        return forecaster

    @app.get("/", response_class=HTMLResponse)
    async def forecast_ui() -> str:
        return template_html

    @app.get("/deployment", response_class=HTMLResponse)
    async def deployment_ui() -> str:
        return deployment_template_html

    @app.get("/deployment/meta")
    async def deployment_meta() -> Dict[str, Any]:
        return deployment_snapshot

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
