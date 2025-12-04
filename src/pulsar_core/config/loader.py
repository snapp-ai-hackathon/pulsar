from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, validator

DEFAULT_CONFIG_FILENAMES = ("config.yaml", "config.yml")
CONFIG_ENV_VAR = "PULSAR_CONFIG"


class RedisNode(BaseModel):
    host: str = "127.0.0.1"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    ssl: bool = False

    def url(self) -> str:
        auth = f":{self.password}@" if self.password else ""
        scheme = "rediss" if self.ssl else "redis"
        return f"{scheme}://{auth}{self.host}:{self.port}/{self.db}"


class RedisConfig(BaseModel):
    raw_master: RedisNode
    raw_slave: RedisNode
    prepared_master: RedisNode
    prepared_slave: RedisNode


class RabbitQueueNames(BaseModel):
    mru: str = "kandoo.mru"
    lru: str = "kandoo.lru"
    canary_mru: str = "kandoo.canary.mru"
    canary_lru: str = "kandoo.canary.lru"


class RabbitConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 5672
    user: str = "guest"
    password: str = "guest"
    vhost: str = "/"
    queues: RabbitQueueNames = Field(default_factory=RabbitQueueNames)


class ForecastConfig(BaseModel):
    horizons: List[int] = Field(default_factory=lambda: [30, 60, 90])
    min_history_points: int = 8
    max_history_points: int = 96
    city_ids: List[int] = Field(default_factory=lambda: [1])
    service_types: List[int] = Field(default_factory=lambda: [1, 2, 3])

    @validator("horizons", each_item=True)
    def _positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("horizon values must be positive minutes")
        return value


class ClickHouseConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 9000
    database: str = "default"
    user: str = "default"
    password: Optional[str] = None
    secure: bool = False
    settings: Dict[str, Any] = Field(default_factory=dict)

    @validator("port")
    def _valid_port(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ValueError("port must be between 1 and 65535")
        return v

    @validator("host")
    def _valid_host(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("host cannot be empty")
        return v.strip()

    @validator("database")
    def _valid_database(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("database cannot be empty")
        return v.strip()


class NatsConfig(BaseModel):
    address: str = "nats://127.0.0.1:4222"
    subject: str = "pulsar.clickhouse"


class PulsarConfig(BaseModel):
    # Make redis and rabbitmq optional so they are not required in config files
    redis: Optional[RedisConfig] = None
    rabbitmq: Optional[RabbitConfig] = None
    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)
    nats: NatsConfig = Field(default_factory=NatsConfig)
    forecast: ForecastConfig = Field(default_factory=ForecastConfig)
    cache_dir: Path = Path("cache")
    period_duration_minutes: float = 7.5
    collect_duration_minutes: float = 5.5
    mlflow_tracking_uri: Optional[str] = None
    mlflow_experiment: str = "pulsar-forecast"

    def ensure_cache_dir(self) -> Path:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        return self.cache_dir


def resolve_config_path(explicit_path: Path | str | None = None) -> Path:
    candidates = []
    if explicit_path is not None:
        candidates.append(Path(explicit_path))
    env_path = os.environ.get(CONFIG_ENV_VAR)
    if env_path:
        candidates.append(Path(env_path))
    if not explicit_path and not env_path:
        for name in DEFAULT_CONFIG_FILENAMES:
            candidates.append(Path(name))

    for candidate in candidates:
        if candidate.is_file():
            return candidate

    locations = [str(path) for path in candidates]
    raise FileNotFoundError(
        "Unable to find Pulsar config file. Provide --config, set PULSAR_CONFIG,"
        f" or create one of: {', '.join(DEFAULT_CONFIG_FILENAMES)}. Checked: {locations}"
    )


def load_config(path: Path | str | None = None) -> PulsarConfig:
    print("path: {}", path)
    config_path = resolve_config_path(path)
    print("config path: {}", config_path)
    read_text = config_path.read_text()
    print("read_text: {}", read_text)
    data = yaml.safe_load(read_text)
    print("data: {}", data)
    pulsar_conf = PulsarConfig.parse_obj(data)
    print("pulsar_conf: {}", pulsar_conf)
    return pulsar_conf
