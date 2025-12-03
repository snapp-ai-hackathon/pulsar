Pulsar – Smart Driver Distribution Prototype
===========================================

Pulsar is an AI co-pilot for Snapp drivers that predicts short-term demand spikes, recommends repositioning moves, and reports impact metrics back to Kandoo (Surge v2). This prototype is designed to sit alongside the existing `surge` stack and reuse its signals (Kandoo collectors, heatmap workers, CMQ publishers) while adding proactive forecasting and driver-facing intelligence.

## Solution Layers

1. **Data & Signals**
   - Consume Kandoo’s redis/clickhouse collectors (AR, PC, ride/get-price MRUs) plus trip telemetry from Kafka/CMQ.
   - Enrich with weather, events, and traffic feeds through a feature registry (Featureform/Feast ready).
   - Persist curated fact tables (15-min cadence) in ClickHouse/Mongo for fast reads.

2. **Forecasting Platform**
   - Hybrid Prophet + sequence model (LSTM/Temporal-Fusion) per city + service type + H3 resolution.
   - Sliding-window retraining every 6 hours with active learning hooks that incorporate operator overrides (freeze/reset) from Kandoo.
   - Geo-temporal attention to capture neighboring hexagon influence (krings) and day-of-week seasonality.

3. **Decision Engine**
   - Ranks hexagons by uplift + fairness constraints (driver density, fuel/time cost).
   - Generates move recommendations (target hex, ETA, surge delta, confidence) for individual drivers or cohorts.
   - Publishes “smart distribution” events back to CMQ / JetStream so downstream services (pricing, ops dashboards) remain in sync.

4. **Driver Experience**
   - Lightweight API feeding the driver app widget (React Native) and a standalone Pulsar web view (React + Mapbox GL).
   - Surfaces live heatmap, surge projections (30/60/90 min), and optimal paths with congestion-aware routing hints.
   - Provides explainer snippets (“Concert exit, +22% demand expected”) to build driver trust.

## Repository Additions

| Path | Description |
| --- | --- |
| `config.example.yaml` | Minimal bridge config that mirrors `surge-dev` Redis/Rabbit layout. |
| `pyproject.toml` / `uv.lock` | uv-managed dependency set and lockfile for the bridge runtime (FastAPI, redis, aio-pika, sklearn). |
| `src/pulsar_core` | Python package that speaks the same dialect as Kandoo (Redis key naming, Rabbit import tasks, period math). |
| `app.py` | CLI entrypoint to (a) stream scheduler tasks from Rabbit and persist feature snapshots, (b) expose a `/forecast` API. |
| `datasets/*.json` | Synthetic import tasks + hexagon metadata for offline tests. |
| `scripts/generate_sample_dataset.py` | Helper to create a full synthetic time-series + tasks without connecting to prod Redis/Rabbit. |
| `pulsar_core/models/trainer.py` | ML training pipeline (ElasticNet baseline) with optional MLflow logging. |
| `prototype/*` | Earlier self-contained notebook-style prototype (still useful for experimentation). |
| `docs/pulsar_roadmap.md` | Product + engineering roadmap. |
| `docs/pulsar_pitch_deck.md` | Slide-by-slide pitch outline & go-to-market strategy. |

## High-Level Data Flow

```
Kandoo collectors / CMQ  --->  Pulsar Feeder (Kafka Connect)  --->  Feature Store
                                                             \
                                                              -> Training jobs (Prophet + LSTM) -> Model registry

Driver telemetry / GPS   --->  Streaming join (Flink) --------/

Model registry + Feature store --> Real-time scoring service --> Pulsar API --> Driver app & Ops dashboards
                                                                |
                                                                -> Feedback loop to Kandoo (surge intents)
```

## Running the Bridge Runtime

```bash
cd pulsar
uv sync  # creates .venv with the locked dependencies
source .venv/bin/activate  # optional; `uv run …` works without activation
cp config.example.yaml config.yaml  # edit host/creds to match surge-dev

# 1) ساخت دیتاست تستی (اختیاری، اگر به Redis/Rabbit دسترسی ندارید)
uv run python scripts/generate_sample_dataset.py --config config.yaml --history 24
# 1-b) یا استفاده از فایل آماده
uv run pulsar --config config.yaml sync --task-file datasets/sample_import_tasks.json

# 2) Serve forecasts (API + mini UI)
uv run pulsar --config config.yaml api --host 0.0.0.0 --port 8088
curl "http://localhost:8088/forecast?hexagon=613280476251029503&service_type=1"
#    open http://localhost:8088/ در مرورگر تا داشبورد سبک را ببینی

# 3) Train ML model و ثبت در MLflow (اختیاری)
uv run pulsar --config config.yaml train --service-types 1 2 --alpha 0.2 --l1-ratio 0.05
```

The `sync` command connects to the same Rabbit queues (`kandoo.mru`, `kandoo.lru`, …) that `surge-dev` uses, parses `ImportTask` payloads, pulls Acceptance/Price Conversion signs from Redis, and writes rolling features to `cache/timeseries/*.parquet`. When running completely offline, use `scripts/generate_sample_dataset.py` to seed those parquet files without any infra. The `api` command then reads the series and produces 30/60/90 minute forecasts using the lightweight linear-trend model in `pulsar_core.models.SimpleForecaster`.

For ML training, set `mlflow_tracking_uri` (e.g., `http://mlflow.snapp.ir`) and `mlflow_experiment` inside `config.yaml`; otherwise the training command simply keeps the fitted model in-memory for experimentation.

The legacy `prototype/` folder is still available if you need the earlier CSV-based experiments (`pipeline.py`, `api/app.py`, etc.).

## Container Image

```
docker build -t pulsar .
docker run --rm \
  -p 8088:8088 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  pulsar --config /app/config.yaml api --host 0.0.0.0 --port 8088
```

The image installs the project with `uv sync --frozen`, so builds are reproducible with `uv.lock`.
On every push to `master`, GitHub Actions also publishes the image to GHCR at `ghcr.io/<owner>/<repo>`.
You can pull it via:

```
docker pull ghcr.io/<owner>/<repo>:latest
```

## Helm Deployment

Deploy Pulsar onto Kubernetes with the bundled chart:

```bash
helm upgrade --install pulsar charts/pulsar \
  --set image.repository=ghcr.io/<owner>/<repo> \
  --set image.tag=latest \
  --set-string config.contents="$(cat config.yaml)"
```

Key values in `charts/pulsar/values.yaml`:
- `image.repository`/`image.tag`: GHCR image the CI pipeline publishes.
- `config.contents`: in-cluster `config.yaml` rendered into a ConfigMap and mounted at `/etc/pulsar/config.yaml`.
- `service.port`/`ingress.*`: expose the FastAPI service.
- `args`: defaults to `pulsar --config /etc/pulsar/config.yaml api --host 0.0.0.0 --port 8088`; adjust for `sync` jobs or training runs.

The chart also supports custom env vars, probes, and imagePullSecrets for private GHCR orgs.

## Infra Integration Cheat Sheet

| Component | What to plug in | Where to edit |
| --- | --- | --- |
| **Redis (prepared + raw clusters)** | Same read-only endpoints that Go worker/collector use. We only read keys like `surge:ar:*`, `surge:pc:*`, `surge:factor:*`. | `config.yaml` → `redis.prepared_slave` (for factors) and `redis.raw_slave` (for AR/PC). |
| **RabbitMQ** | The queues fed by Kandoo scheduler: `kandoo.mru`, `kandoo.lru`, and optionally canary queues. Provide host/port/user/pass/vhost. | `config.yaml` → `rabbitmq.*` and `rabbitmq.queues.*`. |
| **MLflow (optional)** | Tracking server URI + experiment name used by pricing/ML team. Leave blank if you only need local training. | `config.yaml` → `mlflow_tracking_uri`, `mlflow_experiment`. |
| **API ingress** | Any HTTP load balancer (Argo Rollouts, K8s svc, etc.) sitting in front of `app.py --command api`. | Deployment manifests (not shipped here) should mount the same `config.yaml` and point to port `8088`. |

### Steps to replace config placeholders
1. Copy `config.example.yaml` → `config.yaml`.
2. Replace **all** `127.0.0.1` entries in the Redis/Rabbit sections with the internal endpoints. If TLS/password is required, add them there (the `RedisNode` struct supports `password` and `ssl: true`).
3. Optionally set `mlflow_tracking_uri` to the Snapp MLflow cluster so training runs are logged using the same convention as pricing.
4. Run:
   ```bash
   # ingest live data
   uv run pulsar --config config.yaml sync
   # expose the API/UI
   uv run pulsar --config config.yaml api --host 0.0.0.0 --port 8088
   ```
5. (Optional) Train and log a model:
   ```bash
   uv run pulsar --config config.yaml train --service-types 1 2 3
   ```

That’s everything a teammate needs in order to fork this repo (or wipe the previous one) and connect it to real Snapp infrastructure.

## Next Steps

1. Wire pipeline inputs to real Snapp infra (Kafka topics, Redis clusters described in Confluence).
2. Deploy the API via Argo Rollouts alongside `surge` services, fronted by the existing auth middlewares.
3. Embed the Pulsar widget into the Snapp Driver app and schedule an A/B trial vs control fleet to validate income uplift.

## Continuous Integration

The workflow in `.github/workflows/ci.yaml` uses `uv` to install dependencies, runs a lightweight import/bytecode check, and builds the Docker image. On non-PR events (pushes to `master`, manual runs, tags) the job logs in to GHCR with `GITHUB_TOKEN` and pushes multi-tagged images such as `latest`, branch names, and the commit SHA.
