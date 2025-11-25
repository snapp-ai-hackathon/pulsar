Prototype Structure
===================

This folder contains runnable stubs to demonstrate Pulsar’s data pipeline and API.

## Files

- `pipeline.py`: loads trip signals (sample CSV), aggregates by H3 hexagon + service type, and produces 30/60/90 min demand & surge forecasts.
- `api/app.py`: FastAPI server that serves ranked forecasts and zone summaries.
- `sample_data/trips.csv`: synthetic slice of Tehran rides + price checks.
- `config.json`: tweak intervals, H3 resolution, horizons, file paths.
- `requirements.txt`: Python dependencies.

## Quickstart

```bash
cd pulsar/prototype
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python pipeline.py --generate-cache
uvicorn api.app:app --reload
```

### Sample Calls

```
curl "http://localhost:8000/v1/pulsar/forecast?city=Tehran&service_type=1&horizon_min=30"
curl "http://localhost:8000/v1/pulsar/zones?city=Tehran&service_type=2"
curl -X POST "http://localhost:8000/v1/pulsar/refresh"
```

### Replacing the Data Source

Update `config.json` to point `sample_path` at:
- A CSV export from ClickHouse.
- A Parquet dump from the Redis collectors (using Spark/Flink).
- A live Kafka consumer script that writes to disk for prototyping.

The pipeline operates on any table that provides:

```
timestamp, lat, lng, city, service_type, event_type,
status, fare, driver_id, passenger_id
```

To integrate with production, swap the `load()` method for streaming ingestion and persist forecasts back into ClickHouse / Mongo before broadcasting through CMQ.

