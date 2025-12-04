# Offline CSV & Training Workflow

1. Place sanitized CSV snapshots (e.g., `19Nov2025.csv`) inside `data/`. These files are intentionally **not** tracked for security.
2. Build snapshots locally:
   ```bash
   python scripts/process_csv_data.py --config config.yaml --data-dir data --output cache
   ```
3. Train a model:

   ```bash
   # ElasticNet baseline
   python app.py --config config.yaml train --service-types 1 5 7

   # CNN surge forecaster
   python app.py --config config.yaml train --model-type cnn --service-types 1 5 7 --cnn-window 12 --cnn-epochs 10
   ```

4. Serve forecasts and inspect the UI:
   ```bash
   python app.py --config config.yaml api
   open http://localhost:8088/
   ```

The CSV files stay local. Do **not** commit them to GitHub.
