# Agent Instructions

## Project Overview

This repository contains a local fraud detection pipeline:

- `fraud_streaming/`: Kafka producer and consumers, scikit-learn model scoring, Spark Structured Streaming, Cassandra writes, and parquet lake output.
- `dbt/`: DuckDB-backed dbt models over the parquet lake written by the streaming job.
- Root `docker-compose.yml`: full local stack orchestration.

## Working Rules

- Keep changes scoped to the relevant subproject. Do not refactor both `fraud_streaming/` and `dbt/` unless the task explicitly crosses that boundary.
- Do not commit secrets. In particular, never commit Slack webhook URLs or local credentials.
- Treat generated/runtime artifacts as disposable unless the task is specifically about them:
  - `fraud_streaming/.checkpoint/`
  - `fraud_streaming/data/`
  - local virtual environments such as `.venv/` or `.venv-tests/`
- `fraud_streaming/models/fraud_model.pkl` is a binary model artifact. Regenerate it only when the model training or model contract changes.
- Preserve existing Docker Compose service names, Kafka topic names, and Cassandra schema names unless the task requires a migration.

## Python Streaming

Use these commands from `fraud_streaming/`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=src
pytest
```

Train or refresh the model with:

```bash
python src/app/train_model.py
```

Important modules:

- `src/app/fraud_producer.py`: emits fraud events to Kafka.
- `src/app/model_service_kafka.py`: consumes raw events, scores them, and emits decisions.
- `src/app/fraud_streaming.py`: Spark Structured Streaming job.
- `src/app/alert_service.py`: consumes decisions and sends alerts.
- `src/lib/kafka_utils.py`: shared Kafka helper code.

## dbt Analytics

Use these commands from `dbt/`:

```bash
docker compose run --rm dbt debug
docker compose run --rm dbt deps
docker compose run --rm dbt run
```

For local dbt without Docker:

```bash
python -m venv .venv
source .venv/bin/activate
pip install "dbt-core>=1.8" "dbt-duckdb>=1.8"
export DBT_PROFILES_DIR=.
dbt deps
dbt run
```

The dbt models expect parquet data under `fraud_streaming/data/parquet/fraud_decisions/`.

## Full Stack

From the repository root:

```bash
docker compose up -d
docker compose --profile demo up -d
```

Rebuild Python app service images after dependency or application changes:

```bash
docker compose build model-service alert-service fraud-producer
```

Kafka topics used by the stack:

- `tpc_fraud`
- `tpc_fraud_decisions`
- `tpc_alerts_aggregated`

## Validation

- For Python changes, run `pytest` from `fraud_streaming/`.
- For dbt changes, run `docker compose run --rm dbt run` from `dbt/`.
- For Docker or end-to-end pipeline changes, start the root Compose stack and verify the affected service logs.

