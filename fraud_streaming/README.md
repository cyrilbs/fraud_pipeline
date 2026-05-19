# Fraud streaming

Real-time fraud detection: Kafka ingestion, scikit-learn scoring, Spark Structured Streaming, Cassandra persistence, and a parquet lake for analytics.

## Train the model

From this directory:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python src/app/train_model.py
```

Writes `models/fraud_model.pkl`.

## Run with the full stack

From the **repository root**:

```bash
docker compose up -d
docker compose --profile demo up -d
```

## Run app services only

Infrastructure must already be running (from repo root or modular compose files). Create the network once if needed:

```bash
docker network create data-platform-net
```

Modular startup from `fraud_streaming/`:

```bash
docker compose -f docker-compose-kafka-cassandra.yml up -d
docker compose -f docker-compose-spark.yml up -d
docker compose up -d
```

Parquet output: `data/parquet/fraud_decisions/` (consumed by `../dbt`).
