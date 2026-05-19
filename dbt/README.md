# Fraud analytics (dbt + DuckDB)

Query the local parquet lake written by `fraud_streaming` (`data/parquet/fraud_decisions`).

## Prerequisites

- Parquet data exists (run the streaming stack and process some events first)
- Docker (optional, recommended)

## With Docker

From the repo root, start the full stack once so the shared network exists:

```bash
docker compose up -d
```

Then from `dbt/`:

```bash
docker compose run --rm dbt debug
docker compose run --rm dbt deps
docker compose run --rm dbt run
```

## Local (without Docker)

```bash
python -m venv .venv && source .venv/bin/activate
pip install "dbt-core>=1.8" "dbt-duckdb>=1.8"
export DBT_PROFILES_DIR=.
dbt deps && dbt run
```

Point `stg_fraud_decisions.sql` at your parquet path if not using the container mount (`../fraud_streaming/data/parquet/...`).

## Models

| Model | Description |
|-------|-------------|
| `stg_fraud_decisions` | Reads hive-partitioned parquet from the streaming lake |
| `fraud_daily_summary` | Daily counts and average fraud score by decision |
