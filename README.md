# Fraud Pipeline

Real-time fraud detection app and local analytics on the streaming parquet lake. 
Stack: Kafka, Spark Structured Streaming, scikit-learn, Cassandra, Parquet, DuckDB, dbt, Prometheus, Grafana

## Context

Use case: monitoring potentially fraudulent transactions
Business need: quickly detect risky transactions
Two levels of response:
transaction-level decision: ALLOW or BLOCK
aggregated alert: fraud spike over a micro-window
Key concerns: low latency, traceability, historical analysis, supervision

## Architecture

![Architecture diagram](archi.png)

Step 1: generate a JSON transaction
Step 2: publish it to tpc_fraud
Step 3: score it with ML and publish to tpc_fraud_decisions
Step 4: process it with Spark every 5 seconds
Step 5: store BLOCK decisions in Cassandra
Step 6: store all decisions in Parquet
Step 7: publish alerts to tpc_alerts_aggregated
Step 8: Slack, dbt, Grafana

## Technical choices: batch versus streaming

![Batch versus streaming diagram](docs/images/archi.png)

## Technical choices: Kafka

![Kafka technical choice diagram](docs/images/archi.png)

## Technical choices: Spark

![Spark technical choice diagram](docs/images/archi.png)

## Technical choices: Parquet

![Parquet technical choice diagram](docs/images/archi.png)

Parquet is a columnar format
Benefits:
efficient compression
selective column reads
embedded schema
compatible with Spark, DuckDB, dbt, DBeaver
In this project:
output: /streaming/data/parquet/fraud_decisions
partitioning by event_date
local analytics usage

## ML scoring service

![ML scoring service diagram](docs/images/archi.png)

File: model_service_kafka.py
Loads models/fraud_model.pkl with joblib
Features used:
amount
country_risk
Model: RandomForestClassifier
Threshold: fraud_threshold_model = 0.8
Output: BLOCK if probability > 0.8, otherwise ALLOW

## Technical choices: Cassandra

![Cassandra technical choice diagram](docs/images/archi.png)

## Technical choices: Prometheus and Grafana

![Prometheus and Grafana technical choice diagram](docs/images/archi.png)

## Project

```
fraud_pipeline/
├── docker-compose.yml       # Full stack (infra + streaming apps)
├── README.md                # Main project documentation
├── docs/images/archi.png    # Architecture and technical choice diagrams
├── fraud_streaming/         # Kafka, Spark, model, alerts, parquet lake
│   ├── src/app/             # Producer, model service, Spark job, alerts, observability
│   ├── src/lib/             # Shared helpers
│   ├── models/              # Trained fraud model artifact
│   ├── scripts/             # Kafka topic creation script
│   ├── tests/               # Unit tests
│   ├── docker-compose-kafka-cassandra.yml
│   ├── docker-compose-spark.yml
│   └── docker-compose.yml   # App services only (modular)
├── dbt/                     # DuckDB + dbt on local parquet
│   ├── models/staging/      # Staging models over parquet data
│   ├── models/marts/        # Analytics marts
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── docker-compose.yml
├── monitoring/              # Prometheus and Grafana provisioning
│   ├── prometheus/
│   └── grafana/
├── docs/                    # Presentation and generated documentation images
└── scripts/                 # Utility scripts, including presentation generation
```

## Starting everything

### 1. Train the fraud model to produce fraud_model.pkl

```bash
cd fraud_streaming
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python src/app/train_model.py
cd ..
```

### 2. Start the full stack

From the repository root:

```bash
docker compose up -d
docker compose --profile demo up -d   # optional demo producer
```

- Spark UI: http://localhost:8080 (master), http://localhost:8081 (worker)
- Kafka: `localhost:9092`
- Cassandra: `localhost:9042`
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

Note about docker image:
Python app services (`model-service`, `alert-service`, `fraud-producer`) now use
the image built from `fraud_streaming/src/app/Dockerfile` for faster startup
(dependencies preinstalled).

To rebuild when Python code changes:
```bash
docker compose build model-service
```

Note about starting producer container:
to run only the producer without Compose (Kafka and model-service must already be running):

```bash
docker run --rm -it \
  --name kafka-fraud-producer \
  --network data-platform-net \
  -v "$PWD/fraud_streaming/src:/app" \
  -w /app \
  -e PYTHONPATH=/app \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  python:3.11 \
  bash -c "pip install kafka-python && python app/fraud_producer.py"
```

### 3. Checks

To check that tpc_fraud is correctly populated into kafka:

```bash
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic tpc_fraud --from-beginning
```

Cassandra quick checks:

Use `cqlsh` in the Cassandra container to verify schema is created:

```bash
docker exec -it cassandra cqlsh

desc keyspaces;
USE mykeyspace;

desc tables;
desc table fraud;

select count(*) from fraud;
```

To check Spark:
```bash
cd fraud_streaming
export PYTHONPATH=src
python src/app/validate_spark.py
```

### 4. Slack alerts

Create a Slack app first (for example `fraud_detection`) at
[https://api.slack.com/apps](https://api.slack.com/apps), then create an Incoming
Webhook URL for your channel.
Slack web client is available at [https://app.slack.com/client](https://app.slack.com/client).

Set `SLACK_WEBHOOK` in your shell or a local `.env` file before starting
`alert-service`.

```bash
export SLACK_WEBHOOK="https://hooks.slack.com/services/..."
docker compose up -d --force-recreate alert-service
```

To disable Slack if needed:

```bash
unset SLACK_WEBHOOK
docker compose up -d --force-recreate alert-service
```

### 5. Explore parquet with dbt

After checking the stream has written data to `fraud_streaming/data/parquet/` we can start Dbt:

```bash
cd dbt
docker compose run --rm dbt deps
docker compose run --rm dbt run
```

### 6. Observability

The root Compose stack includes Prometheus and Grafana. Prometheus scrapes the Python app services on these metrics endpoints:

| Service | Metrics URL |
|---------|-------------|
| `model-service` | http://localhost:9101/metrics |
| `alert-service` | http://localhost:9102/metrics |
| `fraud-producer` | http://localhost:9103/metrics |

Open Prometheus at http://localhost:9090 to query metrics such as `fraud_processed_messages_total`, `fraud_produced_messages_total`, `fraud_failed_messages_total`, and `fraud_message_processing_latency_seconds`.

Open Grafana at http://localhost:3000 and sign in with `admin` / `admin`.*
The `Fraud Pipeline` dashboard is provisioned automatically under the `Fraud Pipeline` folder.

![Fraud Pipeline Grafana dashboard](docs/images/grafana-fraud-pipeline-dashboard.png)

### 7.Stopping services

```bash
docker compose --profile demo down
docker compose down
```

## Technical choices: Spark in depth

## Limitations

## Improvements

## Conclusion

## Troubleshooting

### Spark: `UnknownTopicOrPartitionException` on `tpc_fraud_decisions`

Spark starts before the Kafka topic exists. The stack now runs `kafka-init` to create `tpc_fraud`, `tpc_fraud_decisions`, and `tpc_alerts_aggregated` before `spark-driver`.

If you reset Kafka or still see this error, clear the streaming checkpoint and recreate the driver:

```bash
docker compose stop spark-driver
rm -rf fraud_streaming/.checkpoint
docker compose up -d kafka-init
docker compose up -d spark-driver
```

Ensure the demo producer (or model service) is sending events so `tpc_fraud_decisions` receives data.

### Spark: Kafka offset was changed or data may have been lost

This happens when Spark's checkpoint still contains old Kafka offsets but the Kafka topic was reset, deleted, or recreated. The local Spark job defaults `SPARK_FAIL_ON_DATA_LOSS=false` so the stream can recover from this during development, but clearing the stale checkpoint is the clean reset:

```bash
docker compose stop spark-driver
rm -rf fraud_streaming/.checkpoint/fraud_decisions
docker compose up -d spark-driver
```

For stricter production-like behavior, set `SPARK_FAIL_ON_DATA_LOSS=true` for `spark-driver`.

### Spark: `Mkdirs failed` when writing parquet

Parquet files are written by Spark **executors** on `spark-worker`, not only on `spark-driver`. The worker must mount `fraud_streaming` at `/streaming` (configured in `docker-compose.yml`). After changing mounts, recreate the worker and driver:

```bash
docker compose up -d --force-recreate spark-worker spark-driver
```
