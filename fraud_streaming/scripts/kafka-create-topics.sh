#!/bin/bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TOPICS=(tpc_fraud tpc_fraud_decisions tpc_alerts_aggregated)

echo "Waiting for Kafka at ${BOOTSTRAP}..."
until kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
  sleep 2
done

for topic in "${TOPICS[@]}"; do
  echo "Creating topic ${topic} (if missing)..."
  kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions 1 \
    --replication-factor 1
done

echo "Kafka topics:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list
