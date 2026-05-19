from kafka import KafkaProducer
import json
import os
import time

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def create_producer():
    while True:
        try:
            print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                api_version_auto_timeout_ms=30000,
                request_timeout_ms=30000,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Connected to Kafka")
            return producer
        except Exception as e:
            print("Kafka not ready, retrying...", e)
            time.sleep(3)