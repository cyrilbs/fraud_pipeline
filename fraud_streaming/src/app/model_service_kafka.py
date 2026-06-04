# model_service_kafka.py
from kafka import KafkaConsumer, KafkaProducer
import json
import joblib
import numpy as np
import os
import time

from app.observability import (
    observe_failed_message,
    observe_processed_message,
    observe_produced_message,
    start_metrics_server,
)

fraud_model_filename = "models/fraud_model.pkl"
fraud_input_topic = "tpc_fraud"
fraud_output_topic = "tpc_fraud_decisions"
fraud_threshold_model = 0.8
SERVICE_NAME = "model-service"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9101"))

start_metrics_server(SERVICE_NAME, METRICS_PORT)

consumer = KafkaConsumer(
    fraud_input_topic,
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="fraud-model",
)

producer = KafkaProducer(
    bootstrap_servers="kafka:9092", value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

model = joblib.load(fraud_model_filename)

print("Starting consumer...")
for msg in consumer:
    started_at = time.perf_counter()
    try:
        data = msg.value

        x = np.array([[data["amount"], data["country_risk"]]])
        prob = model.predict_proba(x)[0][1]

        decision = {
            "transaction_id": data["transaction_id"],
            "fraud_probability": float(prob),
            "timestamp": data["timestamp"],
            "decision": "BLOCK" if prob > fraud_threshold_model else "ALLOW",
        }
        print(f">> {decision}")
        producer.send(fraud_output_topic, decision)
        observe_produced_message(SERVICE_NAME)
        observe_processed_message(SERVICE_NAME, time.perf_counter() - started_at)
    except Exception:
        observe_failed_message(SERVICE_NAME)
        raise
