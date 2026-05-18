# model_service_kafka.py
from kafka import KafkaConsumer, KafkaProducer
import json
import joblib
import numpy as np

fraud_model_filename = "models/fraud_model.pkl"
fraud_input_topic = "tpc_fraud"
fraud_output_topic = "tpc_fraud_decisions"
fraud_threshold_model = 0.8

consumer = KafkaConsumer(
    fraud_input_topic,
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="fraud-model"
)

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

model = joblib.load(fraud_model_filename)

print("Starting consumer...")
for msg in consumer:
    data = msg.value

    x = np.array([[data["amount"], data["country_risk"]]])
    prob = model.predict_proba(x)[0][1]

    decision = {
        "transaction_id": data["transaction_id"],
        "fraud_probability": float(prob),
        "timestamp": data["timestamp"],
        "decision": "BLOCK" if prob > fraud_threshold_model else "ALLOW"
    }
    print(f">> {decision}")
    producer.send(fraud_output_topic, decision)