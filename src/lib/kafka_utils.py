from kafka import KafkaProducer
import json, time

def create_producer():
    while True:
        try:
            print("Connecting to Kafka...")
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka")
            return producer
        except Exception as e:
            print("Kafka not ready, retrying...", e)
            time.sleep(3)