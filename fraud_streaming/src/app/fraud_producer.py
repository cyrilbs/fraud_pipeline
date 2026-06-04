import os
import random
import time
import uuid
from datetime import datetime, timezone

from app.observability import (
    observe_failed_message,
    observe_produced_message,
    start_metrics_server,
)
from lib.kafka_utils import create_producer

SERVICE_NAME = "fraud-producer"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9103"))


def get_random_weight_number(weights: list[int]) -> int:
    values = list(range(1, len(weights) + 1))
    return random.choices(values, weights=weights, k=1)[0]


def build_message() -> dict:
    return {
        "transaction_id": uuid.uuid4().hex,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": 5000,
        # "amount": random.randint(1, 500),
        "country_risk": random.random(),
    }


def run_forever():
    start_metrics_server(SERVICE_NAME, METRICS_PORT)

    sleep_weights = [1000, 1, 1, 5, 6, 5, 1, 1, 1, 1]
    producer = create_producer()

    while True:
        msg = build_message()
        try:
            producer.send("tpc_fraud", msg, msg["event_id"].encode())
            observe_produced_message(SERVICE_NAME)
        except Exception:
            observe_failed_message(SERVICE_NAME)
            raise

        to_wait = get_random_weight_number(sleep_weights)
        print(f"Sent {msg} (waiting {to_wait} sec)")
        time.sleep(to_wait)


if __name__ == "__main__":
    run_forever()
