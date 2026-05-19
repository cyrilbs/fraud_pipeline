import time, uuid, random
from datetime import datetime, timezone
from lib.kafka_utils import create_producer

def get_random_weight_number(weights: list[int]) -> int:
    values = list(range(1, len(weights) + 1))
    return random.choices(values, weights=weights, k=1)[0]

sleep_weights = [1000, 1, 1, 5, 6, 5, 1, 1, 1, 1]

producer = create_producer()
while True:
    msg = {
        "transaction_id": uuid.uuid4().hex,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": 5000,
        #"amount": random.randint(1, 500),
        "country_risk": random.random()
    }
    producer.send("tpc_fraud", msg, msg["event_id"].encode())

    to_wait = get_random_weight_number(sleep_weights)
    print(f"Sent {msg} (waiting {to_wait} sec)")
    time.sleep(to_wait)
