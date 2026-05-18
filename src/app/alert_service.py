from kafka import KafkaConsumer
import json
import time
import logging
import requests

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("alert-service")

# --- CONFIG ---
KAFKA_SERVERS = "kafka:9092"
TOPICS = ["tpc_fraud_decisions", "tpc_alerts_aggregated"]

# simple rate limiting (seconds)
ALERT_COOLDOWN = 10
last_alert_time = 0

# your webhook here
SLACK_WEBHOOK = "REMOVED"

def should_alert():
    global last_alert_time
    now = time.time()
    if now - last_alert_time > ALERT_COOLDOWN:
        last_alert_time = now
        return True
    return False


def handle_instant_alert(data):
    """
    Per-event alert (low latency)
    """
    if data.get("decision") == "BLOCK":
        if should_alert():
            logger.warning(f"🚨 INSTANT ALERT: {data}")
            msg = f"📊 Instant alert: {data}"
            send_slack(msg)

def handle_aggregated_alert(data):
    """
    Aggregated alert (from Spark)
    """
    msg = f"📊 Fraud spike: {data}"
    logger.warning(f"📊 AGGREGATED ALERT: {data}")
    send_slack(msg)

def send_slack(message):
    try:
        requests.post(
            SLACK_WEBHOOK,
            json={"text": message},
            timeout=2
        )
    except Exception as e:
        print(f"Slack error: {e}")

def main():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="alert-service",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    print("Alert service started...", flush=True)

    for msg in consumer:
        topic = msg.topic
        data = msg.value

        try:
            if topic == "tpc_fraud_decisions":
                handle_instant_alert(data)

            elif topic == "tpc_alerts_aggregated":
                handle_aggregated_alert(data)

        except Exception as e:
            logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    main()