from kafka import KafkaConsumer
import json
import os
import time
import logging
import requests

from app.observability import (
    observe_alert_sent,
    observe_failed_message,
    observe_processed_message,
    start_metrics_server,
)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("alert-service")

# --- CONFIG ---
KAFKA_SERVERS = "kafka:9092"
TOPICS = ["tpc_fraud_decisions", "tpc_alerts_aggregated"]
SERVICE_NAME = "alert-service"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9102"))

# simple rate limiting (seconds)
ALERT_COOLDOWN = 10
last_alert_time = 0

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK", "").strip()

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
    if not SLACK_WEBHOOK or SLACK_WEBHOOK == "REMOVED":
        logger.warning("Slack webhook is not configured; skipping alert")
        return

    try:
        response = requests.post(
            SLACK_WEBHOOK,
            json={"text": message},
            timeout=2,
        )
        response.raise_for_status()
        observe_alert_sent(SERVICE_NAME)
    except Exception as e:
        observe_failed_message(SERVICE_NAME)
        print(f"Slack error: {e}")


def main():
    start_metrics_server(SERVICE_NAME, METRICS_PORT)

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
        started_at = time.perf_counter()
        topic = msg.topic
        data = msg.value

        try:
            if topic == "tpc_fraud_decisions":
                handle_instant_alert(data)

            elif topic == "tpc_alerts_aggregated":
                handle_aggregated_alert(data)

            observe_processed_message(SERVICE_NAME, time.perf_counter() - started_at)
        except Exception as e:
            observe_failed_message(SERVICE_NAME)
            logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    main()
