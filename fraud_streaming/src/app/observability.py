import logging
import time
from typing import Optional, Set

from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)

SERVICE_LABEL = ["service"]

PROCESSED_MESSAGES = Counter(
    "fraud_processed_messages_total",
    "Total messages processed by a service.",
    SERVICE_LABEL,
)
PRODUCED_MESSAGES = Counter(
    "fraud_produced_messages_total",
    "Total messages produced by a service.",
    SERVICE_LABEL,
)
FAILED_MESSAGES = Counter(
    "fraud_failed_messages_total",
    "Total messages that failed processing or delivery.",
    SERVICE_LABEL,
)
ALERTS_SENT = Counter(
    "fraud_alerts_sent_total",
    "Total alerts sent by the alert service.",
    SERVICE_LABEL,
)
MESSAGE_PROCESSING_LATENCY = Histogram(
    "fraud_message_processing_latency_seconds",
    "Message processing latency in seconds.",
    SERVICE_LABEL,
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
LAST_PROCESSED_TIMESTAMP = Gauge(
    "fraud_last_processed_timestamp_seconds",
    "Unix timestamp for the last message processed by a service.",
    SERVICE_LABEL,
)
SERVICE_HEALTH = Gauge(
    "fraud_service_health",
    "Service health flag. 1 means healthy, 0 means unhealthy.",
    SERVICE_LABEL,
)

_started_ports: Set[int] = set()


def start_metrics_server(service_name: str, port: int):
    """Start a Prometheus metrics HTTP server for a service."""
    SERVICE_HEALTH.labels(service=service_name).set(1)

    if port in _started_ports:
        return None

    try:
        server = start_http_server(port)
        _started_ports.add(port)
        logger.info("Metrics server started for %s on port %s", service_name, port)
        return server
    except OSError:
        logger.exception("Could not start metrics server for %s on port %s", service_name, port)
        SERVICE_HEALTH.labels(service=service_name).set(0)
        return None


def observe_processed_message(service_name: str, latency_seconds: Optional[float] = None) -> None:
    PROCESSED_MESSAGES.labels(service=service_name).inc()
    LAST_PROCESSED_TIMESTAMP.labels(service=service_name).set(time.time())
    if latency_seconds is not None:
        MESSAGE_PROCESSING_LATENCY.labels(service=service_name).observe(latency_seconds)


def observe_produced_message(service_name: str) -> None:
    PRODUCED_MESSAGES.labels(service=service_name).inc()


def observe_failed_message(service_name: str) -> None:
    FAILED_MESSAGES.labels(service=service_name).inc()


def observe_alert_sent(service_name: str) -> None:
    ALERTS_SENT.labels(service=service_name).inc()
