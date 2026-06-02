from unittest.mock import Mock

import lib.kafka_utils as kafka_utils


def test_create_producer_retries_until_success(monkeypatch):
    call_count = {"n": 0}
    producer_instance = Mock()

    def fake_kafka_producer(**kwargs):
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise RuntimeError("broker unavailable")
        return producer_instance

    sleep_mock = Mock()
    monkeypatch.setattr(kafka_utils, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setattr(kafka_utils, "KafkaProducer", fake_kafka_producer)
    monkeypatch.setattr(kafka_utils.time, "sleep", sleep_mock)

    producer = kafka_utils.create_producer()

    assert producer is producer_instance
    assert call_count["n"] == 3
    assert sleep_mock.call_count == 2
