from app import observability


def test_start_metrics_server_sets_health_and_starts_once(monkeypatch):
    calls = []

    def fake_start_http_server(port):
        calls.append(port)
        return object()

    monkeypatch.setattr(observability, "start_http_server", fake_start_http_server)

    service_name = "test-service"
    port = 19101
    observability._started_ports.discard(port)

    observability.start_metrics_server(service_name, port)
    observability.start_metrics_server(service_name, port)

    assert calls == [port]
    assert observability.SERVICE_HEALTH.labels(service=service_name)._value.get() == 1


def test_observe_message_metrics_increment():
    service_name = "test-observer"

    before_processed = observability.PROCESSED_MESSAGES.labels(service=service_name)._value.get()
    before_produced = observability.PRODUCED_MESSAGES.labels(service=service_name)._value.get()
    before_failed = observability.FAILED_MESSAGES.labels(service=service_name)._value.get()

    observability.observe_processed_message(service_name, latency_seconds=0.01)
    observability.observe_produced_message(service_name)
    observability.observe_failed_message(service_name)

    assert (
        observability.PROCESSED_MESSAGES.labels(service=service_name)._value.get()
        == before_processed + 1
    )
    assert (
        observability.PRODUCED_MESSAGES.labels(service=service_name)._value.get()
        == before_produced + 1
    )
    assert (
        observability.FAILED_MESSAGES.labels(service=service_name)._value.get()
        == before_failed + 1
    )
    assert observability.LAST_PROCESSED_TIMESTAMP.labels(service=service_name)._value.get() > 0
