import random

from app import fraud_producer

def test_get_random_weight_number_returns_valid_bucket():
    value = fraud_producer.get_random_weight_number([10, 1, 1, 1])
    assert value in {1, 2, 3, 4}


def test_build_message_has_expected_fields():
    random.seed(42)
    msg = fraud_producer.build_message()

    assert set(msg.keys()) == {
        "transaction_id",
        "event_id",
        "timestamp",
        "amount",
        "country_risk",
    }
    assert msg["amount"] == 5000
    assert isinstance(msg["country_risk"], float)
    assert 0.0 <= msg["country_risk"] <= 1.0
