"""
Controlled producer for testing Data Quality thresholds.

Examples:

    python -m producer.data_quality_test_producer warning
    python -m producer.data_quality_test_producer critical
"""

import json
import sys
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "stock_prices"

VALID_STOCKS = [
    {
        "symbol": "AAPL",
        "company": "Apple Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "price": 195.50,
        "volume": 5000,
    },
    {
        "symbol": "MSFT",
        "company": "Microsoft Corporation",
        "exchange": "NASDAQ",
        "currency": "USD",
        "price": 510.25,
        "volume": 4200,
    },
    {
        "symbol": "GOOGL",
        "company": "Alphabet Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "price": 180.75,
        "volume": 3600,
    },
]


def build_event(index: int, invalid: bool = False) -> dict:
    """
    Build one stock event.

    For invalid events, price is set to -1.
    The Silver validation rule should reject price <= 0.
    """

    stock = VALID_STOCKS[index % len(VALID_STOCKS)]
    timestamp = datetime.now(timezone.utc).isoformat()

    event = {
        "event_id": str(uuid.uuid4()),
        "schema_version": "1.0",
        "event_type": "stock_price",
        "source": "data-quality-test-producer",
        "event_time": timestamp,
        "ingested_at": timestamp,
        "symbol": stock["symbol"],
        "company": stock["company"],
        "exchange": stock["exchange"],
        "currency": stock["currency"],
        "price": -1.0 if invalid else stock["price"],
        "volume": stock["volume"],
    }

    return event


def get_invalid_count(test_mode: str) -> int:
    """
    Return how many invalid records should be sent.
    """

    if test_mode == "warning":
        return 1

    if test_mode == "critical":
        return 3

    raise ValueError(
        "Test mode must be either 'warning' or 'critical'"
    )


def main() -> None:
    test_mode = (
        sys.argv[1].lower()
        if len(sys.argv) > 1
        else ""
    )

    try:
        invalid_count = get_invalid_count(test_mode)
    except ValueError as error:
        print(error)
        print(
            "Usage: python -m "
            "producer.data_quality_test_producer "
            "<warning|critical>"
        )
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value
        ).encode("utf-8"),
        acks="all",
    )

    total_events = 10

    print(
        f"Starting {test_mode.upper()} Data Quality test"
    )

    print(
        f"Total events: {total_events}, "
        f"valid: {total_events - invalid_count}, "
        f"invalid: {invalid_count}"
    )

    try:
        for index in range(total_events):
            # Put invalid records at the beginning.
            invalid = index < invalid_count

            event = build_event(
                index=index,
                invalid=invalid,
            )

            producer.send(
                KAFKA_TOPIC,
                value=event,
            )

            record_type = (
                "INVALID"
                if invalid
                else "VALID"
            )

            print(
                f"Sent {record_type} event "
                f"{index + 1}/{total_events}: "
                f"symbol={event['symbol']}, "
                f"price={event['price']}"
            )

        producer.flush()

        print(
            f"{test_mode.upper()} test events "
            "sent successfully."
        )

    finally:
        producer.close()


if __name__ == "__main__":
    main()