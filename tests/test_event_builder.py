"""
Unit tests for the stock-market event builder.

These tests do not require Kafka, Spark, PostgreSQL,
Docker, or any running external service.
"""

from datetime import datetime
from uuid import UUID

from producer.event_builder import build_event


def test_build_event_contains_required_fields():
    stock_data = {
        "symbol": "AAPL",
        "company": "Apple Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "price": 195.50,
        "volume": 2500,
    }

    event = build_event(stock_data)

    required_fields = {
        "event_id",
        "schema_version",
        "event_type",
        "source",
        "event_time",
        "ingested_at",
        "symbol",
        "company",
        "exchange",
        "currency",
        "price",
        "volume",
    }

    assert required_fields.issubset(event.keys())


def test_build_event_generates_valid_uuid():
    stock_data = {
        "symbol": "MSFT",
        "company": "Microsoft Corporation",
        "exchange": "NASDAQ",
        "currency": "USD",
        "price": 510.25,
        "volume": 4000,
    }

    event = build_event(stock_data)

    parsed_uuid = UUID(event["event_id"])

    assert str(parsed_uuid) == event["event_id"]


def test_build_event_generates_valid_timestamps():
    stock_data = {
        "symbol": "GOOGL",
        "company": "Alphabet Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "price": 180.75,
        "volume": 3200,
    }

    event = build_event(stock_data)

    event_time = datetime.fromisoformat(event["event_time"])
    ingested_at = datetime.fromisoformat(event["ingested_at"])

    assert event_time.tzinfo is not None
    assert ingested_at.tzinfo is not None
    assert event["event_time"] == event["ingested_at"]


def test_build_event_preserves_business_data():
    stock_data = {
        "symbol": "TSLA",
        "company": "Tesla Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "price": 315.75,
        "volume": 7800,
    }

    event = build_event(stock_data)

    for key, value in stock_data.items():
        assert event[key] == value


def test_build_event_contains_expected_metadata():
    stock_data = {
        "symbol": "AMZN",
        "company": "Amazon.com Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "price": 225.10,
        "volume": 5600,
    }

    event = build_event(stock_data)

    assert event["schema_version"] == "1.0"
    assert event["event_type"] == "stock_price"
    assert event["source"] == "stock-producer"

def test_build_event_generates_unique_event_ids():
    stock_data = {
        "symbol": "AAPL",
        "company": "Apple Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "price": 195.50,
        "volume": 2500,
    }

    first_event = build_event(stock_data)
    second_event = build_event(stock_data)

    assert first_event["event_id"] != second_event["event_id"]


def test_build_event_does_not_modify_input_data():
    stock_data = {
        "symbol": "MSFT",
        "company": "Microsoft Corporation",
        "exchange": "NASDAQ",
        "currency": "USD",
        "price": 510.25,
        "volume": 4000,
    }

    original_data = stock_data.copy()

    build_event(stock_data)

    assert stock_data == original_data