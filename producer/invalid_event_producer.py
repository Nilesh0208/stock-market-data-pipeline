import json

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

invalid_event = {
    "event_id": "11111111-1111-4111-8111-111111111111",
    "schema_version": "1.0",
    "event_type": "stock_price",
    "source": "stock-producer",
    "event_time": "2026-07-12T10:00:00+00:00",
    "ingested_at": "2026-07-12T10:00:00+00:00",
    "symbol": "",
    "company": "Invalid Test Company",
    "exchange": "NASDAQ",
    "currency": "USD",
    "price": -100,
    "volume": -50,
}

producer.send(
    "stock_prices",
    value=invalid_event,
)

producer.flush()
producer.close()

print("Invalid test event sent")