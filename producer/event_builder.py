import uuid
from datetime import datetime, timezone


SCHEMA_VERSION = "1.0"
EVENT_TYPE = "stock_price"
SOURCE = "stock-producer"


def build_event(stock_data: dict) -> dict:
    """
    Build a production-ready event by combining
    event metadata with stock business data.
    """

    timestamp = datetime.now(timezone.utc).isoformat()

    event = {
        "event_id": str(uuid.uuid4()),
        "schema_version": SCHEMA_VERSION,
        "event_type": EVENT_TYPE,
        "source": SOURCE,
        "event_time": timestamp,
        "ingested_at": timestamp,
        **stock_data
    }

    return event