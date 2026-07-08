from pathlib import Path
import os

from dotenv import load_dotenv

# Project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables
load_dotenv(BASE_DIR / ".env")


class Settings:
    """
    Centralized application configuration.
    """

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    )

    KAFKA_TOPIC = os.getenv(
        "KAFKA_TOPIC",
        "stock_prices"
    )

    # Producer
    PRODUCER_INTERVAL_SECONDS = int(
        os.getenv(
            "PRODUCER_INTERVAL_SECONDS",
            "1"
        )
    )

settings = Settings()