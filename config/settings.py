from pathlib import Path
import os

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent

load_dotenv(BASE_DIR / ".env")


class Settings:
    """
    Centralized application configuration.
    """

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    KAFKA_TOPIC = os.getenv(
        "KAFKA_TOPIC",
        "stock_prices",
    )

    # Producer
    PRODUCER_INTERVAL_SECONDS = int(
        os.getenv(
            "PRODUCER_INTERVAL_SECONDS",
            "1",
        )
    )

    # PostgreSQL
    POSTGRES_HOST = os.getenv(
        "POSTGRES_HOST",
        "postgres",
    )

    POSTGRES_PORT = os.getenv(
        "POSTGRES_PORT",
        "5432",
    )

    POSTGRES_DB = os.getenv(
        "POSTGRES_DB",
        "stock_market",
    )

    POSTGRES_USER = os.getenv(
        "POSTGRES_USER",
        "stock_user",
    )

    POSTGRES_PASSWORD = os.getenv(
        "POSTGRES_PASSWORD",
        "stock_password",
    )

    POSTGRES_JDBC_URL = (
        f"jdbc:postgresql://"
        f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    POSTGRES_JDBC_URL_UUID = (
        f"{POSTGRES_JDBC_URL}?stringtype=unspecified"
    )

    POSTGRES_DRIVER = "org.postgresql.Driver"


settings = Settings()