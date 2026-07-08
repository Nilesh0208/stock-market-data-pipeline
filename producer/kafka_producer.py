from config.settings import settings
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

from .logger import logger


class StockKafkaProducer:
    """
    Reusable Kafka Producer for publishing stock market events.
    """

    def __init__(
        self,
        bootstrap_servers=None,
        topic=None
    ):
        bootstrap_servers = (
        bootstrap_servers
        or settings.KAFKA_BOOTSTRAP_SERVERS
        )
        self.topic = (
        topic
        or settings.KAFKA_TOPIC
        )

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,

                value_serializer=lambda value: json.dumps(value).encode("utf-8"),

                key_serializer=lambda key: key.encode("utf-8") if key else None,

                acks="all",

                retries=5,

                linger_ms=10,

                batch_size=16384
            )

            logger.info("Kafka Producer connected successfully.")

        except Exception as e:
            logger.exception(f"Unable to connect to Kafka: {e}")
            raise

    def send(self, message, key=None):
        """
        Send a message to Kafka.
        """

        try:

            future = self.producer.send(
                self.topic,
                key=key,
                value=message
            )

            metadata = future.get(timeout=10)

            logger.info(
                f"Message delivered | "
                f"Topic={metadata.topic} "
                f"Partition={metadata.partition} "
                f"Offset={metadata.offset}"
            )

        except KafkaError as e:
            logger.exception(f"Kafka Error: {e}")

        except Exception as e:
            logger.exception(f"Unexpected Error: {e}")

    def close(self):
        self.producer.flush()
        self.producer.close()

        logger.info("Kafka Producer closed.")