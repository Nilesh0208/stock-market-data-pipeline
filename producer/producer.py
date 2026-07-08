import time

from config.settings import settings
from .kafka_producer import StockKafkaProducer
from .logger import logger
from .stock_generator import generate_stock


def main():
    producer = StockKafkaProducer()

    logger.info("Starting Stock Market Producer...")

    try:
        while True:
            stock = generate_stock()

            producer.send(
                message=stock,
                key=stock["symbol"]
            )

            logger.info(stock)

            time.sleep(
                settings.PRODUCER_INTERVAL_SECONDS
            )

    except KeyboardInterrupt:
        logger.info("Stopping Producer...")

    finally:
        producer.close()


if __name__ == "__main__":
    main()