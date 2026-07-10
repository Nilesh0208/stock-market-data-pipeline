from config.settings import settings


class SparkConfig:
    """
    Centralized Spark configuration.
    """

    APP_NAME = "Stock Market Data Pipeline"

    MASTER = "local[*]"

    LOG_LEVEL = "WARN"

