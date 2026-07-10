from pyspark.sql import SparkSession

from config.spark_config import SparkConfig


def create_spark_session():
    """
    Create and configure a Spark session.
    """

    spark = (
        SparkSession.builder
        .appName(SparkConfig.APP_NAME)
        .master(SparkConfig.MASTER)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        SparkConfig.LOG_LEVEL
    )

    return spark


def main():
    spark = create_spark_session()

    print("=" * 60)
    print("Spark Session Created Successfully")
    print("=" * 60)

    print(f"Application Name : {spark.sparkContext.appName}")
    print(f"Spark Version    : {spark.version}")
    print(f"Master           : {spark.sparkContext.master}")

    spark.stop()


if __name__ == "__main__":
    main()