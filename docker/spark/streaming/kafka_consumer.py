"""
Spark Structured Streaming Kafka Consumer

This is the entry point for reading stock market events
from Kafka using Spark Structured Streaming.
"""

from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """
    Create and configure a Spark Session.
    """

    spark = (
        SparkSession.builder
        .appName("StockMarketStreaming")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():
    spark = create_spark_session()

    print("=" * 60)
    print("Spark Session Created Successfully")
    print(f"Spark Version : {spark.version}")
    print(f"Application   : {spark.sparkContext.appName}")
    print(f"Master        : {spark.sparkContext.master}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()