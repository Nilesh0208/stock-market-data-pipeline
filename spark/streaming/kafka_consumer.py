"""
Spark Structured Streaming Kafka Consumer

Reads stock market events from Kafka topic
and displays streaming data.
"""
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)


def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("StockMarketStreaming")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark

def write_to_postgres(batch_df, batch_id):

    (
        batch_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/stock_market")
        .option("dbtable", "bronze.stock_events")
        .option("user", "stock_user")
        .option("password", "stock_password")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print(f"Batch {batch_id} written to PostgreSQL")

def main():

    spark = create_spark_session()

    schema = StructType([
        StructField("event_id", StringType()),
        StructField("schema_version", StringType()),
        StructField("event_type", StringType()),
        StructField("source", StringType()),
        StructField("event_time", StringType()),
        StructField("ingested_at", StringType()),
        StructField("symbol", StringType()),
        StructField("company", StringType()),
        StructField("exchange", StringType()),
        StructField("currency", StringType()),
        StructField("price", DoubleType()),
        StructField("volume", IntegerType())
    ])

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            "stock-kafka:29092"
        )
        .option(
            "subscribe",
            "stock_prices"
        )
        .option(
            "startingOffsets",
            "latest"
        )
        .load()
    )


    value_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING)")
        .select(
            from_json(
                col("value"),
                schema
            ).alias("data")
        )
        .select("data.*")
        .withColumn(
            "event_time",
            to_timestamp(col("event_time"))
        )
        .withColumn(
            "ingested_at",
            to_timestamp(col("ingested_at"))
        )
    )

    query = (
        value_df
        .writeStream
        .outputMode("append")
        .foreachBatch(write_to_postgres)
        .option(
            "checkpointLocation",
            "/opt/spark-checkpoints/bronze_stock_events"
        )
        .start()   
        )

    query.awaitTermination()

if __name__ == "__main__":
    main()