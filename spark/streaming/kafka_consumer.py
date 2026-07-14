"""
Spark Structured Streaming Kafka Consumer

Reads stock market events from Kafka topic
and displays streaming data.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from gold_processor import build_gold_metrics_1min
from jdbc_utils import get_postgres_options
from pyspark.sql.functions import col, from_json, to_timestamp
from silver_transformations import (
    transform_to_silver,
    transform_to_rejected,
)
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
    spark = batch_df.sparkSession
    
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty")
        return

    print(f"Processing batch {batch_id}")

    # Step 1: Write raw records to Bronze staging
    (
        batch_df.write
        .format("jdbc")
        .options(
            **get_postgres_options(
                table_name="bronze.stock_events_staging"
            )
        )
        .mode("append")
        .save()
    )

    print(
        f"Batch {batch_id} written to "
        "bronze.stock_events_staging"
    )

    bronze_merge_query = (
        "SELECT bronze.merge_stock_events() "
        "AS inserted_count"
    )

    bronze_merge_result = (
        spark.read
        .format("jdbc")
        .options(
            **get_postgres_options(
                query=bronze_merge_query
            )
        )
        .load()
        .collect()
    )

    bronze_inserted_count = bronze_merge_result[0]["inserted_count"]

    print(
        f"Batch {batch_id} merged into bronze.stock_events. "
        f"New rows inserted: {bronze_inserted_count}"
    )

    # Step 2: Build valid and rejected DataFrames
    silver_df = transform_to_silver(batch_df)

    staging_df = (
        silver_df
        .withColumn(
            "spark_batch_id",
            F.lit(batch_id).cast("long")
        )
    )    

    rejected_df = (
        transform_to_rejected(batch_df)
        .withColumn(
            "batch_id",
            F.lit(batch_id).cast("long")
        )
    )
    

    # Display valid Silver records
    print(f"Silver output for batch {batch_id}")

    silver_df.show(
        n=20,
        truncate=False
    )

    # Step 3: Write valid records to Silver
    (
        staging_df.write
        .format("jdbc")
        .options(
            **get_postgres_options(
                table_name="silver.stock_events_staging"
            )
        )
        .mode("append")
        .save()
    )

    print(
        f"Batch {batch_id} written to "
        "silver.stock_events_staging"
    )

    # Execute PostgreSQL staging-to-Silver merge
    spark = batch_df.sparkSession

    merge_query = (
        f"SELECT silver.merge_stock_events({batch_id}) "
        "AS inserted_count"
    )

    merge_result = (
        spark.read
        .format("jdbc")
        .options(
            **get_postgres_options(
                query=merge_query
            )
        )
        .load()
        .collect()
    )

    inserted_count = merge_result[0]["inserted_count"]

    print(
        f"Batch {batch_id} merged into silver.stock_events. "
        f"New rows inserted: {inserted_count}"
    )    

    # Step 4: Build Gold one-minute metrics
    gold_df = (
        build_gold_metrics_1min(silver_df)
        .withColumn(
            "spark_batch_id",
            F.lit(batch_id).cast("long")
        )
    )

    if gold_df.isEmpty():
        print(f"No Gold records for batch {batch_id}")

    else:
        print(f"Gold output for batch {batch_id}")

        gold_df.show(
            n=20,
            truncate=False
        )

        # Write Gold aggregates to staging
        (
            gold_df.write
            .format("jdbc")
            .options(
                **get_postgres_options(
                    table_name="gold.stock_metrics_1min_staging"
                )
            )
            .mode("append")
            .save()
        )
        
        print(
            f"Batch {batch_id} written to "
            "gold.stock_metrics_1min_staging"
        )

        # Merge Gold staging into final Gold table
        gold_merge_query = (
            f"SELECT gold.merge_stock_metrics_1min({batch_id}) "
            "AS affected_count"
        )

        gold_merge_result = (
            spark.read
            .format("jdbc")
            .options(
                **get_postgres_options(
                    query=gold_merge_query
                )
            )
            .load()
            .collect()
        )

        affected_count = gold_merge_result[0]["affected_count"]

        print(
            f"Batch {batch_id} merged into "
            "gold.stock_metrics_1min. "
            f"Rows affected: {affected_count}"
        )

    # Check rejected records
    rejected_count = rejected_df.count()

    print(f"Rejected records in batch {batch_id}: {rejected_count}")

    if rejected_count > 0:

        rejected_df.show(
            n=20,
            truncate=False
        )

        (
            rejected_df.write
            .format("jdbc")
            .options(
                **get_postgres_options(
                    table_name="silver.stock_events_rejected"
                )
            )
            .mode("append")
            .save()
        )

        print(
            f"Rejected records from batch {batch_id} "
            "written to silver.stock_events_rejected"
        )

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