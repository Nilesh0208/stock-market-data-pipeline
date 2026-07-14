from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_gold_metrics_1min(silver_df: DataFrame) -> DataFrame:
    """
    Convert valid Silver stock events into one-minute Gold metrics.

    Grain:
        One row per symbol per one-minute event-time window
        within the current Spark micro-batch.
    """

    required_columns = {
        "event_id",
        "event_time",
        "symbol",
        "company",
        "exchange",
        "currency",
        "price",
        "volume",
        "processing_latency_ms",
    }

    missing_columns = required_columns.difference(silver_df.columns)

    if missing_columns:
        raise ValueError(
            "Gold aggregation is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    prepared_df = (
        silver_df
        .filter(
            F.col("event_id").isNotNull()
            & F.col("event_time").isNotNull()
            & F.col("symbol").isNotNull()
            & F.col("price").isNotNull()
            & F.col("volume").isNotNull()
        )
        .withColumn(
            "price",
            F.col("price").cast("decimal(18,4)")
        )
        .withColumn(
            "volume",
            F.col("volume").cast("long")
        )
        .withColumn(
            "processing_latency_ms",
            F.col("processing_latency_ms").cast("double")
        )
        .withColumn(
            "minute_start",
            F.date_trunc(
                "minute",
                F.col("event_time")
            )
        )
    )

    event_order_ascending = (
        Window
        .partitionBy(
            "minute_start",
            "symbol"
        )
        .orderBy(
            F.col("event_time").asc(),
            F.col("event_id").asc(),
        )
    )

    event_order_descending = (
        Window
        .partitionBy(
            "minute_start",
            "symbol"
        )
        .orderBy(
            F.col("event_time").desc(),
            F.col("event_id").desc(),
        )
    )

    ranked_df = (
        prepared_df
        .withColumn(
            "ascending_rank",
            F.row_number().over(
                event_order_ascending
            )
        )
        .withColumn(
            "descending_rank",
            F.row_number().over(
                event_order_descending
            )
        )
    )

    gold_df = (
        ranked_df
        .groupBy(
            "minute_start",
            "symbol",
        )
        .agg(
            F.first(
                "company",
                ignorenulls=True
            ).alias("company"),

            F.first(
                "exchange",
                ignorenulls=True
            ).alias("exchange"),

            F.first(
                "currency",
                ignorenulls=True
            ).alias("currency"),

            # Earliest event in the minute
            F.max(
                F.when(
                    F.col("ascending_rank") == 1,
                    F.col("price"),
                )
            ).alias("open_price"),

            F.max(
                F.when(
                    F.col("ascending_rank") == 1,
                    F.col("event_time"),
                )
            ).alias("open_event_time"),

            F.max(
                F.when(
                    F.col("ascending_rank") == 1,
                    F.col("event_id"),
                )
            ).alias("open_event_id"),

            # Highest and lowest price
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),

            # Latest event in the minute
            F.max(
                F.when(
                    F.col("descending_rank") == 1,
                    F.col("price"),
                )
            ).alias("close_price"),

            F.max(
                F.when(
                    F.col("descending_rank") == 1,
                    F.col("event_time"),
                )
            ).alias("close_event_time"),

            F.max(
                F.when(
                    F.col("descending_rank") == 1,
                    F.col("event_id"),
                )
            ).alias("close_event_id"),

            F.avg("price").alias("average_price"),
            F.sum("volume").alias("total_volume"),
            F.count("*").alias("event_count"),
            F.avg("processing_latency_ms").alias(
                "average_latency_ms"
            ),
        )
        .withColumnRenamed(
            "minute_start",
            "window_start"
        )
        .withColumn(
            "window_end",
            F.col("window_start")
            + F.expr("INTERVAL 1 MINUTE")
        )
        .withColumn(
            "average_price",
            F.col("average_price").cast(
                "decimal(18,4)"
            )
        )
        .select(
            "window_start",
            "window_end",
            "symbol",
            "company",
            "exchange",
            "currency",
            "open_price",
            "open_event_time",
            "open_event_id",
            "high_price",
            "low_price",
            "close_price",
            "close_event_time",
            "close_event_id",
            "average_price",
            "total_volume",
            "event_count",
            "average_latency_ms",
        )
    )

    return gold_df