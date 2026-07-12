from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_to_silver(bronze_df: DataFrame) -> DataFrame:
    """
    Clean, validate, standardize, and enrich Bronze stock events
    before loading them into the Silver layer.
    """

    silver_df = (
        bronze_df

        # Remove duplicates within the current micro-batch.
        .dropDuplicates(["event_id"])

        # Standardize string columns.
        .withColumn("symbol", F.upper(F.trim(F.col("symbol"))))
        .withColumn("company", F.trim(F.col("company")))
        .withColumn("exchange", F.upper(F.trim(F.col("exchange"))))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("source", F.lower(F.trim(F.col("source"))))

        # Standardize numeric types.
        .withColumn("price", F.col("price").cast("decimal(18,4)"))
        .withColumn("volume", F.col("volume").cast("long"))

        # Add enrichment columns.
        .withColumn("processing_time", F.current_timestamp())
        .withColumn("event_date", F.to_date(F.col("event_time")))

        .withColumn(
            "producer_latency_ms",
            (
                (
                    F.col("ingested_at").cast("double")
                    - F.col("event_time").cast("double")
                ) * 1000
            ).cast("long")
        )

        .withColumn(
            "processing_latency_ms",
            (
                (
                    F.col("processing_time").cast("double")
                    - F.col("ingested_at").cast("double")
                ) * 1000
            ).cast("long")
        )

        # Apply data-quality rules.
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("ingested_at").isNotNull())
        .filter(F.col("symbol").isNotNull())
        .filter(F.length(F.col("symbol")) > 0)
        .filter(F.col("price").isNotNull())
        .filter(F.col("price") > 0)
        .filter(F.col("volume").isNotNull())
        .filter(F.col("volume") >= 0)

        # Match PostgreSQL Silver table columns.
        .select(
            "event_id",
            "schema_version",
            "event_type",
            "source",
            "event_time",
            "ingested_at",
            "symbol",
            "company",
            "exchange",
            "currency",
            "price",
            "volume",
            "processing_time",
            "event_date",
            "producer_latency_ms",
            "processing_latency_ms",
        )
    )

    return silver_df


def transform_to_rejected(bronze_df: DataFrame) -> DataFrame:
    """
    Identify invalid Bronze records and attach all applicable
    rejection reasons.
    """

    prepared_df = (
        bronze_df
        .withColumn("symbol", F.upper(F.trim(F.col("symbol"))))
        .withColumn("company", F.trim(F.col("company")))
        .withColumn("exchange", F.upper(F.trim(F.col("exchange"))))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("source", F.lower(F.trim(F.col("source"))))
        .withColumn("price", F.col("price").cast("decimal(18,4)"))
        .withColumn("volume", F.col("volume").cast("long"))
    )

    rejected_df = (
        prepared_df
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                "; ",
                F.when(
                    F.col("event_id").isNull(),
                    F.lit("missing event_id")
                ),
                F.when(
                    F.col("event_time").isNull(),
                    F.lit("missing or invalid event_time")
                ),
                F.when(
                    F.col("ingested_at").isNull(),
                    F.lit("missing or invalid ingested_at")
                ),
                F.when(
                    F.col("symbol").isNull()
                    | (F.length(F.col("symbol")) == 0),
                    F.lit("missing or empty symbol")
                ),
                F.when(
                    F.col("price").isNull(),
                    F.lit("missing or invalid price")
                ),
                F.when(
                    F.col("price") <= 0,
                    F.lit("price must be greater than zero")
                ),
                F.when(
                    F.col("volume").isNull(),
                    F.lit("missing or invalid volume")
                ),
                F.when(
                    F.col("volume") < 0,
                    F.lit("volume must be non-negative")
                )
            )
        )
        .filter(F.length(F.col("rejection_reason")) > 0)
        .withColumn("rejected_at", F.current_timestamp())
        .select(
            "event_id",
            "schema_version",
            "event_type",
            "source",
            "event_time",
            "ingested_at",
            "symbol",
            "company",
            "exchange",
            "currency",
            "price",
            "volume",
            "rejection_reason",
            "rejected_at",
        )
    )

    return rejected_df