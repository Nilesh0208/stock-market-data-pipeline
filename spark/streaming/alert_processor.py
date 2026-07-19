"""
Operational alert processing.

Converts Data Quality results into WARNING or CRITICAL alerts.
HEALTHY batches do not generate alerts.
"""

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import settings


def build_pipeline_alert(
    data_quality_df: DataFrame,
) -> Optional[DataFrame]:
    """
    Build an operational alert from a Data Quality result.

    Expected Data Quality columns:
    - batch_id
    - source_count
    - valid_count
    - rejected_count
    - rejection_rate_pct
    - average_latency_ms
    - maximum_latency_ms
    - health_status

    Returns:
        A one-row DataFrame for WARNING or CRITICAL status.
        None when alerts are disabled.
        An empty DataFrame when the batch is HEALTHY.
    """

    if not settings.DQ_ALERTS_ENABLED:
        print("Operational alerts are disabled.")
        return None

    alert_df = (
        data_quality_df

        # HEALTHY batches must not create operational alerts.
        .filter(
            F.col("health_status").isin(
                "WARNING",
                "CRITICAL",
            )
        )

        # Identify which pipeline generated the alert.
        .withColumn(
            "pipeline_name",
            F.lit(settings.DQ_ALERT_PIPELINE_NAME),
        )

        # Create a different alert type for each severity.
        .withColumn(
            "alert_type",
            F.when(
                F.col("health_status") == "CRITICAL",
                F.lit("DATA_QUALITY_CRITICAL"),
            ).otherwise(
                F.lit("DATA_QUALITY_WARNING")
            ),
        )

        # Copy the Data Quality status into alert severity.
        .withColumn(
            "severity",
            F.col("health_status"),
        )

        # Build a null-safe alert message.
        .withColumn(
            "alert_message",
            F.concat(
                F.lit("Data Quality status is "),
                F.col("severity"),
                F.lit(". Rejection rate: "),
                F.coalesce(
                    F.round(
                        F.col("rejection_rate_pct"),
                        2,
                    ).cast("string"),
                    F.lit("0.0"),
                ),
                F.lit("%. Average latency: "),
                F.coalesce(
                    F.round(
                        F.col("average_latency_ms"),
                        2,
                    ).cast("string"),
                    F.lit("N/A"),
                ),
                F.lit(" ms. Maximum latency: "),
                F.coalesce(
                    F.round(
                        F.col("maximum_latency_ms"),
                        2,
                    ).cast("string"),
                    F.lit("N/A"),
                ),
                F.lit(" ms."),
            ),
        )

        .withColumn(
            "created_at",
            F.current_timestamp(),
        )

        # Keep only columns that exist in the staging table.
        .select(
            "pipeline_name",
            "batch_id",
            "alert_type",
            "severity",
            "alert_message",
            "source_count",
            "valid_count",
            "rejected_count",
            "rejection_rate_pct",
            "average_latency_ms",
            "maximum_latency_ms",
            "created_at",
        )
    )

    return alert_df