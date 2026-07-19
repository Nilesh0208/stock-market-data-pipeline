from decimal import Decimal
from typing import Optional

from pyspark.sql import SparkSession

from config.settings import settings
from jdbc_utils import get_postgres_options


def _execute_query(
    spark: SparkSession,
    query: str
):
    """
    Execute a PostgreSQL SELECT query through Spark JDBC
    and return the collected rows.
    """

    return (
        spark.read
        .format("jdbc")
        .options(
            **get_postgres_options(
                query=query
            )
        )
        .load()
        .collect()
    )


def _to_sql_numeric(
    value: Optional[float]
) -> str:
    """
    Convert a Python numeric value into SQL-safe text.

    PostgreSQL receives NULL when the value is missing.
    """

    if value is None:
        return "NULL"

    return str(Decimal(str(value)))


def evaluate_batch_quality(
    spark: SparkSession,
    batch_id: int,
    source_record_count: int,
    valid_record_count: int,
    rejected_record_count: int,
    average_latency_ms: Optional[float],
    maximum_latency_ms: Optional[int]
) -> str:
    """
    Evaluate and store data-quality metrics for one Spark batch.
    """

    average_latency_sql = _to_sql_numeric(
        average_latency_ms
    )

    maximum_latency_sql = (
        "NULL"
        if maximum_latency_ms is None
        else str(maximum_latency_ms)
    )

    query = (
        "SELECT monitoring.evaluate_batch_quality("
        f"'{settings.DQ_ALERT_PIPELINE_NAME}'::VARCHAR, "
        f"{int(batch_id)}::BIGINT, "
        f"{int(source_record_count)}::BIGINT, "
        f"{int(valid_record_count)}::BIGINT, "
        f"{int(rejected_record_count)}::BIGINT, "
        f"{average_latency_sql}::NUMERIC, "
        f"{maximum_latency_sql}::BIGINT, "
        f"{settings.DQ_WARNING_REJECTION_RATE_PCT}::NUMERIC, "
        f"{settings.DQ_CRITICAL_REJECTION_RATE_PCT}::NUMERIC, "
        f"{settings.DQ_WARNING_AVG_LATENCY_MS}::NUMERIC, "
        f"{settings.DQ_CRITICAL_AVG_LATENCY_MS}::NUMERIC, "
        f"{int(settings.DQ_CRITICAL_MAX_LATENCY_MS)}::BIGINT"
        ") AS quality_status"
    )
    print(
    f"[DATA QUALITY SQL] {query}"
    )
    
    result = _execute_query(
        spark=spark,
        query=query
    )

    quality_status = result[0]["quality_status"]

    print(
        f"[DATA QUALITY] Batch {batch_id} "
        f"status={quality_status}, "
        f"source={source_record_count}, "
        f"valid={valid_record_count}, "
        f"rejected={rejected_record_count}, "
        f"avg_latency_ms={average_latency_ms}, "
        f"max_latency_ms={maximum_latency_ms}"
    )

    return quality_status