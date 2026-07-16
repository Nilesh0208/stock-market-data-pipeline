from decimal import Decimal
from typing import Optional

from pyspark.sql import SparkSession

from jdbc_utils import get_postgres_options


PIPELINE_NAME = "stock_market_medallion_pipeline"


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
        f"'{PIPELINE_NAME}', "
        f"{batch_id}, "
        f"{source_record_count}, "
        f"{valid_record_count}, "
        f"{rejected_record_count}, "
        f"{average_latency_sql}, "
        f"{maximum_latency_sql}"
        ") AS quality_status"
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