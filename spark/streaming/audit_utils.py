from pyspark.sql import SparkSession

from jdbc_utils import get_postgres_options


PIPELINE_NAME = "stock_market_medallion_pipeline"


def _execute_query(spark: SparkSession, query: str) -> None:
    """
    Execute a PostgreSQL SELECT query through Spark JDBC.
    """

    (
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


def _escape_sql_text(value: str) -> str:
    """
    Escape single quotes before placing text inside a SQL string literal.
    """

    return value.replace("'", "''")


def start_batch(
    spark: SparkSession,
    batch_id: int,
    source_record_count: int
) -> None:
    query = (
        "SELECT monitoring.start_pipeline_batch("
        f"'{PIPELINE_NAME}', "
        f"{batch_id}, "
        f"{source_record_count}"
        ")"
    )

    _execute_query(spark, query)

    print(
        f"[AUDIT] Batch {batch_id} marked as STARTED. "
        f"Source records: {source_record_count}"
    )


def complete_batch(
    spark: SparkSession,
    batch_id: int,
    bronze_inserted_count: int,
    silver_inserted_count: int,
    rejected_record_count: int,
    gold_affected_count: int
) -> None:
    query = (
        "SELECT monitoring.complete_pipeline_batch("
        f"'{PIPELINE_NAME}', "
        f"{batch_id}, "
        f"{bronze_inserted_count}, "
        f"{silver_inserted_count}, "
        f"{rejected_record_count}, "
        f"{gold_affected_count}"
        ")"
    )

    _execute_query(spark, query)

    print(
        f"[AUDIT] Batch {batch_id} marked as SUCCESS"
    )


def fail_batch(
    spark: SparkSession,
    batch_id: int,
    error_message: str
) -> None:
    safe_error_message = _escape_sql_text(
        error_message[:4000]
    )

    query = (
        "SELECT monitoring.fail_pipeline_batch("
        f"'{PIPELINE_NAME}', "
        f"{batch_id}, "
        f"'{safe_error_message}'"
        ")"
    )

    _execute_query(spark, query)

    print(
        f"[AUDIT] Batch {batch_id} marked as FAILED"
    )