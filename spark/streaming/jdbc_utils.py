import psycopg2

from typing import Optional

from config.settings import settings


def get_postgres_options(
    table_name: Optional[str] = None,
    query: Optional[str] = None,
) -> dict:
    """
    Build reusable PostgreSQL JDBC options.

    Provide either table_name or query, but not both.
    """

    if table_name and query:
        raise ValueError(
            "Provide either table_name or query, not both."
        )

    if not table_name and not query:
        raise ValueError(
            "Either table_name or query must be provided."
        )

    options = {
        "url": settings.POSTGRES_JDBC_URL,
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": settings.POSTGRES_DRIVER,
    }

    if table_name:
        options["dbtable"] = table_name

    if query:
        options["query"] = query

    return options

def merge_pipeline_alerts() -> None:
    """
    Merge operational alerts from staging into the final alert table.

    Duplicate alerts are ignored using the PostgreSQL unique constraint.
    """

    connection = None
    cursor = None

    try:
        connection = psycopg2.connect(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            database=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
        )

        cursor = connection.cursor()

        merge_sql = """
            INSERT INTO monitoring.pipeline_alerts (
                pipeline_name,
                batch_id,
                alert_type,
                severity,
                alert_message,
                source_count,
                valid_count,
                rejected_count,
                rejection_rate_pct,
                average_latency_ms,
                maximum_latency_ms,
                created_at
            )
            SELECT
                pipeline_name,
                batch_id,
                alert_type,
                severity,
                alert_message,
                source_count,
                valid_count,
                rejected_count,
                rejection_rate_pct,
                average_latency_ms,
                maximum_latency_ms,
                created_at
            FROM monitoring.pipeline_alerts_staging
            ON CONFLICT (
                pipeline_name,
                batch_id,
                alert_type
            )
            DO NOTHING;
        """

        clear_staging_sql = """
            TRUNCATE TABLE monitoring.pipeline_alerts_staging;
        """

        cursor.execute(merge_sql)
        cursor.execute(clear_staging_sql)

        connection.commit()

        print("Operational alerts merged successfully.")

    except Exception:
        if connection:
            connection.rollback()

        print("Operational alert merge failed.")
        raise

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()