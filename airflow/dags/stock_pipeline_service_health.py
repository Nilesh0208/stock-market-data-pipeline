"""
Production-style service health checks for the
Real-Time Stock Market Data Pipeline.

Checks:
1. PostgreSQL TCP connectivity
2. Kafka broker TCP connectivity
3. Spark master TCP connectivity
4. Latest pipeline batch-audit record
"""

import socket
from datetime import datetime, timedelta, timezone

import pendulum
import psycopg2
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException


POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "stock_market"
POSTGRES_USER = "stock_user"
POSTGRES_PASSWORD = "stock_password"

KAFKA_HOST = "stock-kafka"
KAFKA_PORT = 29092

SPARK_MASTER_HOST = "spark-master"
SPARK_MASTER_PORT = 7077

MAX_PIPELINE_STALENESS_MINUTES = 30

def check_tcp_connection(
    service_name: str,
    host: str,
    port: int,
    timeout_seconds: int = 10,
) -> str:
    """
    Verify that a TCP connection can be established
    with the given Docker service.
    """

    try:
        with socket.create_connection(
            (host, port),
            timeout=timeout_seconds,
        ):
            message = (
                f"{service_name} is reachable at "
                f"{host}:{port}"
            )
            print(message)
            return message

    except OSError as error:
        raise AirflowException(
            f"{service_name} health check failed for "
            f"{host}:{port}. Error: {error}"
        ) from error


@dag(
    dag_id="stock_pipeline_service_health",
    description="Checks PostgreSQL, Kafka, Spark and pipeline audit health",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(
        2026,
        7,
        20,
        tz="Asia/Kolkata",
    ),
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=[
        "stock-market",
        "monitoring",
        "service-health",
    ],
)
def stock_pipeline_service_health():

    @task
    def start_health_check() -> str:
        message = "Starting stock pipeline service health checks"
        print(message)
        return message

    @task
    def check_postgres() -> str:
        return check_tcp_connection(
            service_name="PostgreSQL",
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
        )

    @task
    def check_kafka() -> str:
        return check_tcp_connection(
            service_name="Kafka",
            host=KAFKA_HOST,
            port=KAFKA_PORT,
        )

    @task
    def check_spark_master() -> str:
        return check_tcp_connection(
            service_name="Spark Master",
            host=SPARK_MASTER_HOST,
            port=SPARK_MASTER_PORT,
        )

    @task
    def check_latest_pipeline_audit() -> dict:
        """
        Read the latest pipeline execution recorded in
        monitoring.pipeline_batch_audit and verify freshness.
        """

        connection = None

        try:
            connection = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DATABASE,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=10,
            )

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        pipeline_name,
                        spark_batch_id,
                        status,
                        source_record_count,
                        bronze_inserted_count,
                        silver_inserted_count,
                        rejected_record_count,
                        gold_affected_count,
                        started_at,
                        completed_at,
                        error_message
                    FROM monitoring.pipeline_batch_audit
                    ORDER BY started_at DESC
                    LIMIT 1;
                    """
                )

                row = cursor.fetchone()

            if row is None:
                raise AirflowException(
                    "No records were found in "
                    "monitoring.pipeline_batch_audit"
                )

            audit_record = {
                "pipeline_name": row[0],
                "spark_batch_id": row[1],
                "status": row[2],
                "source_record_count": row[3],
                "bronze_inserted_count": row[4],
                "silver_inserted_count": row[5],
                "rejected_record_count": row[6],
                "gold_affected_count": row[7],
                "started_at": (
                    row[8].isoformat()
                    if row[8] is not None
                    else None
                ),
                "completed_at": (
                    row[9].isoformat()
                    if row[9] is not None
                    else None
                ),
                "error_message": row[10],
            }

            print("Latest pipeline audit record:")
            print(audit_record)

            if audit_record["status"] == "FAILED":
                raise AirflowException(
                    "The latest pipeline batch has FAILED status. "
                    f"Batch ID: {audit_record['spark_batch_id']}. "
                    f"Error: {audit_record['error_message']}"
                )

            if row[9] is None:
                raise AirflowException(
                    "The latest pipeline batch has no completed_at timestamp. "
                    f"Batch ID: {audit_record['spark_batch_id']}"
                )

            completed_at = row[9]

            if completed_at.tzinfo is None:
                completed_at = completed_at.replace(
                    tzinfo=timezone.utc
                )

            current_time = datetime.now(timezone.utc)

            staleness_minutes = (
                current_time - completed_at
            ).total_seconds() / 60

            audit_record["staleness_minutes"] = round(
                staleness_minutes,
                2,
            )

            print(
                "Latest pipeline batch status: "
                f"{audit_record['status']}"
            )

            print(
                "Latest batch age: "
                f"{audit_record['staleness_minutes']} minutes"
            )

            if (
                staleness_minutes
                > MAX_PIPELINE_STALENESS_MINUTES
            ):
                raise AirflowException(
                    "Pipeline data is stale. "
                    f"Latest completed batch is "
                    f"{staleness_minutes:.2f} minutes old. "
                    f"Maximum allowed age is "
                    f"{MAX_PIPELINE_STALENESS_MINUTES} minutes."
                )

            print("Pipeline freshness check passed")

            return audit_record

        except psycopg2.Error as error:
            raise AirflowException(
                f"Unable to query pipeline audit data: {error}"
            ) from error

        finally:
            if connection is not None:
                connection.close()

    @task
    def finish_health_check() -> str:
        message = (
            "All stock market pipeline service health "
            "checks completed successfully"
        )
        print(message)
        return message

    start = start_health_check()

    postgres = check_postgres()
    kafka = check_kafka()
    spark = check_spark_master()

    latest_audit = check_latest_pipeline_audit()
    finish = finish_health_check()

    start >> [postgres, kafka, spark]
    [postgres, kafka, spark] >> latest_audit
    latest_audit >> finish


stock_pipeline_service_health()