"""
PostgreSQL repository for external alert notification tracking.

This module creates notification records and updates delivery status.
It does not send emails.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2

from config.settings import settings


def get_postgres_connection():
    """
    Create a PostgreSQL connection using centralized settings.
    """

    return psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        database=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
    )


def create_pending_notification(
    pipeline_name: str,
    spark_batch_id: int,
    alert_severity: str,
    recipient: str,
    subject: str,
    notification_message: str,
    notification_channel: str = "EMAIL",
) -> int:
    """
    Create a PENDING notification record.

    The unique database constraint prevents duplicate notification
    records for the same batch, severity, channel, and recipient.

    If the notification already exists, its existing ID is returned.
    """

    connection = None
    cursor = None

    try:
        connection = get_postgres_connection()
        cursor = connection.cursor()

        insert_sql = """
            INSERT INTO monitoring.alert_notifications (
                pipeline_name,
                spark_batch_id,
                alert_severity,
                notification_channel,
                recipient,
                subject,
                notification_message,
                delivery_status,
                attempt_count,
                maximum_attempts
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                'PENDING',
                0,
                %s
            )
            ON CONFLICT (
                pipeline_name,
                spark_batch_id,
                alert_severity,
                notification_channel,
                recipient
            )
            DO NOTHING
            RETURNING notification_id;
        """

        cursor.execute(
            insert_sql,
            (
                pipeline_name,
                spark_batch_id,
                alert_severity,
                notification_channel,
                recipient,
                subject,
                notification_message,
                settings.EMAIL_NOTIFICATION_MAXIMUM_ATTEMPTS,
            ),
        )

        inserted_row = cursor.fetchone()

        if inserted_row:
            notification_id = inserted_row[0]
        else:
            select_existing_sql = """
                SELECT notification_id
                FROM monitoring.alert_notifications
                WHERE pipeline_name = %s
                  AND spark_batch_id = %s
                  AND alert_severity = %s
                  AND notification_channel = %s
                  AND recipient = %s;
            """

            cursor.execute(
                select_existing_sql,
                (
                    pipeline_name,
                    spark_batch_id,
                    alert_severity,
                    notification_channel,
                    recipient,
                ),
            )

            existing_row = cursor.fetchone()

            if not existing_row:
                raise RuntimeError(
                    "Notification was not inserted and existing "
                    "notification could not be found."
                )

            notification_id = existing_row[0]

        connection.commit()

        return notification_id

    except Exception:
        if connection:
            connection.rollback()

        raise

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()


def mark_notification_attempt_started(
    notification_id: int,
) -> int:
    """
    Start a notification delivery attempt.

    The function:
    - increments attempt_count
    - records attempt timestamps
    - changes status to RETRYING
    - clears the previous retry time and error

    Returns:
        Updated attempt count.
    """

    connection = None
    cursor = None

    try:
        connection = get_postgres_connection()
        cursor = connection.cursor()

        update_sql = """
            UPDATE monitoring.alert_notifications
            SET
                delivery_status = 'RETRYING',
                attempt_count = attempt_count + 1,
                first_attempted_at = COALESCE(
                    first_attempted_at,
                    CURRENT_TIMESTAMP
                ),
                last_attempted_at = CURRENT_TIMESTAMP,
                next_retry_at = NULL,
                last_error = NULL
            WHERE notification_id = %s
              AND delivery_status <> 'SENT'
              AND attempt_count < maximum_attempts
            RETURNING attempt_count;
        """

        cursor.execute(
            update_sql,
            (notification_id,),
        )

        updated_row = cursor.fetchone()

        if not updated_row:
            raise RuntimeError(
                "Notification attempt could not be started. "
                "The notification may already be SENT or may have "
                "reached its maximum attempts."
            )

        connection.commit()

        return updated_row[0]

    except Exception:
        if connection:
            connection.rollback()

        raise

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()


def mark_notification_sent(
    notification_id: int,
) -> None:
    """
    Mark a notification as successfully delivered.
    """

    connection = None
    cursor = None

    try:
        connection = get_postgres_connection()
        cursor = connection.cursor()

        update_sql = """
            UPDATE monitoring.alert_notifications
            SET
                delivery_status = 'SENT',
                delivered_at = CURRENT_TIMESTAMP,
                next_retry_at = NULL,
                last_error = NULL
            WHERE notification_id = %s
              AND delivery_status <> 'SENT';
        """

        cursor.execute(
            update_sql,
            (notification_id,),
        )

        if cursor.rowcount == 0:
            raise RuntimeError(
                "Notification could not be marked as SENT."
            )

        connection.commit()

    except Exception:
        if connection:
            connection.rollback()

        raise

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()


def mark_notification_failed(
    notification_id: int,
    error_message: str,
) -> None:
    """
    Mark a notification delivery attempt as FAILED.

    A retry time is created only when the notification has attempts
    remaining.
    """

    connection = None
    cursor = None

    try:
        connection = get_postgres_connection()
        cursor = connection.cursor()

        next_retry_at = datetime.now(timezone.utc) + timedelta(
            seconds=settings.EMAIL_NOTIFICATION_RETRY_DELAY_SECONDS
        )

        update_sql = """
            UPDATE monitoring.alert_notifications
            SET
                delivery_status = 'FAILED',
                last_error = %s,
                next_retry_at = CASE
                    WHEN attempt_count < maximum_attempts
                    THEN %s
                    ELSE NULL
                END
            WHERE notification_id = %s
              AND delivery_status <> 'SENT';
        """

        cursor.execute(
            update_sql,
            (
                error_message[:4000],
                next_retry_at,
                notification_id,
            ),
        )

        if cursor.rowcount == 0:
            raise RuntimeError(
                "Notification could not be marked as FAILED."
            )

        connection.commit()

    except Exception:
        if connection:
            connection.rollback()

        raise

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()


def get_notification_status(
    notification_id: int,
) -> Optional[dict]:
    """
    Read the current notification delivery status.

    This helper is useful for testing and operational diagnostics.
    """

    connection = None
    cursor = None

    try:
        connection = get_postgres_connection()
        cursor = connection.cursor()

        select_sql = """
            SELECT
                notification_id,
                pipeline_name,
                spark_batch_id,
                alert_severity,
                notification_channel,
                recipient,
                delivery_status,
                attempt_count,
                maximum_attempts,
                first_attempted_at,
                last_attempted_at,
                next_retry_at,
                delivered_at,
                last_error
            FROM monitoring.alert_notifications
            WHERE notification_id = %s;
        """

        cursor.execute(
            select_sql,
            (notification_id,),
        )

        row = cursor.fetchone()

        if not row:
            return None

        column_names = [
            description[0]
            for description in cursor.description
        ]

        return dict(zip(column_names, row))

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()