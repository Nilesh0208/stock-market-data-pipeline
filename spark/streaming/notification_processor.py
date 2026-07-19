"""
Notification delivery processor.

Coordinates notification tracking and email delivery.

Responsibilities:
1. Create or find an idempotent PENDING notification.
2. Check whether it is eligible for delivery.
3. Start a delivery attempt.
4. Send the email.
5. Mark the notification as SENT or FAILED.
"""

from datetime import datetime, timezone
from typing import Optional

from config.settings import settings
from spark.streaming.email_notification_service import (
    send_email_notification,
)
from spark.streaming.notification_repository import (
    create_pending_notification,
    get_notification_status,
    mark_notification_attempt_started,
    mark_notification_failed,
    mark_notification_sent,
)


def process_critical_email_notification(
    pipeline_name: str,
    spark_batch_id: int,
    subject: str,
    notification_message: str,
    recipient: Optional[str] = None,
) -> dict:
    """
    Process one CRITICAL email notification.

    Args:
        pipeline_name: Name of the data pipeline.
        spark_batch_id: Spark micro-batch identifier.
        subject: Email subject.
        notification_message: Email body.
        recipient: Optional recipient override.

    Returns:
        Dictionary containing processing result and notification state.
    """

    email_recipient = recipient or settings.ALERT_EMAIL_TO

    notification_id = create_pending_notification(
        pipeline_name=pipeline_name,
        spark_batch_id=spark_batch_id,
        alert_severity="CRITICAL",
        recipient=email_recipient,
        subject=subject,
        notification_message=notification_message,
        notification_channel="EMAIL",
    )

    current_status = get_notification_status(notification_id)

    if current_status is None:
        raise RuntimeError(
            f"Notification {notification_id} could not be loaded."
        )

    # Idempotency: never send an email that was already delivered.
    if current_status["delivery_status"] == "SENT":
        return {
            "notification_id": notification_id,
            "result": "SKIPPED",
            "reason": "Notification was already sent.",
            "status": current_status,
        }

    # Prevent another process from starting the same active attempt.
    if current_status["delivery_status"] == "RETRYING":
        return {
            "notification_id": notification_id,
            "result": "SKIPPED",
            "reason": "Notification delivery is already in progress.",
            "status": current_status,
        }

    attempt_count = current_status["attempt_count"]
    maximum_attempts = current_status["maximum_attempts"]

    if attempt_count >= maximum_attempts:
        return {
            "notification_id": notification_id,
            "result": "SKIPPED",
            "reason": "Maximum delivery attempts have been reached.",
            "status": current_status,
        }

    # A failed notification must wait until next_retry_at.
    next_retry_at = current_status["next_retry_at"]

    if (
        current_status["delivery_status"] == "FAILED"
        and next_retry_at is not None
        and next_retry_at > datetime.now(timezone.utc)
    ):
        return {
            "notification_id": notification_id,
            "result": "SKIPPED",
            "reason": "Notification is not yet eligible for retry.",
            "status": current_status,
        }

    try:
        current_attempt = mark_notification_attempt_started(
            notification_id
        )

        send_email_notification(
            subject=subject,
            body=notification_message,
            recipient=email_recipient,
        )

        mark_notification_sent(notification_id)

        final_status = get_notification_status(notification_id)

        return {
            "notification_id": notification_id,
            "result": "SENT",
            "attempt_count": current_attempt,
            "status": final_status,
        }

    except Exception as delivery_error:
        error_message = (
            f"{type(delivery_error).__name__}: {delivery_error}"
        )

        mark_notification_failed(
            notification_id=notification_id,
            error_message=error_message,
        )

        final_status = get_notification_status(notification_id)

        return {
            "notification_id": notification_id,
            "result": "FAILED",
            "error": error_message,
            "status": final_status,
        }