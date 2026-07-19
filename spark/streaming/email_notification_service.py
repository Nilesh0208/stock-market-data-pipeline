"""
Email notification service for CRITICAL pipeline alerts.

This module is responsible only for sending email.
Database delivery tracking will be handled separately.
"""

import smtplib
from email.message import EmailMessage
from typing import Optional

from config.settings import settings


def validate_email_configuration() -> None:
    """
    Validate required SMTP configuration before sending email.

    Raises:
        ValueError: If any required configuration is missing.
    """

    required_values = {
        "SMTP_HOST": settings.SMTP_HOST,
        "SMTP_USERNAME": settings.SMTP_USERNAME,
        "SMTP_PASSWORD": settings.SMTP_PASSWORD,
        "ALERT_EMAIL_FROM": settings.ALERT_EMAIL_FROM,
        "ALERT_EMAIL_TO": settings.ALERT_EMAIL_TO,
    }

    missing_values = [
        name
        for name, value in required_values.items()
        if not value or not value.strip()
    ]

    if missing_values:
        raise ValueError(
            "Missing email configuration: "
            + ", ".join(missing_values)
        )


def build_email_message(
    subject: str,
    body: str,
    recipient: Optional[str] = None,
) -> EmailMessage:
    """
    Build a standard email message.

    Args:
        subject: Email subject.
        body: Email message body.
        recipient: Optional recipient override.

    Returns:
        Configured EmailMessage object.
    """

    email_recipient = recipient or settings.ALERT_EMAIL_TO

    message = EmailMessage()
    message["From"] = settings.ALERT_EMAIL_FROM
    message["To"] = email_recipient
    message["Subject"] = subject
    message.set_content(body)

    return message


def send_email_notification(
    subject: str,
    body: str,
    recipient: Optional[str] = None,
) -> None:
    """
    Send an email using the configured SMTP server.

    Args:
        subject: Email subject.
        body: Email body.
        recipient: Optional recipient override.

    Raises:
        RuntimeError: When email notifications are disabled.
        ValueError: When required configuration is missing.
        smtplib.SMTPException: When SMTP delivery fails.
        OSError: When a network-level connection error occurs.
    """

    if not settings.CRITICAL_EMAIL_NOTIFICATIONS_ENABLED:
        raise RuntimeError(
            "Critical email notifications are disabled."
        )

    validate_email_configuration()

    message = build_email_message(
        subject=subject,
        body=body,
        recipient=recipient,
    )

    with smtplib.SMTP(
        host=settings.SMTP_HOST,
        port=settings.SMTP_PORT,
        timeout=settings.SMTP_CONNECTION_TIMEOUT_SECONDS,
    ) as smtp_server:

        smtp_server.ehlo()

        if settings.SMTP_USE_TLS:
            smtp_server.starttls()
            smtp_server.ehlo()

        smtp_server.login(
            settings.SMTP_USERNAME,
            settings.SMTP_PASSWORD,
        )

        smtp_server.send_message(message)