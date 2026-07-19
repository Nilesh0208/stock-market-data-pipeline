-- ============================================================
-- External notification delivery tracking
-- ============================================================

CREATE SCHEMA IF NOT EXISTS monitoring;


CREATE TABLE IF NOT EXISTS monitoring.alert_notifications
(
    notification_id BIGSERIAL PRIMARY KEY,

    pipeline_name VARCHAR(200) NOT NULL,
    spark_batch_id BIGINT NOT NULL,

    alert_severity VARCHAR(20) NOT NULL,
    notification_channel VARCHAR(30) NOT NULL,

    recipient VARCHAR(500) NOT NULL,
    subject VARCHAR(500),
    notification_message TEXT,

    delivery_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',

    attempt_count INTEGER NOT NULL DEFAULT 0,
    maximum_attempts INTEGER NOT NULL DEFAULT 3,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    first_attempted_at TIMESTAMPTZ,
    last_attempted_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,

    last_error TEXT,

    CONSTRAINT chk_alert_notification_severity
        CHECK (
            alert_severity IN (
                'WARNING',
                'CRITICAL'
            )
        ),

    CONSTRAINT chk_alert_notification_channel
        CHECK (
            notification_channel IN (
                'EMAIL',
                'SLACK',
                'TEAMS'
            )
        ),

    CONSTRAINT chk_alert_notification_status
        CHECK (
            delivery_status IN (
                'PENDING',
                'RETRYING',
                'SENT',
                'FAILED'
            )
        ),

    CONSTRAINT chk_alert_notification_attempts
        CHECK (
            attempt_count >= 0
            AND maximum_attempts > 0
        ),

    CONSTRAINT uq_alert_notification_delivery
        UNIQUE (
            pipeline_name,
            spark_batch_id,
            alert_severity,
            notification_channel,
            recipient
        )
);


CREATE INDEX IF NOT EXISTS idx_alert_notifications_status
    ON monitoring.alert_notifications (
        delivery_status,
        next_retry_at
    );


CREATE INDEX IF NOT EXISTS idx_alert_notifications_batch
    ON monitoring.alert_notifications (
        pipeline_name,
        spark_batch_id
    );


CREATE INDEX IF NOT EXISTS idx_alert_notifications_created_at
    ON monitoring.alert_notifications (
        created_at DESC
    );


COMMENT ON TABLE monitoring.alert_notifications IS
'Tracks external alert notification delivery, failure details, and retry attempts.';


COMMENT ON COLUMN monitoring.alert_notifications.delivery_status IS
'PENDING, RETRYING, SENT, or FAILED.';


COMMENT ON COLUMN monitoring.alert_notifications.attempt_count IS
'Number of delivery attempts already performed.';


COMMENT ON COLUMN monitoring.alert_notifications.next_retry_at IS
'Next time a failed notification is eligible for retry.';