CREATE OR REPLACE VIEW monitoring.pipeline_operational_summary AS
SELECT
    COUNT(*) FILTER (
        WHERE alert_status IN (
            'OPEN',
            'ACKNOWLEDGED'
        )
    ) AS total_active_alerts,

    COUNT(*) FILTER (
        WHERE severity = 'CRITICAL'
          AND alert_status = 'OPEN'
    ) AS open_critical_alerts,

    COUNT(*) FILTER (
        WHERE severity = 'WARNING'
          AND alert_status = 'OPEN'
    ) AS open_warning_alerts,

    COUNT(*) FILTER (
        WHERE alert_status = 'ACKNOWLEDGED'
    ) AS acknowledged_alerts,

    COUNT(*) FILTER (
        WHERE alert_status = 'RESOLVED'
    ) AS resolved_alerts,

    MIN(created_at) FILTER (
        WHERE alert_status IN (
            'OPEN',
            'ACKNOWLEDGED'
        )
    ) AS oldest_active_alert_created_at,

    MAX(created_at) AS latest_alert_created_at

FROM monitoring.pipeline_alerts;

CREATE OR REPLACE VIEW monitoring.recent_pipeline_batches AS
SELECT
    a.spark_batch_id AS batch_id,
    a.status AS batch_status,

    a.source_record_count,
    a.bronze_inserted_count,
    a.silver_inserted_count,
    a.rejected_record_count,
    a.gold_affected_count,

    dq.rejection_rate_percent,
    dq.average_latency_ms,
    dq.maximum_latency_ms,
    dq.quality_status,
    dq.quality_message,

    a.started_at,
    a.completed_at,

    EXTRACT(
        EPOCH FROM (
            a.completed_at - a.started_at
        )
    )::NUMERIC(18, 2) AS batch_duration_seconds

FROM monitoring.pipeline_batch_audit a

LEFT JOIN monitoring.data_quality_results dq
    ON dq.pipeline_name = a.pipeline_name
   AND dq.spark_batch_id = a.spark_batch_id

ORDER BY a.spark_batch_id DESC;

CREATE OR REPLACE VIEW monitoring.pipeline_health_dashboard AS
WITH latest_batch AS (
    SELECT
        batch_id,
        batch_status,
        quality_status,
        source_record_count,
        rejected_record_count,
        rejection_rate_percent,
        average_latency_ms,
        maximum_latency_ms,
        started_at,
        completed_at,
        batch_duration_seconds,
        error_message
    FROM monitoring.recent_pipeline_batches
    ORDER BY batch_id DESC
    LIMIT 1
),

alert_summary AS (
    SELECT
        total_active_alerts,
        open_critical_alerts,
        open_warning_alerts,
        acknowledged_alerts,
        resolved_alerts,
        oldest_active_alert_created_at,
        latest_alert_created_at
    FROM monitoring.pipeline_operational_summary
)

SELECT
    latest_batch.batch_id AS latest_batch_id,
    latest_batch.batch_status AS latest_batch_status,
    latest_batch.quality_status AS latest_quality_status,

    latest_batch.source_record_count,
    latest_batch.rejected_record_count,
    latest_batch.rejection_rate_percent,
    latest_batch.average_latency_ms,
    latest_batch.maximum_latency_ms,
    latest_batch.batch_duration_seconds,

    alert_summary.total_active_alerts,
    alert_summary.open_critical_alerts,
    alert_summary.open_warning_alerts,
    alert_summary.acknowledged_alerts,
    alert_summary.resolved_alerts,

    alert_summary.oldest_active_alert_created_at,
    alert_summary.latest_alert_created_at,

    latest_batch.started_at AS latest_batch_started_at,
    latest_batch.completed_at AS latest_batch_completed_at,
    latest_batch.error_message AS latest_batch_error_message,

    CASE
        WHEN latest_batch.batch_status = 'FAILED'
            THEN 'UNHEALTHY'

        WHEN alert_summary.open_critical_alerts > 0
            THEN 'CRITICAL'

        WHEN latest_batch.quality_status = 'CRITICAL'
            THEN 'CRITICAL'

        WHEN alert_summary.open_warning_alerts > 0
            THEN 'WARNING'

        WHEN latest_batch.quality_status = 'WARNING'
            THEN 'WARNING'

        WHEN latest_batch.batch_status = 'SUCCESS'
            AND latest_batch.quality_status = 'HEALTHY'
            THEN 'HEALTHY'

        ELSE 'UNKNOWN'
    END AS overall_pipeline_status

FROM latest_batch
CROSS JOIN alert_summary;
