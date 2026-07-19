/*
Operational alert lifecycle.

Supported transitions:

OPEN -> ACKNOWLEDGED
OPEN -> RESOLVED
ACKNOWLEDGED -> RESOLVED
*/

CREATE OR REPLACE FUNCTION monitoring.acknowledge_pipeline_alert(
    p_alert_id BIGINT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $function$
DECLARE
    affected_rows INTEGER;
BEGIN
    UPDATE monitoring.pipeline_alerts
    SET
        alert_status = 'ACKNOWLEDGED',
        acknowledged_at = CURRENT_TIMESTAMP
    WHERE alert_id = p_alert_id
      AND alert_status = 'OPEN';

    GET DIAGNOSTICS affected_rows = ROW_COUNT;

    RETURN affected_rows > 0;
END;
$function$;


CREATE OR REPLACE FUNCTION monitoring.resolve_pipeline_alert(
    p_alert_id BIGINT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $function$
DECLARE
    affected_rows INTEGER;
BEGIN
    UPDATE monitoring.pipeline_alerts
    SET
        alert_status = 'RESOLVED',
        resolved_at = CURRENT_TIMESTAMP
    WHERE alert_id = p_alert_id
      AND alert_status IN (
          'OPEN',
          'ACKNOWLEDGED'
      );

    GET DIAGNOSTICS affected_rows = ROW_COUNT;

    RETURN affected_rows > 0;
END;
$function$;

CREATE OR REPLACE VIEW monitoring.open_pipeline_alerts AS
SELECT
    alert_id,
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

    alert_status,
    created_at,
    acknowledged_at,

    EXTRACT(
        EPOCH FROM (
            CURRENT_TIMESTAMP - created_at
        )
    )::BIGINT AS alert_age_seconds

FROM monitoring.pipeline_alerts

WHERE alert_status IN (
    'OPEN',
    'ACKNOWLEDGED'
);


CREATE OR REPLACE VIEW monitoring.open_critical_alerts AS
SELECT
    alert_id,
    pipeline_name,
    batch_id,
    alert_type,
    severity,
    alert_message,

    rejection_rate_pct,
    average_latency_ms,
    maximum_latency_ms,

    alert_status,
    created_at,
    acknowledged_at,

    EXTRACT(
        EPOCH FROM (
            CURRENT_TIMESTAMP - created_at
        )
    )::BIGINT AS alert_age_seconds

FROM monitoring.pipeline_alerts

WHERE severity = 'CRITICAL'
  AND alert_status IN (
      'OPEN',
      'ACKNOWLEDGED'
  );