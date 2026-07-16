CREATE TABLE IF NOT EXISTS monitoring.data_quality_results
(
    pipeline_name             VARCHAR(100) NOT NULL,
    spark_batch_id            BIGINT NOT NULL,

    source_record_count       BIGINT NOT NULL DEFAULT 0,
    valid_record_count        BIGINT NOT NULL DEFAULT 0,
    rejected_record_count     BIGINT NOT NULL DEFAULT 0,

    rejection_rate_percent    NUMERIC(10, 4) NOT NULL DEFAULT 0,

    average_latency_ms        NUMERIC(18, 4),
    maximum_latency_ms        BIGINT,

    quality_status            VARCHAR(20) NOT NULL,

    quality_message           TEXT,

    checked_at                TIMESTAMP WITH TIME ZONE
                              NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_data_quality_results
        PRIMARY KEY (
            pipeline_name,
            spark_batch_id
        ),

    CONSTRAINT chk_data_quality_status
        CHECK (
            quality_status IN (
                'HEALTHY',
                'WARNING',
                'CRITICAL'
            )
        )
);


CREATE OR REPLACE FUNCTION monitoring.evaluate_batch_quality(
    p_pipeline_name VARCHAR,
    p_spark_batch_id BIGINT,
    p_source_record_count BIGINT,
    p_valid_record_count BIGINT,
    p_rejected_record_count BIGINT,
    p_average_latency_ms NUMERIC,
    p_maximum_latency_ms BIGINT
)
RETURNS VARCHAR
LANGUAGE plpgsql
AS $$
DECLARE
    rejection_rate NUMERIC(10, 4);
    calculated_status VARCHAR(20);
    calculated_message TEXT;
BEGIN
    IF p_source_record_count > 0 THEN
        rejection_rate :=
            (
                p_rejected_record_count::NUMERIC
                / p_source_record_count::NUMERIC
            ) * 100;
    ELSE
        rejection_rate := 0;
    END IF;

    /*
        Initial quality thresholds:

        CRITICAL:
        - rejection rate >= 20%
        - maximum latency >= 120000 ms

        WARNING:
        - rejection rate >= 5%
        - average latency >= 30000 ms

        HEALTHY:
        - everything below warning thresholds
    */

    IF rejection_rate >= 20
       OR COALESCE(p_maximum_latency_ms, 0) >= 120000 THEN

        calculated_status := 'CRITICAL';

        calculated_message :=
            'Critical data-quality threshold exceeded';

    ELSIF rejection_rate >= 5
          OR COALESCE(p_average_latency_ms, 0) >= 30000 THEN

        calculated_status := 'WARNING';

        calculated_message :=
            'Warning data-quality threshold exceeded';

    ELSE
        calculated_status := 'HEALTHY';

        calculated_message :=
            'Batch passed data-quality checks';
    END IF;

    INSERT INTO monitoring.data_quality_results
    (
        pipeline_name,
        spark_batch_id,
        source_record_count,
        valid_record_count,
        rejected_record_count,
        rejection_rate_percent,
        average_latency_ms,
        maximum_latency_ms,
        quality_status,
        quality_message,
        checked_at
    )
    VALUES
    (
        p_pipeline_name,
        p_spark_batch_id,
        p_source_record_count,
        p_valid_record_count,
        p_rejected_record_count,
        rejection_rate,
        p_average_latency_ms,
        p_maximum_latency_ms,
        calculated_status,
        calculated_message,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (
        pipeline_name,
        spark_batch_id
    )
    DO UPDATE SET
        source_record_count =
            EXCLUDED.source_record_count,

        valid_record_count =
            EXCLUDED.valid_record_count,

        rejected_record_count =
            EXCLUDED.rejected_record_count,

        rejection_rate_percent =
            EXCLUDED.rejection_rate_percent,

        average_latency_ms =
            EXCLUDED.average_latency_ms,

        maximum_latency_ms =
            EXCLUDED.maximum_latency_ms,

        quality_status =
            EXCLUDED.quality_status,

        quality_message =
            EXCLUDED.quality_message,

        checked_at =
            CURRENT_TIMESTAMP;

    RETURN calculated_status;
END;
$$;