CREATE OR REPLACE FUNCTION monitoring.start_pipeline_batch(
    p_pipeline_name VARCHAR,
    p_spark_batch_id BIGINT,
    p_source_record_count BIGINT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO monitoring.pipeline_batch_audit
    (
        pipeline_name,
        spark_batch_id,
        status,
        source_record_count,
        started_at,
        completed_at,
        error_message
    )
    VALUES
    (
        p_pipeline_name,
        p_spark_batch_id,
        'STARTED',
        p_source_record_count,
        CURRENT_TIMESTAMP,
        NULL,
        NULL
    )
    ON CONFLICT (pipeline_name, spark_batch_id)
    DO UPDATE SET
        status = 'STARTED',
        source_record_count = EXCLUDED.source_record_count,
        started_at = CURRENT_TIMESTAMP,
        completed_at = NULL,
        error_message = NULL;
END;
$$;


CREATE OR REPLACE FUNCTION monitoring.complete_pipeline_batch(
    p_pipeline_name VARCHAR,
    p_spark_batch_id BIGINT,
    p_bronze_inserted_count BIGINT,
    p_silver_inserted_count BIGINT,
    p_rejected_record_count BIGINT,
    p_gold_affected_count BIGINT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE monitoring.pipeline_batch_audit
    SET
        status = 'SUCCESS',
        bronze_inserted_count = p_bronze_inserted_count,
        silver_inserted_count = p_silver_inserted_count,
        rejected_record_count = p_rejected_record_count,
        gold_affected_count = p_gold_affected_count,
        completed_at = CURRENT_TIMESTAMP,
        error_message = NULL
    WHERE pipeline_name = p_pipeline_name
      AND spark_batch_id = p_spark_batch_id;
END;
$$;


CREATE OR REPLACE FUNCTION monitoring.fail_pipeline_batch(
    p_pipeline_name VARCHAR,
    p_spark_batch_id BIGINT,
    p_error_message TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE monitoring.pipeline_batch_audit
    SET
        status = 'FAILED',
        completed_at = CURRENT_TIMESTAMP,
        error_message = p_error_message
    WHERE pipeline_name = p_pipeline_name
      AND spark_batch_id = p_spark_batch_id;
END;
$$;