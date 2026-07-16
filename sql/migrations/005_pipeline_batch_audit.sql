CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.pipeline_batch_audit
(
    pipeline_name          VARCHAR(100) NOT NULL,
    spark_batch_id         BIGINT NOT NULL,

    status                 VARCHAR(20) NOT NULL,

    source_record_count    BIGINT DEFAULT 0,
    bronze_inserted_count  BIGINT DEFAULT 0,
    silver_inserted_count  BIGINT DEFAULT 0,
    rejected_record_count  BIGINT DEFAULT 0,
    gold_affected_count    BIGINT DEFAULT 0,

    started_at             TIMESTAMP WITH TIME ZONE NOT NULL
                           DEFAULT CURRENT_TIMESTAMP,

    completed_at           TIMESTAMP WITH TIME ZONE,

    error_message          TEXT,

    CONSTRAINT pk_pipeline_batch_audit
        PRIMARY KEY (pipeline_name, spark_batch_id),

    CONSTRAINT chk_pipeline_batch_status
        CHECK (
            status IN (
                'STARTED',
                'SUCCESS',
                'FAILED'
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_pipeline_batch_audit_status
ON monitoring.pipeline_batch_audit(status);

CREATE INDEX IF NOT EXISTS idx_pipeline_batch_audit_started_at
ON monitoring.pipeline_batch_audit(started_at DESC);