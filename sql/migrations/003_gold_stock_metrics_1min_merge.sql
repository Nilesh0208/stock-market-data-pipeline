CREATE OR REPLACE FUNCTION gold.merge_stock_metrics_1min(
    p_batch_id BIGINT
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_affected_rows INTEGER := 0;
    v_pipeline_name CONSTANT VARCHAR(100)
        := 'gold_stock_metrics_1min';
BEGIN
    /*
     * Prevent the same Spark batch from being aggregated twice.
     */
    IF EXISTS (
        SELECT 1
        FROM gold.processed_batches
        WHERE pipeline_name = v_pipeline_name
          AND spark_batch_id = p_batch_id
    ) THEN
        DELETE FROM gold.stock_metrics_1min_staging
        WHERE spark_batch_id = p_batch_id;

        RETURN 0;
    END IF;

    INSERT INTO gold.stock_metrics_1min AS target
    (
        window_start,
        window_end,
        symbol,
        company,
        exchange,
        currency,

        open_price,
        open_event_time,
        open_event_id,

        high_price,
        low_price,

        close_price,
        close_event_time,
        close_event_id,

        average_price,
        total_volume,
        event_count,
        average_latency_ms,

        created_at,
        updated_at
    )
    SELECT
        window_start,
        window_end,
        symbol,
        company,
        exchange,
        currency,

        open_price,
        open_event_time,
        open_event_id,

        high_price,
        low_price,

        close_price,
        close_event_time,
        close_event_id,

        average_price,
        total_volume,
        event_count,
        average_latency_ms,

        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    FROM gold.stock_metrics_1min_staging
    WHERE spark_batch_id = p_batch_id

    ON CONFLICT (window_start, symbol)
    DO UPDATE SET
        window_end = EXCLUDED.window_end,

        company = COALESCE(
            EXCLUDED.company,
            target.company
        ),

        exchange = COALESCE(
            EXCLUDED.exchange,
            target.exchange
        ),

        currency = COALESCE(
            EXCLUDED.currency,
            target.currency
        ),

        open_price =
            CASE
                WHEN (
                    EXCLUDED.open_event_time,
                    EXCLUDED.open_event_id
                ) < (
                    target.open_event_time,
                    target.open_event_id
                )
                THEN EXCLUDED.open_price
                ELSE target.open_price
            END,

        open_event_time =
            CASE
                WHEN (
                    EXCLUDED.open_event_time,
                    EXCLUDED.open_event_id
                ) < (
                    target.open_event_time,
                    target.open_event_id
                )
                THEN EXCLUDED.open_event_time
                ELSE target.open_event_time
            END,

        open_event_id =
            CASE
                WHEN (
                    EXCLUDED.open_event_time,
                    EXCLUDED.open_event_id
                ) < (
                    target.open_event_time,
                    target.open_event_id
                )
                THEN EXCLUDED.open_event_id
                ELSE target.open_event_id
            END,

        high_price = GREATEST(
            target.high_price,
            EXCLUDED.high_price
        ),

        low_price = LEAST(
            target.low_price,
            EXCLUDED.low_price
        ),

        close_price =
            CASE
                WHEN (
                    EXCLUDED.close_event_time,
                    EXCLUDED.close_event_id
                ) > (
                    target.close_event_time,
                    target.close_event_id
                )
                THEN EXCLUDED.close_price
                ELSE target.close_price
            END,

        close_event_time =
            CASE
                WHEN (
                    EXCLUDED.close_event_time,
                    EXCLUDED.close_event_id
                ) > (
                    target.close_event_time,
                    target.close_event_id
                )
                THEN EXCLUDED.close_event_time
                ELSE target.close_event_time
            END,

        close_event_id =
            CASE
                WHEN (
                    EXCLUDED.close_event_time,
                    EXCLUDED.close_event_id
                ) > (
                    target.close_event_time,
                    target.close_event_id
                )
                THEN EXCLUDED.close_event_id
                ELSE target.close_event_id
            END,

        average_price =
            (
                target.average_price
                * target.event_count
                +
                EXCLUDED.average_price
                * EXCLUDED.event_count
            )
            /
            (
                target.event_count
                + EXCLUDED.event_count
            ),

        total_volume =
            target.total_volume
            + EXCLUDED.total_volume,

        event_count =
            target.event_count
            + EXCLUDED.event_count,

        average_latency_ms =
            CASE
                WHEN target.average_latency_ms IS NULL
                    THEN EXCLUDED.average_latency_ms

                WHEN EXCLUDED.average_latency_ms IS NULL
                    THEN target.average_latency_ms

                ELSE
                    (
                        target.average_latency_ms
                        * target.event_count
                        +
                        EXCLUDED.average_latency_ms
                        * EXCLUDED.event_count
                    )
                    /
                    (
                        target.event_count
                        + EXCLUDED.event_count
                    )
            END,

        updated_at = CURRENT_TIMESTAMP;

    GET DIAGNOSTICS
        v_affected_rows = ROW_COUNT;

    INSERT INTO gold.processed_batches
    (
        pipeline_name,
        spark_batch_id
    )
    VALUES
    (
        v_pipeline_name,
        p_batch_id
    );

    DELETE FROM gold.stock_metrics_1min_staging
    WHERE spark_batch_id = p_batch_id;

    RETURN v_affected_rows;
END;
$$;