ALTER TABLE bronze.stock_events
ADD CONSTRAINT uq_bronze_stock_events_event_id
UNIQUE (event_id);

CREATE TABLE IF NOT EXISTS bronze.stock_events_staging
(
    LIKE bronze.stock_events
    INCLUDING DEFAULTS
);

CREATE OR REPLACE FUNCTION bronze.merge_stock_events()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count BIGINT;
BEGIN
    INSERT INTO bronze.stock_events
    (
        event_id,
        schema_version,
        event_type,
        source,
        event_time,
        ingested_at,
        symbol,
        company,
        exchange,
        currency,
        price,
        volume
    )
    SELECT
        event_id,
        schema_version,
        event_type,
        source,
        event_time,
        ingested_at,
        symbol,
        company,
        exchange,
        currency,
        price,
        volume
    FROM bronze.stock_events_staging
    ON CONFLICT (event_id) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;

    TRUNCATE TABLE bronze.stock_events_staging;

    RETURN inserted_count;
END;
$$;