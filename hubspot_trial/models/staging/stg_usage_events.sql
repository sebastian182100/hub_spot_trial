WITH source AS (
    SELECT
        account_id,
        user_id,
        event_id,
        event_time,
        product,
        event_type
    FROM {{ source('raw', 'usage_events') }}
)
SELECT
    account_id,
    user_id,
    event_id,
    CAST(event_time AS TIMESTAMP) AS event_time,
    product,
    event_type
FROM source