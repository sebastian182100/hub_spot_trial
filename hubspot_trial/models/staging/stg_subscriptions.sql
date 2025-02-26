WITH source AS (
    SELECT
        account_id,
        start_date,
        cancel_date,
        product,
        tier,
        monthly_cost,
        touchless_conversion
    FROM {{ source('raw', 'subscriptions') }}
)
SELECT
    account_id,
    start_date,
    cancel_date,
    product,
    tier,
    CAST(monthly_cost AS DECIMAL(10,2)) AS monthly_cost,
    touchless_conversion
FROM source