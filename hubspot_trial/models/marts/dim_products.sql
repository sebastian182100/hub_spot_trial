{{ config(
    materialized='table',
    unique_key='product_id'
) }}

WITH raw_products AS (
    -- Extract unique product details from usage events
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product']) }} AS product_id,  -- Generate surrogate key
        product AS product_name
    FROM {{ ref('stg_usage_events') }}
    WHERE product IS NOT NULL
    GROUP BY
        product
)

SELECT
    product_id,
    product_name
FROM raw_products
