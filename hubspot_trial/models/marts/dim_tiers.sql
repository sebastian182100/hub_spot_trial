{{ config(
    materialized='table',
    unique_key='tier_id'
) }}

WITH raw_tiers AS (
    -- Extract relevant tier details from the staging data
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['tier', 'monthly_cost']) }} AS tier_id,  -- Generate surrogate key from tier_name and tier_price
        tier AS tier_name,
        monthly_cost AS tier_price
    FROM {{ ref('stg_subscriptions') }} ss  -- Assuming this data comes from a staging table
    WHERE tier IS NOT NULL AND tier_price IS NOT NULL
)

SELECT 
    tier_id,
    tier_name,
    tier_price
FROM raw_tiers
