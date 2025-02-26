{{ config(
    materialized='incremental',
    unique_key=['account_id', 'product_id', 'usage_date_day'],
    incremental_strategy='insert_overwrite'
) }}

WITH raw_usage_events AS (
    -- Extract relevant product usage events from staging
    SELECT
        su.account_id,
        su.product AS product_name,
        su.event_time AS usage_date,  -- Using event_time directly for daily tracking
        su.event_type
    FROM {{ ref('stg_usage_events') }} su
    WHERE su.event_type IN ('Activation', 'Usage')  -- Only relevant financial events
)

, daily_financial_dates AS (
    -- Convert event time to the day of the month for financial aggregation
    SELECT
        sue.account_id,
        sue.product_name,
        dp.product_id,
        DATE_TRUNC('DAY', sue.usage_date) AS usage_date_day,  -- Truncate to the first day of the day
        sue.event_type,
        sue.usage_date
    FROM raw_usage_events sue
        JOIN {{ ref('dim_products') }} dp
            ON sue.product_name = dp.product_name
)

, enriched_financials AS (
    -- Enrich the raw usage events with product_id and count active users, usage, and activations
    SELECT
        fdi.account_id,
        fdi.product_id,
        fdi.usage_date_day,
        COUNT(DISTINCT fdi.account_id) AS active_users,  -- Count distinct users for active users
        SUM(CASE WHEN fdi.event_type = 'Usage' THEN 1 ELSE 0 END) AS total_usage,  -- Count total usage events
        SUM(CASE WHEN fdi.event_type = 'Activation' THEN 1 ELSE 0 END) AS total_activations -- Count total activations
    FROM daily_financial_dates fdi
    GROUP BY
        fdi.account_id,
        fdi.product_id,
        fdi.usage_date_day
)

, aggregated_usage AS (
    -- Aggregate the usage data by account and product on a daily level
    SELECT
        ef.account_id,
        ef.product_id,
        ef.usage_date_day,
        SUM(ef.total_usage) AS total_usage_events,  -- Aggregate usage events
        SUM(ef.total_activations) AS total_activations,  -- Aggregate activations
        COUNT(DISTINCT ef.account_id) AS active_users  -- Aggregate active users
    FROM enriched_financials ef
    GROUP BY
        ef.account_id,
        ef.product_id,
        ef.usage_date_day
)


{% if is_incremental() %}

-- Step 1: Remove existing records for the same usage_date_id
DELETE FROM {{ this }}
WHERE usage_date_day IN (SELECT usage_date_day FROM aggregated_usage)

-- Step 2: Insert new product usage records
INSERT INTO {{ this }} (
    account_id,
    product_id,
    usage_date_day,
    total_usage_events,
    total_activations,
    active_users
)
SELECT
    account_id,
    product_id,
    usage_date_day,
    total_usage_events,
    total_activations,
    active_users
FROM aggregated_usage

{% else %}

-- Full refresh: Insert all historical product usage data
SELECT
    account_id,
    product_id,
    usage_date_day,
    total_usage_events,
    total_activations,
    active_users
FROM aggregated_usage

{% endif %}
