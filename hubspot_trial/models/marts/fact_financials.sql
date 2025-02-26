{{ config(
    materialized='incremental',
    unique_key=['account_id', 'product_id', 'tier_id', 'financial_month_id'],
    incremental_strategy='insert_overwrite'
) }}

WITH subscription_events AS (
    -- Extract relevant subscription transactions
    SELECT
        ss.account_id,
        dp.product_id,
        ss.product AS product_name,
        ss.start_date AS financial_date,
        ss.cancel_date,
        ss.monthly_cost AS revenue,
        ss.tier AS tier_name,
        t.tier_id
    FROM dbt_db.dbt_schema.stg_subscriptions ss
        LEFT JOIN dbt_db.dbt_schema.dim_tiers t
            ON ss.tier = t.tier_name
            AND ss.monthly_cost = t.tier_price
        LEFT JOIN dbt_db.dbt_schema.dim_products dp
            ON ss.product = dp.product_name
)
, financial_monthly_dates_prep AS (
    SELECT DISTINCT
        DATE_TRUNC('month', dd.full_date) AS month,
        dd.full_date,
        s.account_id,
        s.product_id,
        s.product_name,
        s.financial_date,
        s.cancel_date,
        s.revenue,
        s.tier_name,
        s.tier_id
    FROM subscription_events s
        JOIN dbt_db.dbt_schema.dim_dates dd
            ON dd.full_date BETWEEN s.financial_date AND s.cancel_date
)
, financial_monthly_dates AS (
    SELECT DISTINCT
        s.month AS financial_month,
        COUNT(*) OVER (PARTITION BY s.month, s.account_id) AS days_active,
        LAST_DAY(s.month) - s.month + 1 AS total_days,  -- Days in the month
        s.account_id,
        s.product_id,
        s.product_name,
        s.financial_date,
        s.cancel_date,
        s.revenue,
        s.tier_name,
        s.tier_id
    FROM financial_monthly_dates_prep s
)
, enriched_financials AS (
    -- Assign the correct `product_id`, `tier_id`, and revenue per monthly event
    SELECT
        fmi.account_id,
        fmi.product_id,
        fmi.tier_id,
        fmi.revenue,
        fmi.days_active,
        fmi.total_days,
        fmi.financial_month,
        CASE
            WHEN fmi.cancel_date IS NOT NULL
            AND DATE_TRUNC('MONTH', fmi.cancel_date) = DATE_TRUNC('MONTH', fmi.financial_month)
            THEN fmi.revenue * (fmi.days_active::FLOAT / fmi.total_days)  -- Pro-rate revenue if canceled in the same month
            ELSE fmi.revenue
        END AS adjusted_revenue,
        fmi.cancel_date
    FROM financial_monthly_dates fmi
)

, aggregated_financials AS (
    -- Aggregate financial data at the monthly level
    SELECT
        ef.account_id,
        ef.product_id,
        ef.tier_id,
        ef.financial_month,
        SUM(ef.adjusted_revenue) AS total_revenue,
        COUNT(DISTINCT ef.account_id) AS active_subscriptions,
        COUNT(DISTINCT CASE
            WHEN ef.cancel_date IS NOT NULL AND DATE_TRUNC('MONTH', ef.cancel_date) = DATE_TRUNC('MONTH', ef.financial_month)
            THEN ef.account_id
        END) AS churned_subscriptions
    FROM enriched_financials ef
    GROUP BY
        ef.account_id,
        ef.product_id,
        ef.tier_id,
        ef.financial_month
)

{% if is_incremental() %}

-- Step 1: Remove existing records for the same financial_month
DELETE FROM {{ this }}
WHERE financial_month IN (SELECT financial_month FROM aggregated_financials)

-- Step 2: Insert new financial records
INSERT INTO {{ this }} (
    account_id,
    product_id,
    tier_id,
    financial_month,
    total_revenue,
    active_subscriptions,
    churned_subscriptions
)
SELECT 
    account_id,
    product_id,
    tier_id,
    financial_month,
    total_revenue,
    active_subscriptions,
    churned_subscriptions
FROM aggregated_financials

{% else %}

-- Full refresh: Insert all historical monthly financial data
SELECT 
    account_id,
    product_id,
    tier_id,
    financial_month,
    total_revenue,
    active_subscriptions,
    churned_subscriptions
FROM aggregated_financials

{% endif %}
