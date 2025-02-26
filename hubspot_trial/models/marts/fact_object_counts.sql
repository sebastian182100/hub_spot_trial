{{ config(
    materialized='incremental',
    unique_key=['account_id', 'usage_date'],
    incremental_strategy='insert_overwrite'
) }}

WITH raw_object_counts AS (
    -- Extract raw object count data
    SELECT
        CAST(oc.date AS DATE) AS usage_date,
        oc.account_id,
        oc.total_contacts,
        oc.total_lists,
        oc.total_deals,
        oc.total_landing_pages,
        oc.total_blogs,
        oc.total_workflows
    FROM {{ ref('stg_object_counts_daily') }} oc
)

, financial_dates AS (
    -- Map usage_date to financial date_id in dim_dates
    SELECT
        roc.account_id,
        roc.usage_date,
        roc.total_contacts,
        roc.total_lists,
        roc.total_deals,
        roc.total_landing_pages,
        roc.total_blogs,
        roc.total_workflows
    FROM raw_object_counts roc
)

, aggregated_object_counts AS (
    -- Aggregate the object counts per account and date
    SELECT
        fd.account_id,
        fd.usage_date,
        SUM(fd.total_contacts) AS total_contacts,
        SUM(fd.total_lists) AS total_lists,
        SUM(fd.total_deals) AS total_deals,
        SUM(fd.total_landing_pages) AS total_landing_pages,
        SUM(fd.total_blogs) AS total_blogs,
        SUM(fd.total_workflows) AS total_workflows
    FROM financial_dates fd
    GROUP BY
        fd.account_id,
        fd.usage_date
)

{% if is_incremental() %}

-- Step 1: Remove existing records for the same usage_date
DELETE FROM {{ this }}
WHERE usage_date IN (SELECT usage_date FROM aggregated_object_counts)

-- Step 2: Insert new object count records
INSERT INTO {{ this }} (
    account_id,
    usage_date,
    total_contacts,
    total_lists,
    total_deals,
    total_landing_pages,
    total_blogs,
    total_workflows
)
SELECT
    account_id,
    usage_date,
    total_contacts,
    total_lists,
    total_deals,
    total_landing_pages,
    total_blogs,
    total_workflows
FROM aggregated_object_counts

{% else %}

-- Full refresh: Insert all historical object count data
SELECT
    account_id,
    usage_date,
    total_contacts,
    total_lists,
    total_deals,
    total_landing_pages,
    total_blogs,
    total_workflows
FROM aggregated_object_counts

{% endif %}