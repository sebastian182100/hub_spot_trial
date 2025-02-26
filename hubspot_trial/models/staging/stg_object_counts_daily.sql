WITH source AS (
    SELECT
        date,
        account_id,
        total_contacts,
        total_lists,
        total_deals,
        total_landing_pages,
        total_blogs,
        total_workflows
    FROM {{ source('raw', 'object_counts_daily') }}
)
SELECT
    CAST(date AS DATE) AS date,
    account_id,
    total_contacts,
    total_lists,
    total_deals,
    total_landing_pages,
    total_blogs,
    total_workflows
FROM source