{{ config(
    materialized='incremental',
    unique_key=['account_id', 'start_date_id'],
    incremental_strategy='insert_overwrite'
) }}

WITH latest_accounts AS (
    -- Get the latest account details from the staging table
    SELECT
        account_id,
        name,
        country,
        vertical,
        nps,
        MIN(signup_date) AS first_signup_date,
        {{ dbt.current_timestamp() }} AS processed_at
    FROM {{ ref('stg_accounts') }}
    GROUP BY
        account_id,
        name,
        country,
        vertical,
        nps
)

, signup_dates AS (
    -- Get `signup_date_id` by linking to `dim_dates`
    SELECT
        la.account_id,
        la.name,
        d.date_id AS signup_date_id
    FROM latest_accounts la
    JOIN {{ ref('dim_dates') }} d
        ON d.full_date = la.first_signup_date
)

, existing_accounts AS (
    -- Fetch existing records from `dim_accounts`
    SELECT
        account_id,
        name AS existing_name,
        country AS existing_country,
        vertical AS existing_vertical,
        nps AS existing_nps,
        signup_date_id,
        start_date_id,
        end_date_id,
        is_current
    FROM {{ this }}
)

, new_accounts AS (
    -- Identify new records (changes in country, vertical, or NPS)
    SELECT
        la.account_id,
        la.name,
        la.country,
        la.vertical,
        la.nps,
        sd.signup_date_id,
        sd.signup_date_id AS start_date_id,
        NULL AS end_date_id,
        TRUE AS is_current
    FROM latest_accounts la
    JOIN signup_dates sd
        ON la.account_id = sd.account_id
    LEFT JOIN existing_accounts ea
        ON la.account_id = ea.account_id
        AND la.country = ea.existing_country
        AND la.vertical = ea.existing_vertical
        AND la.nps = ea.existing_nps
    WHERE ea.account_id IS NULL  -- Insert new version only if something changed
)

, all_versions AS (
    -- Combine existing and new records for correct ordering
    SELECT
        account_id,
        name,
        country,
        vertical,
        nps,
        signup_date_id,
        start_date_id
    FROM new_accounts
    UNION ALL
    SELECT
        account_id,
        name,
        country,
        vertical,
        nps,
        signup_date_id,
        start_date_id
    FROM {{ this }}
)

, ordered_versions AS (
    -- Assign `end_date_id` as the next `start_date_id`
    SELECT
        account_id,
        name,
        country,
        vertical,
        nps,
        signup_date_id,
        start_date_id,
        LEAD(start_date_id) OVER (PARTITION BY account_id ORDER BY start_date_id) AS end_date_id,
        CASE
            WHEN LEAD(start_date_id) OVER (PARTITION BY account_id ORDER BY start_date_id) IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM all_versions
)

{% if is_incremental() %}

-- Step 1: Close previous active records
UPDATE {{ this }}
SET
    end_date_id = ov.end_date_id,
    is_current = FALSE
FROM ordered_versions ov
WHERE {{ this }}.account_id = ov.account_id
AND {{ this }}.is_current = TRUE

-- Step 2: Insert new account versions
INSERT INTO {{ this }} (
    account_id,
    name,
    signup_date_id,
    country,
    vertical,
    nps,
    start_date_id,
    end_date_id,
    is_current
)
SELECT
    account_id,
    name,
    signup_date_id,
    country,
    vertical,
    nps,
    start_date_id,
    end_date_id,
    is_current
FROM ordered_versions

{% else %}

-- Full refresh: Insert all versions with correctly calculated `end_date_id`
SELECT
    account_id,
    name,
    signup_date_id,
    country,
    vertical,
    nps,
    start_date_id,
    end_date_id,
    is_current
FROM ordered_versions

{% endif %}
