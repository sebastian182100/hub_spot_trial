WITH source AS (
    SELECT
        id AS account_id,
        name,
        signup_date,
        country,
        vertical,
        nps
    FROM {{ source('raw', 'accounts') }}
)
SELECT * FROM source
