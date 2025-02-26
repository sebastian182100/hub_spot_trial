{{ config(
    materialized='incremental',
    unique_key='date_id'
) }}

WITH date_range AS (
    {% if is_incremental() %}
        -- Get the latest date in `dim_dates` and start from the next day
        SELECT MAX(full_date) + INTERVAL '1 DAY' AS start_date FROM {{ this }}
    {% else %}
        -- Full refresh: Start from 2000-01-01
        SELECT TO_DATE('2000-01-01') AS start_date
    {% endif %}
)

, end_date AS (
    -- Set the end date for full refresh (Current Date + 3 Years, ensuring we go to Dec 31)
    SELECT LAST_DAY(DATEADD(YEAR, 3, CURRENT_DATE), 'YEAR') AS max_date
)

, date_numbers AS (
    -- Generate row numbers for creating sequential dates
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ8()) - 1 AS rn
    FROM TABLE(GENERATOR(ROWCOUNT => 100000))  -- Ensure enough rows are generated
)

, generated_dates AS (
    -- Generate sequential dates from start_date to max_date
    SELECT
        DATEADD(DAY, rn, dr.start_date) AS full_date
    FROM date_numbers
    JOIN date_range dr
    WHERE DATEADD(DAY, rn, dr.start_date) <= (SELECT max_date FROM end_date)
)

SELECT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS date_id,  -- Convert date to YYYYMMDD format
    full_date,
    EXTRACT(DOW FROM full_date) + 1 AS day_of_week, -- Snowflake: 0=Sunday, so we add 1
    TO_CHAR(full_date, 'Day') AS day_name,
    EXTRACT(DAY FROM full_date) AS day_of_month,
    EXTRACT(WEEK FROM full_date) AS week_number,
    EXTRACT(MONTH FROM full_date) AS month,
    TO_CHAR(full_date, 'Month') AS month_name,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    EXTRACT(YEAR FROM full_date) AS year,
    CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday -- Placeholder, update this logic for actual holidays
FROM generated_dates
