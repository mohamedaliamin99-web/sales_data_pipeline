SELECT
    ROW_NUMBER() OVER (ORDER BY country) AS country_sk,
    country
FROM (
    SELECT DISTINCT country
    FROM {{ref('silver_data')}}
    WHERE country IS NOT NULL
)