SELECT
    ROW_NUMBER() OVER (ORDER BY date) AS date_sk,
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(WEEK FROM date) AS week,
    EXTRACT(QUARTER FROM date) AS quarter
FROM (
    SELECT DISTINCT TO_DATE(invoice_date, 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"') AS date
    FROM {{ref('silver_data')}}
    WHERE invoice_date IS NOT NULL
)