SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_sk,
    customer_id AS customer_bk
FROM (
    SELECT DISTINCT customer_id
    FROM {{ref('silver_data')}}
    WHERE customer_id IS NOT NULL
)