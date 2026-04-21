SELECT
    ROW_NUMBER() OVER (ORDER BY stock_code) AS product_sk,
    stock_code AS product_bk,
    description
FROM (
    SELECT DISTINCT stock_code, description
    FROM {{ref('silver_data')}}
    WHERE stock_code IS NOT NULL
)