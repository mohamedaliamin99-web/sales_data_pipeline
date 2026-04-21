SELECT DISTINCT s.invoice_no,
       c.customer_sk,
       co.country_sk,
       d.date_sk,
       p.product_sk,
       s.unit_price,
       s.revenue,
       s.is_return
FROM {{ref('silver_data')}} s
JOIN {{ref('dim_customer')}} c ON s.customer_id = c.customer_bk
JOIN {{ref('dim_country')}} co ON s.country = co.country
JOIN {{ref('dim_date')}} d ON TO_DATE(s.invoice_date, 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"') = d.date
JOIN {{ref('dim_product')}} p ON s.stock_code = p.product_bk