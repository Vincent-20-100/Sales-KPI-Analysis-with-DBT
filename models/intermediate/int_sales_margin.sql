SELECT
    stg_raw__sales.date_date,
    stg_raw__sales.products_id,
    ROUND(SUM(stg_raw__sales.revenue - stg_raw__product.purchase_price), 2) AS margin,
    ROUND(SUM(stg_raw__sales.quantity * stg_raw__product.purchase_price), 2) AS purchase_cost,
FROM {{ ref('stg_raw__sales') }}
LEFT JOIN {{ ref('stg_raw__product') }}
ON stg_raw__product.products_id = stg_raw__sales.products_id
GROUP BY stg_raw__sales.date_date, stg_raw__sales.products_id