SELECT
    orders_id,
    date_date,
    ROUND(SUM(revenue), 2) AS revenue,
    ROUND(SUM(quantity), 2) AS quantity,
    ROUND(SUM(purchase_cost), 2) AS purchase_cost,
    ROUND(SUM(margin), 2) AS margin
FROM {{ ref('int_sales_margin') }}
GROUP BY orders_id, date_date