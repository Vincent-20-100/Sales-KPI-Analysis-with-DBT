SELECT
    orders_id,
    date_date,
    ROUND(SUM(revenue*quantity), 2) AS revenue,
    SUM(quantity) AS products_sold,
    ROUND(SUM(purchase_cost*quantity), 2) AS purchase_cost,
    ROUND(SUM(margin*quantity), 2) AS margin
FROM {{ ref('int_sales_margin') }}
GROUP BY orders_id, date_date