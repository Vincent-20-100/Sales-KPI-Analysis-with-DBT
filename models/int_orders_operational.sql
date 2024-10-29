WITH sub_shipcost AS (SELECT
    date_date,
    orders_id,
    ROUND(SUM(revenue), 2) AS revenue,
    ROUND(SUM(quantity), 2) AS quantity,
    ROUND(SUM(margin), 2) AS margin,
    ROUND(AVG(shipping_fee), 2) AS shipping_fee,
    ROUND(AVG(logcost), 2) AS logcost,
    ROUND(AVG(ship_cost), 2) AS ship_cost
FROM {{ ref('int_sales_margin') }}
LEFT JOIN {{ ref('stg_raw__ship') }}
USING (orders_id)
GROUP BY date_date, orders_id
)
SELECT
    date_date,
    orders_id,
    ROUND(margin + shipping_fee - logcost - ship_cost, 2) AS operational_margin
FROM sub_shipcost