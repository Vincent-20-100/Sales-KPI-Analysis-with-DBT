SELECT
    date_date,
    orders_id,
    ROUND(SUM(margin) + SUM(shipping_fee) - SUM(log_cost) - SUM(ship_cost), 2) AS operational_margin,
    SUM(margin) AS margin,
    SUM(shipping_fee) AS shipping_fee,
    SUM(log_cost) AS log_cost,
    SUM(ship_cost) AS ship_cost
FROM {{ ref('int_orders_margin') }}
LEFT JOIN {{ ref('stg_raw__ship') }}
USING (orders_id)
GROUP BY date_date, orders_id