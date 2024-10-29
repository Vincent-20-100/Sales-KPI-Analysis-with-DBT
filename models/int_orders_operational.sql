SELECT
    date_date,
    orders_id,
    ROUND(SUM(margin) + SUM(shipping_fee) - SUM(logcost) - SUM(ship_cost), 2) AS operational_margin
FROM {{ ref('int_orders_margin') }}
LEFT JOIN {{ ref('stg_raw__ship') }}
USING (orders_id)
GROUP BY date_date, orders_id