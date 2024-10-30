SELECT
    sales.date_date,
    ROUND(SUM(sales.revenue), 2) AS total_revenue,
    ROUND(AVG(sales.revenue), 2) AS avereage_basket,
    ROUND(SUM(margin.operational_margin), 2) AS operational_margin,
    ROUND(SUM(sales.purchase_cost), 2) AS purchase_cost,
    ROUND(SUM(margin.shipping_fee), 2) AS shipping_fee,
    ROUND(SUM(margin.log_cost), 2) AS log_cost,
    SUM(sales.products_sold) AS products_sold
FROM {{ref('int_orders_margin')}} AS sales 
LEFT JOIN {{ref('int_orders_operational')}} AS margin
USING(orders_id)
GROUP BY date_date