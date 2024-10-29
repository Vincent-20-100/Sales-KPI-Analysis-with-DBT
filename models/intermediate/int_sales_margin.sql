WITH sub_purchasecost AS (SELECT
    date_date,
    orders_id,
    products_id,
    revenue,
    quantity,
    purchase_price,
ROUND(purchase_price*quantity,2) AS purchase_cost,
FROM {{ ref('stg_raw__sales') }}
LEFT JOIN {{ ref('stg_raw__product') }}
USING (products_id)
)
SELECT
*,
ROUND(revenue - purchase_cost,2) AS margin
FROM sub_purchasecost
