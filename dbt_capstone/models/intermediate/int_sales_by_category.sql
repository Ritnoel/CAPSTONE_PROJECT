With sales AS (
    SELECT
        o.order_id,
        p.product_category_name,
        oi.price
    FROM
        {{ ref('stg_orders') }} o 
    JOIN
        {{ ref('stg_order_items') }} oi
    ON
        o.order_id = oi.order_id
    JOIN
        {{ ref('stg_products') }} p 
    ON
       oi.product_id = p.product_id 
)
SELECT
    product_category_name,
    SUM(price) AS total_sales
FROM
    sales
GROUP BY
    product_category_name