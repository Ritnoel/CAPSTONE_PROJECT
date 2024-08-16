With avg_delivery AS (
    SELECT
        o.order_id,
        avg(TIMESTAMP_DIFF(order_delivered_customer_date, order_purchase_timestamp, hour)) AS avg_delivery_time_hour
    FROM
        {{ ref('stg_orders') }} o 
    JOIN
        {{ ref('stg_order_items') }} oi
    ON
        o.order_id = oi.order_id
    WHERE o.order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL 
    GROUP BY o.order_id
)
SELECT
    *
FROM
    avg_delivery