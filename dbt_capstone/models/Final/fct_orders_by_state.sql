select *
from {{ ref("int_orders_by_state") }}
order by order_count desc
limit 1