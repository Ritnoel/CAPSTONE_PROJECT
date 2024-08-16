select *
from {{ ref("int_sales_by_category") }}
order by total_sales desc
limit 1