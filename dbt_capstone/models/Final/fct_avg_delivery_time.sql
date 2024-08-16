select avg(avg_delivery_time_hour) as total_average_delivery_time_hour
from {{ ref("int_avg_delivery_time") }}