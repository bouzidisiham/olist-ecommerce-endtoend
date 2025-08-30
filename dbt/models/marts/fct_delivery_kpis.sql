with f as (
  select * from {{ ref('fact_order_items') }}
), by_order as (
  select
    order_id,
    max(customer_id) as customer_id,
    min(order_purchase_timestamp) as purchase_ts,
    max(order_delivered_customer_date) as delivered_ts,
    max(order_estimated_delivery_date) as estimated_ts
  from f
  group by 1
)
select
  order_id,
  customer_id,
  purchase_ts,
  delivered_ts,
  estimated_ts,
  case when delivered_ts is not null
       then extract(day from (delivered_ts - purchase_ts))
       else null end as delivery_days,
  case when delivered_ts is not null and delivered_ts > estimated_ts then 1 else 0 end as is_late
from by_order
