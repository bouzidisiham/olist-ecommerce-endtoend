with oi as (
  select * from {{ ref('stg_order_items') }}
), ord as (
  select * from {{ ref('stg_orders') }}
)
select
  oi.order_id,
  oi.order_item_id,
  ord.customer_id,
  oi.product_id,
  oi.seller_id,
  oi.price,
  oi.freight_value,
  ord.order_purchase_timestamp,
  ord.order_delivered_customer_date,
  ord.order_estimated_delivery_date
from oi
left join ord using (order_id)
