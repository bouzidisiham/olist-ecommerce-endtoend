{% set churn_window_days = 120 %}

with o as (
  select customer_id, order_id, order_purchase_timestamp::date as ts
  from {{ ref('stg_orders') }}
), per_client as (
  select
    customer_id,
    count(*) as order_count,
    max(ts) as last_order_date,
    min(ts) as first_order_date,
    (current_date - max(ts))::int as recency_days  
  from o
  group by 1
), rfm as (
  select
    customer_id,
    order_count,
    last_order_date,
    first_order_date,
    recency_days,
    case
      when order_count >= 5 then 'High'
      when order_count between 2 and 4 then 'Medium'
      else 'Low'
    end as frequency_band,
    case
      when recency_days <= 30 then 'Hot'
      when recency_days between 31 and 120 then 'Warm'
      else 'Cold'
    end as recency_band
  from per_client
)
select
  customer_id,
  order_count,
  first_order_date,
  last_order_date,
  recency_days,
  frequency_band,
  recency_band,
  case when recency_days > {{ churn_window_days }} then 1 else 0 end as is_churned
from rfm
