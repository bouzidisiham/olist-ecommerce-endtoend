with bounds as (
  select
    min(order_purchase_timestamp::date) as min_date,
    max(order_estimated_delivery_date::date) as max_date
  from {{ ref('stg_orders') }}
), series as (
  select generate_series(min_date, max_date, interval '1 day')::date as d
  from bounds
)
select
  d as date,
  extract(year from d)::int as year,
  extract(month from d)::int as month,
  to_char(d, 'YYYY-MM-DD') as date_key,
  to_char(d, 'Day') as weekday_name,
  extract(isodow from d)::int as isoweekday
from series
