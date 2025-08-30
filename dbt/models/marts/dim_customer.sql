with c as (
  select * from {{ ref('stg_customers') }}
)
select
  distinct customer_id,
  customer_unique_id,
  customer_city,
  customer_state
from c
