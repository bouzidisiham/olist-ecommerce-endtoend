with s as (
  select * from {{ ref('stg_sellers') }}
)
select
  distinct seller_id,
  seller_city,
  seller_state
from s
