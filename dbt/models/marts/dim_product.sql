with p as (
  select * from {{ ref('stg_products') }}
)
select
  distinct product_id,
  product_category_name,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm
from p
