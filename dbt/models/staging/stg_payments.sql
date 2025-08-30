with source as (
  select * from raw.olist_order_payments_dataset
)
select
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value::numeric as payment_value
from source
