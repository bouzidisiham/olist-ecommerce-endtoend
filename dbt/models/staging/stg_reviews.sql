with source as (
  select * from raw.olist_order_reviews_dataset
)
select
  review_id,
  order_id,
  review_score,
  review_comment_title,
  review_comment_message,
  review_creation_date::timestamp as review_creation_date,
  review_answer_timestamp::timestamp as review_answer_timestamp
from source
