{{ config(materialized='table') }}

SELECT
  policy_id,
  product_id,
  start_date,
  end_date,
  customer_id,
  load_timestamp as load_ts
FROM {{ source('raw','policy') }}
