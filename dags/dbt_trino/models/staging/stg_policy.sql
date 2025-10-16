{{ config(materialized='table') }}

SELECT
  policy_id,
  policy_type as product_id,
  start_date,
  end_date,
  customer_id,
  updated_at as load_ts
FROM {{ source('raw','policy') }}
