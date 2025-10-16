{{ config(materialized='table') }}

SELECT
  payment_id,
  policy_id,
  payment_date,
  payment_amount,
  payment_method,
  updated_at as load_ts
FROM {{ source('raw','payment') }}
