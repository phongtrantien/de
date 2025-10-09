{{ config(materialized='table') }}

SELECT
  payment_id,
  policy_id,
  payment_date,
  payment_amount,
  payment_method,
  load_timestamp as load_ts
FROM {{ source('raw','payment') }}
