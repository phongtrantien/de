{{ config(materialized='table') }}

SELECT
  customer_id,
  customer_name,
  dob,
  gender,
  address,
  updated_at as load_ts
FROM {{ source('raw','customer') }}
