{{ config(materialized='table') }}

SELECT
  customer_id,
  full_name,
  dob,
  gender,
  address,
  phone,
  email,
  load_timestamp as load_ts
FROM {{ source('raw','customer') }}
