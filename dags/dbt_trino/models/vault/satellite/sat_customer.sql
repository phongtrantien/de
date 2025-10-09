{{ config(materialized='incremental', unique_key='hk_customer,load_date') }}

SELECT
  {{ hash_key('customer_id') }} AS hk_customer,
  full_name,
  dob,
  gender,
  address,
  phone,
  email,
  load_ts,
  'stg_customer' AS record_source
FROM {{ ref('stg_customer') }}

