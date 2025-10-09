{{ config(materialized='incremental', unique_key='hk_payment,load_date') }}

SELECT
  {{ hash_key('payment_id') }} AS hk_payment,
  payment_date,
  payment_amount,
  payment_method,
  load_ts,
  'stg_payment' AS record_source
FROM {{ ref('stg_payment') }}
