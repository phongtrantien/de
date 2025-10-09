{{ config(materialized='incremental', unique_key='hk_policy_customer') }}

SELECT
  {{ hash_key('policy_id','customer_id') }} AS hk_policy_customer,
  {{ hash_key('policy_id') }} AS hk_policy,
  {{ hash_key('customer_id') }} AS hk_customer,
  {{ load_ts() }} AS load_date,
  'stg_policy' AS record_source
FROM {{ ref('stg_policy') }}
WHERE customer_id IS NOT NULL
