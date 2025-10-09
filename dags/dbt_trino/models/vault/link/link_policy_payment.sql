{{ config(materialized='incremental', unique_key='hk_policy_payment') }}

SELECT
  {{ hash_key('policy_id','payment_id') }} AS hk_policy_payment,
  {{ hash_key('policy_id') }} AS hk_policy,
  {{ hash_key('payment_id') }} AS hk_payment,
  {{ load_ts() }} AS load_date,
  'stg_payment' AS record_source
FROM {{ ref('stg_payment') }}
WHERE policy_id IS NOT NULL AND payment_id IS NOT NULL
