{{ config(materialized='incremental', unique_key='hk_policy_product') }}

SELECT
  {{ hash_key('policy_id','product_id') }} AS hk_policy_product,
  {{ hash_key('policy_id') }} AS hk_policy,
  {{ hash_key('product_id') }} AS hk_product,
  {{ load_ts() }} AS load_date,
  'stg_policy' AS record_source
FROM {{ ref('stg_policy') }}
WHERE product_id IS NOT NULL
