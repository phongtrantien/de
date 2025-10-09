{{ config(materialized='incremental', unique_key='hk_policy_claim') }}

SELECT
  {{ hash_key('policy_id','claim_id') }} AS hk_policy_claim,
  {{ hash_key('policy_id') }} AS hk_policy,
  {{ hash_key('claim_id') }} AS hk_claim,
  {{ load_ts() }} AS load_date,
  'stg_claim' AS record_source
FROM {{ ref('stg_claim') }}
WHERE policy_id IS NOT NULL AND claim_id IS NOT NULL
