{{ config(materialized='incremental', unique_key='hk_claim,load_date') }}

SELECT
  {{ hash_key('claim_id') }} AS hk_claim,
  claim_date,
  claim_amount,
  claim_status,
  load_ts,
  'stg_claim' AS record_source
FROM {{ ref('stg_claim') }}

