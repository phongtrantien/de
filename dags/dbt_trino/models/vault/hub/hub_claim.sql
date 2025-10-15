{{ config(materialized='incremental', unique_key='hk_claim') }}

SELECT
  {{ hash_key('claim_id') }} AS hk_claim,
  claim_id,
  {{ load_ts() }} AS load_date,
  'stg_claim' AS record_source
FROM {{ ref('stg_claim') }}
{% if is_incremental() %}
WHERE claim_id NOT IN (SELECT claim_id FROM {{ this }})
{% endif %}
