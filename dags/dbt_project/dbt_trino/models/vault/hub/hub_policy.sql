{{ config(materialized='incremental', unique_key='hk_policy') }}
SELECT
  {{ hash_key('policy_id') }} AS hk_policy,
  policy_id,
  {{ load_ts() }} AS load_date,
  'stg_policy' AS record_source
FROM {{ ref('stg_policy') }}
{% if is_incremental() %}
WHERE policy_id NOT IN (SELECT policy_id FROM {{ this }})
{% endif %}
