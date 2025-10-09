{{ config(materialized='incremental', unique_key='hk_policy,load_date') }}

SELECT
  {{ hash_key('policy_id') }} AS hk_policy,
  policy_id,
  start_date,
  end_date,
  load_ts,
  'stg_policy' AS record_source
FROM {{ ref('stg_policy') }}
