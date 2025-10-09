{{ config(materialized='incremental', unique_key='hk_agent') }}

SELECT
  {{ hash_key('agent_id') }} AS hk_agent,
  agent_id,
  {{ load_ts() }} AS load_date,
  'stg_agent' AS record_source
FROM {{ ref('stg_agent') }}
WHERE agent_id IS NOT NULL
{% if is_incremental() %}
AND agent_id NOT IN (SELECT agent_id FROM {{ this }})
{% endif %}
