{{ config(materialized='incremental', unique_key='hk_payment') }}

SELECT
  {{ hash_key('payment_id') }} AS hk_payment,
  payment_id,
  {{ load_ts() }} AS load_date,
  'stg_payment' AS record_source
FROM {{ ref('stg_payment') }}
{% if is_incremental() %}
WHERE payment_id NOT IN (SELECT payment_id FROM {{ this }})
{% endif %}

