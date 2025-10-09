{{ config(materialized='incremental', unique_key='hk_customer') }}

SELECT
  {{ hash_key('customer_id') }} AS hk_customer,
  customer_id,
  {{ load_ts() }} AS load_date,
  'stg_customer' AS record_source
FROM {{ ref('stg_customer') }}
{% if is_incremental() %}
WHERE customer_id NOT IN (SELECT customer_id FROM {{ this }})
{% endif %}
