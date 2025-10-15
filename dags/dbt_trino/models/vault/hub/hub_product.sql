{{ config(materialized='incremental', unique_key='hk_product') }}

SELECT
  {{ hash_key('product_id') }} AS hk_product,
  product_name,
  {{ load_ts() }} AS load_date,
  'stg_policy' AS record_source
FROM {{ ref('stg_product') }}
WHERE product_id IS NOT NULL
{% if is_incremental() %}
AND product_id NOT IN (SELECT product_id FROM {{ this }})
{% endif %}

